from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from underfit_api.buffers import BadStartLineError, BadStepError
from underfit_api.buffers import scalars as scalar_buffer
from underfit_api.config import config
from underfit_api.dependencies import Conn, Ctx, CurrentWorker, MaybeUser
from underfit_api.models import Body, BufferedResponse, Scalar, ScalarSeriesResponse
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class WriteScalarsBody(Body):
    start_line: int
    scalars: list[Scalar]


def _select_resolution(counts: dict[int, int], target_points: int) -> int:
    for resolution in config.buffer.scalar_resolutions:
        if 0 < counts[resolution] <= target_points:
            return resolution
    for resolution in reversed(config.buffer.scalar_resolutions):
        if counts[resolution] > 0:
            return resolution
    return 1


@router.post("/ingest/scalars")
def write_scalars(body: WriteScalarsBody, conn: Conn, worker_id: CurrentWorker) -> BufferedResponse:
    if not workers_repo.touch(conn, worker_id):
        raise HTTPException(401, "Unauthorized")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if not body.scalars:
        return BufferedResponse(next_start_line=body.start_line)
    try:
        scalar_buffer.append(conn, worker_id, body.start_line, body.scalars)
    except BadStartLineError as e:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": e.expected}) from e
    except BadStepError as e:
        raise HTTPException(409, detail={"error": "Step must be strictly increasing", "lastStep": e.last_step}) from e
    return BufferedResponse(next_start_line=body.start_line + len(body.scalars))


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars")
def read_scalars(
    handle: str, project_name: str, run_name: str, conn: Conn, ctx: Ctx, user: MaybeUser,
    resolution: Annotated[int | None, Query(gt=0)] = None,
    target_points: Annotated[int | None, Query(alias="targetPoints", gt=0)] = None,
) -> ScalarSeriesResponse:
    run = resolve_run(conn, ctx, handle, project_name, run_name, user)
    if resolution is not None and target_points is not None:
        raise HTTPException(400, "Cannot specify both resolution and targetPoints")
    workers = workers_repo.list_by_run(conn, run.id)
    counts = scalar_buffer.total_line_counts(conn, [w.id for w in workers])
    selected_resolution = resolution if resolution is not None else 1
    if target_points is not None:
        selected_resolution = _select_resolution(counts, target_points)
    if counts.get(selected_resolution, 0) == 0:
        raise HTTPException(404, "Resolution not found")

    scalars: list[Scalar] = []
    for worker in workers:
        for seg in scalar_seg_repo.list_by_resolution(conn, worker.id, selected_resolution):
            data = ctx.storage.read(f"{run.storage_key}/{seg.storage_key}")
            scalars.extend(Scalar.model_validate_json(line) for line in data.decode().splitlines() if line)
        scalars.extend(scalar_buffer.read_buffered(conn, worker.id, selected_resolution))
    return ScalarSeriesResponse(resolution=selected_resolution, point_count=len(scalars), points=scalars)
