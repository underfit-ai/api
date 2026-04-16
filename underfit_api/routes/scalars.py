from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

import underfit_api.storage as storage_mod
from underfit_api.buffer import ScalarPoint, get_scalar_resolutions, scalar_buffer
from underfit_api.dependencies import Conn, CurrentWorker, MaybeUser
from underfit_api.models import BufferedResponse, Scalar, Worker
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class WriteScalarsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    start_line: int
    scalars: list[ScalarPoint]


def _total_line_count(conn: Conn, workers: list[Worker], resolution: int) -> int:
    return sum(scalar_buffer.resolution_line_count(conn, w.id, resolution) for w in workers)


def _select_resolution(conn: Conn, workers: list[Worker], max_points: int) -> int:
    for resolution in reversed(get_scalar_resolutions()):
        if 0 < _total_line_count(conn, workers, resolution) <= max_points:
            return resolution
    for resolution in reversed(get_scalar_resolutions()):
        if _total_line_count(conn, workers, resolution) > 0:
            return resolution  # Intentionally prefer full data at the coarsest available resolution.
    return 1


@router.post("/ingest/scalars")
def write_scalars(body: WriteScalarsBody, conn: Conn, worker: CurrentWorker) -> BufferedResponse:
    if not workers_repo.touch(conn, worker):
        raise HTTPException(401, "Unauthorized")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if not body.scalars:
        return BufferedResponse(next_start_line=body.start_line)
    if (expected := scalar_buffer.append(conn, worker, body.start_line, body.scalars)) is not None:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": expected})
    scalar_buffer.flush_if_needed(conn, storage_mod.storage, worker)
    assert (row := workers_repo.get_by_id(conn, worker)) is not None
    runs_repo.update_summary(conn, row.run_id, body.scalars)
    return BufferedResponse(next_start_line=body.start_line + len(body.scalars))


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars")
def read_scalars(
    handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser,
    resolution: Annotated[int | None, Query()] = None,
    max_points: Annotated[int | None, Query(alias="maxPoints")] = None,
) -> list[Scalar]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    if resolution is not None and max_points is not None:
        raise HTTPException(400, "Cannot specify both resolution and maxPoints")
    workers = workers_repo.list_by_run(conn, run.id)
    selected_resolution = resolution if resolution is not None else 1
    if max_points is not None:
        selected_resolution = _select_resolution(conn, workers, max_points)
    if _total_line_count(conn, workers, selected_resolution) == 0:
        raise HTTPException(404, "Resolution not found")

    scalars: list[Scalar] = []
    for worker in workers:
        buf_start = scalar_buffer.buffer_start_line(worker.id, selected_resolution)
        segments = scalar_seg_repo.list_by_resolution(conn, worker.id, selected_resolution)
        if buf_start is not None:
            segments = [s for s in segments if s.start_line < buf_start]
        for seg in segments:
            data = storage_mod.storage.read(f"{run.storage_key}/{seg.storage_key}")
            for line in data.decode().splitlines():
                if line:
                    parsed = json.loads(line)
                    scalars.append(Scalar(
                        step=parsed.get("step"),
                        values=parsed["values"],
                        timestamp=datetime.fromisoformat(parsed["timestamp"].replace("Z", "+00:00")),
                    ))
        for point in scalar_buffer.read_buffered(worker.id, selected_resolution):
            scalars.append(Scalar(step=point.step, values=point.values, timestamp=point.timestamp))
    return scalars
