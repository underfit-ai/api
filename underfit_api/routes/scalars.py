from __future__ import annotations

from datetime import datetime
from typing import Annotated

import sqlalchemy as sa
from fastapi import APIRouter, HTTPException, Query

from underfit_api.buffers import scalars as scalar_buffer
from underfit_api.config import config
from underfit_api.dependencies import Conn, Ctx, CurrentWorker, MaybeUser
from underfit_api.models import Body, BufferedResponse, Scalar, Schema, UTCDatetime
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.routes.resolvers import resolve_run
from underfit_api.schema import run_metric_keys

router = APIRouter()


class ScalarAxis(Schema):
    steps: list[int | None]
    timestamps: list[UTCDatetime]


class ScalarSeries(Schema):
    axis: int
    values: list[float]


class ScalarSeriesResponse(Schema):
    resolution: int
    axes: list[ScalarAxis]
    series: dict[str, ScalarSeries]


class WriteScalarsBody(Body):
    start_line: int
    scalars: list[Scalar]


def _build_response(resolution: int, scalars: list[Scalar]) -> ScalarSeriesResponse:
    by_key: dict[str, list[tuple[int | None, datetime, float]]] = {}
    for s in scalars:
        for k, v in s.values.items():
            by_key.setdefault(k, []).append((s.step, s.timestamp, v))
    axes: list[ScalarAxis] = []
    axis_ids: dict[tuple[tuple[int | None, ...], tuple[datetime, ...]], int] = {}
    series: dict[str, ScalarSeries] = {}
    for key, pts in by_key.items():
        sig = (tuple(p[0] for p in pts), tuple(p[1] for p in pts))
        if sig not in axis_ids:
            axis_ids[sig] = len(axes)
            axes.append(ScalarAxis(steps=list(sig[0]), timestamps=list(sig[1])))
        series[key] = ScalarSeries(axis=axis_ids[sig], values=[p[2] for p in pts])
    return ScalarSeriesResponse(resolution=resolution, axes=axes, series=series)


def _select_resolution(counts: dict[int, int], target_points: int) -> int:
    for resolution in config.buffer.scalar_resolutions:
        if counts[resolution] <= target_points:
            return resolution
    return max(config.buffer.scalar_resolutions)


@router.post("/ingest/scalars")
def write_scalars(body: WriteScalarsBody, conn: Conn, worker_id: CurrentWorker) -> BufferedResponse:
    if not workers_repo.touch(conn, worker_id):
        raise HTTPException(401, "Unauthorized")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if body.scalars:
        scalar_buffer.append(conn, worker_id, body.start_line, body.scalars)
    return BufferedResponse(next_start_line=body.start_line + len(body.scalars))


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars")
def read_scalars(
    handle: str, project_name: str, run_name: str, conn: Conn, ctx: Ctx, user: MaybeUser,
    target_points: Annotated[int, Query(alias="targetPoints", gt=0)] = 1000,
    start_time: Annotated[UTCDatetime | None, Query(alias="startTime")] = None,
    end_time: Annotated[UTCDatetime | None, Query(alias="endTime")] = None,
    keys: Annotated[list[str] | None, Query()] = None,
) -> ScalarSeriesResponse:
    _, run = resolve_run(conn, ctx, handle, project_name, run_name, user)
    workers = workers_repo.list_by_run(conn, run.id)
    if keys:
        owned = set(conn.execute(sa.select(run_metric_keys.c.worker_id).where(
            run_metric_keys.c.run_id == run.id, run_metric_keys.c.key.in_(keys),
        )).scalars().all())
        workers = [w for w in workers if w.id in owned]
    worker_ids = [w.id for w in workers]
    counts = scalar_buffer.window_line_counts(conn, worker_ids, start_time, end_time)
    resolution = _select_resolution(counts, target_points)

    scalars: list[Scalar] = []
    for worker in workers:
        for seg in scalar_seg_repo.list_by_resolution(conn, worker.id, resolution, start_time, end_time):
            data = ctx.storage.read(f"{run.storage_key}/{seg.storage_key}")
            scalars.extend(Scalar.model_validate_json(line) for line in data.decode().splitlines() if line)
        scalars.extend(scalar_buffer.read_buffered(
            conn, worker.id, resolution, start_time=start_time, end_time=end_time,
        ))
    if start_time or end_time or keys:
        filtered: list[Scalar] = []
        for s in scalars:
            if (start_time and s.timestamp < start_time) or (end_time and s.timestamp > end_time):
                continue
            values = {k: v for k, v in s.values.items() if not keys or k in keys}
            if values:
                filtered.append(Scalar(step=s.step, timestamp=s.timestamp, values=values))
        scalars = filtered
    return _build_response(resolution, scalars)
