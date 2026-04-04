from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

import underfit_api.storage as storage_mod
from underfit_api.buffer import ScalarPoint, scalar_buffer
from underfit_api.config import config
from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import BufferedResponse, FlushedResponse, Scalar, UTCDatetime, Worker
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class ScalarInput(BaseModel):
    step: int | None = None
    values: dict[str, float]
    timestamp: UTCDatetime


class WriteScalarsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    worker_label: str = "0"
    start_line: int
    scalars: list[ScalarInput]


class FlushScalarsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    worker_label: str = "0"


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars")
def write_scalars(
    handle: str, project_name: str, run_name: str, body: WriteScalarsBody, conn: Conn, user: CurrentUser,
) -> BufferedResponse:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if not (worker := workers_repo.get(conn, run.id, body.worker_label)):
        raise HTTPException(404, "Worker not found")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if not body.scalars:
        return BufferedResponse()
    parsed = [ScalarPoint(step=s.step, values=s.values, timestamp=s.timestamp) for s in body.scalars]
    if (expected := scalar_buffer.append(
        conn, worker.id, run.id, body.worker_label, body.start_line, parsed,
    )) is not None:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": expected})
    scalar_buffer.flush_if_needed(conn, storage_mod.storage, worker.id)
    return BufferedResponse()


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars/flush")
def flush_scalars(
    handle: str, project_name: str, run_name: str, body: FlushScalarsBody, conn: Conn, user: CurrentUser,
) -> FlushedResponse:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if not (worker := workers_repo.get(conn, run.id, body.worker_label)):
        raise HTTPException(404, "Worker not found")
    scalar_buffer.flush(conn, storage_mod.storage, worker.id)
    return FlushedResponse()


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/scalars")
def read_scalars(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: MaybeUser,
    worker_label: Annotated[str, Query(alias="workerLabel")] = "0",
    resolution: Annotated[int | None, Query()] = None,
    max_points: Annotated[int | None, Query(alias="maxPoints")] = None,
) -> list[Scalar]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    if not (worker := workers_repo.get(conn, run.id, worker_label)):
        raise HTTPException(404, "Worker not found")
    if resolution is not None and max_points is not None:
        raise HTTPException(400, "Cannot specify both resolution and maxPoints")
    tier = resolution if resolution is not None else 0
    if max_points is not None:
        tier = _select_tier(conn, worker, max_points)
    max_tier = len(config.buffer.scalar_resolutions)
    if tier < 0 or tier > max_tier:
        raise HTTPException(400, f"Resolution must be 0-{max_tier}")
    return _read_tier(conn, worker, tier)


def _select_tier(conn: Conn, worker: Worker, max_points: int) -> int:
    for res in range(len(config.buffer.scalar_resolutions), -1, -1):
        if scalar_buffer.tier_line_count(conn, worker.id, res) >= max_points:
            return res
    return 0


def _read_tier(conn: Conn, worker: Worker, resolution: int) -> list[Scalar]:
    segments = scalar_seg_repo.list_by_resolution(conn, worker.id, resolution)
    scalars: list[Scalar] = []
    for seg in segments:
        data = storage_mod.storage.read(seg.storage_key, seg.byte_offset, seg.byte_count)
        for line in data.decode().splitlines():
            if line:
                parsed = json.loads(line)
                scalars.append(Scalar(
                    step=parsed.get("step"),
                    values=parsed["values"],
                    timestamp=datetime.fromisoformat(parsed["timestamp"].replace("Z", "+00:00")),
                ))
    for point in scalar_buffer.read_buffered(worker.id, resolution):
        scalars.append(Scalar(step=point.step, values=point.values, timestamp=point.timestamp))
    return scalars
