from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

import underfit_api.storage as storage_mod
from underfit_api.buffer import LogLine, log_buffer
from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import UTCDatetime
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class LogLineInput(BaseModel):
    timestamp: UTCDatetime
    content: str


class WriteLogsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    worker_id: str
    start_line: int
    lines: list[LogLineInput]


class FlushLogsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    worker_id: str


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/logs")
def write_logs(
    handle: str, project_name: str, run_name: str, body: WriteLogsBody, conn: Conn, user: CurrentUser,
) -> dict[str, str]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if not (worker := workers_repo.get(conn, run.id, body.worker_id)):
        raise HTTPException(404, "Worker not found")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if not body.lines:
        return {"status": "buffered"}
    parsed = [LogLine(timestamp=ln.timestamp, content=ln.content) for ln in body.lines]
    expected = log_buffer.append(conn, worker.id, run.id, body.worker_id, body.start_line, parsed)
    if expected is not None:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": expected})
    log_buffer.flush_if_needed(conn, storage_mod.storage, worker.id)
    return {"status": "buffered"}


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/logs/flush")
def flush_logs(
    handle: str, project_name: str, run_name: str, body: FlushLogsBody, conn: Conn, user: CurrentUser,
) -> dict[str, str]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if not (worker := workers_repo.get(conn, run.id, body.worker_id)):
        raise HTTPException(404, "Worker not found")
    log_buffer.flush(conn, storage_mod.storage, worker.id)
    return {"status": "flushed"}


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/logs")
def read_logs(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: MaybeUser,
    worker_id: Annotated[str, Query(alias="workerId")],
    cursor: Annotated[int, Query()] = 0,
    count: Annotated[int, Query()] = 10000,
) -> dict[str, object]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    if not (worker := workers_repo.get(conn, run.id, worker_id)):
        raise HTTPException(404, "Worker not found")
    entries: list[dict[str, object]] = []
    segments = log_seg_repo.list_for_range(conn, worker.id, cursor, count)
    for seg in segments:
        data = storage_mod.storage.read(seg.storage_key, seg.byte_offset, seg.byte_count)
        all_lines = data.decode().splitlines()
        seg_start = max(cursor, seg.start_line)
        seg_end = min(cursor + count, seg.end_line)
        offset = seg_start - seg.start_line
        clipped = all_lines[offset:offset + (seg_end - seg_start)]
        entries.append({
            "startLine": seg_start,
            "endLine": seg_end,
            "content": "\n".join(clipped),
            "startAt": seg.start_at.isoformat() + "Z",
            "endAt": seg.end_at.isoformat() + "Z",
        })
    if not entries and (buffered := log_buffer.read_buffered(worker.id, cursor, count)):
        entries.append({
            "startLine": cursor,
            "endLine": cursor + len(buffered),
            "content": "\n".join(line.content for line in buffered),
            "startAt": buffered[0].timestamp.isoformat() + "Z",
            "endAt": buffered[-1].timestamp.isoformat() + "Z",
        })
    last_end = entries[-1]["endLine"] if entries else cursor
    next_cursor = last_end if isinstance(last_end, int) else cursor
    has_more = bool(entries) and next_cursor < log_buffer.get_end_line(conn, worker.id)
    return {"entries": entries, "nextCursor": next_cursor, "hasMore": has_more}
