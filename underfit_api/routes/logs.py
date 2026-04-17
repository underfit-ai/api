from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from underfit_api.buffer import BadStartLineError, LogLine
from underfit_api.dependencies import Conn, Ctx, CurrentWorker, MaybeUser
from underfit_api.models import BufferedResponse, LogEntriesResponse, LogEntry
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class WriteLogsBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    start_line: int
    lines: list[LogLine]


@router.post("/ingest/logs")
def write_logs(body: WriteLogsBody, conn: Conn, ctx: Ctx, worker: CurrentWorker) -> BufferedResponse:
    if not workers_repo.touch(conn, worker):
        raise HTTPException(401, "Unauthorized")
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if any("\n" in ln.content or "\r" in ln.content for ln in body.lines):
        raise HTTPException(400, "Log lines must not contain newlines")
    if not body.lines:
        return BufferedResponse(next_start_line=body.start_line)
    try:
        ctx.log_buffer.append(conn, worker, body.start_line, body.lines)
    except BadStartLineError as e:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": e.expected}) from e
    ctx.log_buffer.flush_if_needed(conn, ctx.storage, worker)
    return BufferedResponse(next_start_line=body.start_line + len(body.lines))


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/logs/{worker_label}")
def read_logs(
    handle: str, project_name: str, run_name: str, worker_label: str, conn: Conn, ctx: Ctx, user: MaybeUser,
    cursor: Annotated[int, Query()] = 0, count: Annotated[int, Query()] = 10000,
) -> LogEntriesResponse:
    run = resolve_run(conn, handle, project_name, run_name, user)
    if not (worker := workers_repo.get(conn, run.id, worker_label)):
        raise HTTPException(404, "Worker not found")
    entries: list[LogEntry] = []
    buf_start = ctx.log_buffer.buffer_start_line(worker.id)
    segments = log_seg_repo.list_for_range(conn, worker.id, cursor, count)
    if buf_start is not None:
        segments = [s for s in segments if s.start_line < buf_start]
    for seg in segments:
        data = ctx.storage.read(f"{run.storage_key}/{seg.storage_key}")
        all_lines = data.decode().splitlines()
        seg_start = max(cursor, seg.start_line)
        seg_end = min(cursor + count, seg.end_line)
        offset = seg_start - seg.start_line
        clipped = all_lines[offset:offset + (seg_end - seg_start)]
        entries.append(LogEntry(
            start_line=seg_start, end_line=seg_end, content="\n".join(clipped),
            start_at=seg.start_at, end_at=seg.end_at,
        ))
    if not entries and (buffered := ctx.log_buffer.read_buffered(worker.id, cursor, count)):
        entries.append(LogEntry(
            start_line=cursor, end_line=cursor + len(buffered),
            content="\n".join(line.content for line in buffered),
            start_at=buffered[0].timestamp, end_at=buffered[-1].timestamp,
        ))
    next_cursor = entries[-1].end_line if entries else cursor
    has_more = bool(entries) and next_cursor < ctx.log_buffer.get_end_line(conn, worker.id)
    return LogEntriesResponse(entries=entries, next_cursor=next_cursor, has_more=has_more)
