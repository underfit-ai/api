from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from app.buffer import LogLine, log_buffer
from app.dependencies import Conn, CurrentUser, MaybeUser
from app.models import UTCDatetime
from app.permissions import require_project_contributor
from app.routes.resolvers import resolve_run
from app.schema import log_segments
from app.storage import get_storage

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
    if body.start_line < 0:
        raise HTTPException(400, "startLine must be >= 0")
    if not body.lines:
        return {"status": "buffered"}
    parsed = [
        LogLine(timestamp=ln.timestamp, content=ln.content)
        for ln in body.lines
    ]
    expected = log_buffer.append(conn, run.id, body.worker_id, body.start_line, parsed)
    if expected is not None:
        raise HTTPException(409, detail={"error": "Invalid startLine", "expectedStartLine": expected})
    storage = get_storage()
    log_buffer.flush_if_needed(conn, storage, run.id, body.worker_id)
    return {"status": "buffered"}


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/logs/flush")
def flush_logs(
    handle: str, project_name: str, run_name: str, body: FlushLogsBody, conn: Conn, user: CurrentUser,
) -> dict[str, str]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    storage = get_storage()
    log_buffer.flush(conn, storage, run.id, body.worker_id)
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
    entries: list[dict[str, object]] = []
    segments = conn.execute(
        log_segments.select()
        .where(
            log_segments.c.run_id == run.id,
            log_segments.c.worker_id == worker_id,
            log_segments.c.end_line > cursor,
            log_segments.c.start_line < cursor + count,
        )
        .order_by(log_segments.c.start_line),
    ).all()
    storage = get_storage()
    for seg in segments:
        data = storage.read(seg.storage_key, seg.byte_offset, seg.byte_count)
        all_lines = data.decode().split("\n")
        if all_lines and all_lines[-1] == "":
            all_lines = all_lines[:-1]
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
    if not entries:
        buffered = log_buffer.read_buffered(run.id, worker_id, cursor, count)
        if buffered:
            entries.append({
                "startLine": cursor,
                "endLine": cursor + len(buffered),
                "content": "\n".join(line.content for line in buffered),
                "startAt": buffered[0].timestamp.isoformat() + "Z",
                "endAt": buffered[-1].timestamp.isoformat() + "Z",
            })
    last_end = entries[-1]["endLine"] if entries else cursor
    next_cursor: int = last_end if isinstance(last_end, int) else cursor
    has_more = bool(entries) and next_cursor < log_buffer.get_end_line(conn, run.id, worker_id)
    return {"entries": entries, "nextCursor": next_cursor, "hasMore": has_more}
