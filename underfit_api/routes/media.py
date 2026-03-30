from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse

from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import Media
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import media as media_repo
from underfit_api.routes.resolvers import resolve_run
from underfit_api.storage import get_storage

router = APIRouter()

VALID_MEDIA_TYPES = {"image", "video", "audio", "html"}
MAX_JSON_BYTES = 65536


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media")
async def create_media(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: CurrentUser,
    metadata: Annotated[str, Form()],
    files: Annotated[list[UploadFile], File()],
) -> Media:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if not files:
        raise HTTPException(400, "No files provided")
    try:
        meta = json.loads(metadata)
    except json.JSONDecodeError as e:
        raise HTTPException(400, "Invalid metadata JSON") from e
    key = meta.get("key")
    if not key or not isinstance(key, str):
        raise HTTPException(400, "metadata.key is required")
    media_type = meta.get("type")
    if media_type not in VALID_MEDIA_TYPES:
        raise HTTPException(400, "Invalid media type")
    step = meta.get("step")
    extra_metadata = meta.get("metadata")
    if extra_metadata is not None and len(json.dumps(extra_metadata)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Metadata too large")
    media_id = uuid4()
    storage_key = f"{run.id}/media/{media_id}"
    storage = get_storage()
    for i, f in enumerate(files):
        async def _chunks(f: UploadFile = f) -> AsyncIterator[bytes]:
            while chunk := await f.read(262144):
                yield chunk
        await storage.write_stream(f"{storage_key}/{i}", _chunks())
    return media_repo.create(
        conn,
        run_id=run.id,
        key=key,
        step=step,
        media_type=media_type,
        storage_key=storage_key,
        count=len(files),
        metadata=extra_metadata,
    )


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media")
def list_media(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: MaybeUser,
    key: Annotated[str | None, Query()] = None,
    step: Annotated[int | None, Query()] = None,
) -> list[Media]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    return media_repo.list_by_run(conn, run.id, key=key, step=step)


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media/{media_id}/file")
def get_media_file(
    handle: str,
    project_name: str,
    run_name: str,
    media_id: UUID,
    conn: Conn,
    user: MaybeUser,
    index: Annotated[int, Query()] = 0,
) -> Response:
    run = resolve_run(conn, handle, project_name, run_name, user)
    record = media_repo.get_by_id(conn, media_id)
    if not record or record.run_id != run.id:
        raise HTTPException(404, "Media not found")
    if index < 0 or index >= record.count:
        raise HTTPException(400, "Index out of range")
    storage = get_storage()
    key = f"{record.storage_key}/{index}"
    if not storage.exists(key):
        raise HTTPException(404, "File not found")
    return StreamingResponse(storage.read_stream(key), media_type="application/octet-stream")
