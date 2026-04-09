from __future__ import annotations

import json
import mimetypes
from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Json

import underfit_api.storage as storage_mod
from underfit_api.dependencies import Conn, CurrentWorker, MaybeUser
from underfit_api.helpers import as_conflict, validate_path
from underfit_api.models import Media, MediaType
from underfit_api.repositories import media as media_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()

MAX_JSON_BYTES = 65536


class CreateMediaMetadata(BaseModel):
    key: str
    step: int
    type: MediaType
    metadata: dict[str, object] | None = None


MediaMetadata = Annotated[Json[CreateMediaMetadata], Form()]
MediaFiles = Annotated[list[UploadFile], File()]


@router.post("/ingest/media")
async def create_media(conn: Conn, worker: CurrentWorker, metadata: MediaMetadata, files: MediaFiles) -> Media:
    if not workers_repo.touch(conn, worker):
        raise HTTPException(401, "Unauthorized")
    run_worker = workers_repo.get_by_id(conn, worker)
    assert run_worker is not None
    run = runs_repo.get_by_id(conn, run_worker.run_id)
    assert run is not None
    if not files:
        raise HTTPException(400, "No files provided")
    if metadata.metadata is not None and len(json.dumps(metadata.metadata)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Metadata too large")
    key = validate_path(metadata.key)
    if any(
        media.type == metadata.type and media.key == key and media.step == metadata.step
        for media in media_repo.list_by_run(conn, run.id, key=key)
    ):
        raise HTTPException(409, "Media already exists for this type/key/step")
    prefix, _, name = key.rpartition("/")
    ext = mimetypes.guess_extension(files[0].content_type or "") or ".bin"
    if any((mimetypes.guess_extension(f.content_type or "") or ".bin") != ext for f in files[1:]):
        raise HTTPException(400, "All media files must use the same content type")
    storage_prefix = "/".join(["media", metadata.type, *([prefix] if prefix else []), name])
    storage_prefix = f"{storage_prefix}_{metadata.step}"
    for i, f in enumerate(files):
        async def _chunks(f: UploadFile = f) -> AsyncIterator[bytes]:
            while chunk := await f.read(262144):
                yield chunk
        await storage_mod.storage.write_stream(f"{run.storage_key}/{storage_prefix}_{i}{ext}", _chunks())
    with as_conflict(conn, "Media already exists for this type/key/step"):
        return media_repo.create(
            conn,
            run_id=run_worker.run_id,
            key=metadata.key,
            step=metadata.step,
            media_type=metadata.type,
            storage_prefix=storage_prefix,
            ext=ext,
            count=len(files),
            metadata=metadata.metadata,
        )


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media")
def list_media(
    handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser,
    key: Annotated[str | None, Query()] = None, step: Annotated[int | None, Query()] = None,
) -> list[Media]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    return media_repo.list_by_run(conn, run.id, key=key, step=step)


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media/{media_id}/file")
def get_media_file(
    handle: str, project_name: str, run_name: str, media_id: UUID, conn: Conn, user: MaybeUser,
    index: Annotated[int, Query()] = 0,
) -> Response:
    run = resolve_run(conn, handle, project_name, run_name, user)
    record = media_repo.get_by_id(conn, media_id)
    if not record or record.run_id != run.id:
        raise HTTPException(404, "Media not found")
    if index < 0 or index >= record.count:
        raise HTTPException(400, "Index out of range")
    key = f"{run.storage_key}/{record.storage_prefix}_{index}{record.ext}"
    if not key or not storage_mod.storage.exists(key):
        raise HTTPException(404, "File not found")
    return StreamingResponse(storage_mod.storage.read_stream(key), media_type="application/octet-stream")
