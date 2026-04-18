from __future__ import annotations

import mimetypes
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Json

from underfit_api.dependencies import Auth, Conn, Ctx, CurrentWorker, MaybeUser
from underfit_api.helpers import as_conflict, validate_json_size, validate_path
from underfit_api.models import Media, MediaType
from underfit_api.repositories import media as media_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


class CreateMediaMetadata(BaseModel):
    key: str
    step: int
    type: MediaType
    metadata: dict[str, object] | None = None


MediaMetadata = Annotated[Json[CreateMediaMetadata], Form()]
MediaFiles = Annotated[list[UploadFile], File()]


def _content_type_is_valid(media_type: str, content_type: str) -> bool:
    if media_type == "html":
        return content_type in {"text/html", "application/xhtml+xml"}
    return content_type.startswith(f"{media_type}/")


@router.post("/ingest/media")
async def create_media(worker: CurrentWorker, ctx: Ctx, metadata: MediaMetadata, files: MediaFiles) -> list[Media]:
    if not files:
        raise HTTPException(400, "No files provided")
    if any(f.content_type and not _content_type_is_valid(metadata.type.value, f.content_type) for f in files):
        raise HTTPException(400, "Files must all match the declared media type")
    validate_json_size(metadata.metadata, "Metadata")
    with ctx.engine.begin() as conn:
        if not workers_repo.touch(conn, worker):
            raise HTTPException(401, "Unauthorized")
        assert (run_worker := workers_repo.get_by_id(conn, worker)) is not None
        key = validate_path(metadata.key)
        run_storage_key = run_worker.run_storage_key
        run_id = run_worker.run_id
    storage_keys: list[str] = []
    try:
        with ctx.engine.begin() as conn, as_conflict(conn, "Media already exists for this type/key/step"):
            rows = []
            for i, f in enumerate(files):
                ext = mimetypes.guess_extension(f.content_type or "") or ".bin"
                storage_key = f"media/{metadata.type.value}/{key}_{metadata.step}_{i}{ext}"
                storage_keys.append(storage_key)
                rows.append(media_repo.create(
                    conn, run_id=run_id, key=key, step=metadata.step, media_type=metadata.type,
                    index=i, storage_key=storage_key, metadata=metadata.metadata,
                ))
        for f, storage_key in zip(files, storage_keys):
            async def _chunks(f: UploadFile = f) -> AsyncIterator[bytes]:
                while chunk := await f.read(262144):
                    yield chunk
            await ctx.storage.write_stream(f"{run_storage_key}/{storage_key}", _chunks())
        with ctx.engine.begin() as conn:
            media_repo.finalize_group(conn, run_id, metadata.type, key, metadata.step)
        return [row.model_copy(update={"finalized": True}) for row in rows]
    except Exception:
        with suppress(Exception), ctx.engine.begin() as conn:
            media_repo.delete_group(conn, run_id, metadata.type, key, metadata.step)
        for storage_key in storage_keys:
            with suppress(Exception):
                ctx.storage.delete(f"{run_storage_key}/{storage_key}")
        raise


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media")
def list_media(
    handle: str, project_name: str, run_name: str, conn: Conn, ctx: Ctx, user: MaybeUser,
    key: Annotated[str | None, Query()] = None, step: Annotated[int | None, Query()] = None,
) -> list[Media]:
    run = resolve_run(conn, ctx, handle, project_name, run_name, user)
    return media_repo.list_by_run(conn, run.id, key=key, step=step)


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/media/{media_id}/file")
def get_media_file(handle: str, project_name: str, run_name: str, media_id: UUID, ctx: Ctx, auth: Auth) -> Response:
    with ctx.engine.begin() as conn:
        user = auth.maybe_user(conn)
        run = resolve_run(conn, ctx, handle, project_name, run_name, user)
        record = media_repo.get_by_id(conn, media_id)
        if not record or record.run_id != run.id:
            raise HTTPException(404, "Media not found")
        key = f"{run.storage_key}/{record.storage_key}"
    if not ctx.storage.exists(key):
        raise HTTPException(404, "File not found")
    return StreamingResponse(ctx.storage.read_stream(key), media_type="application/octet-stream")
