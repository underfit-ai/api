from __future__ import annotations

from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from underfit_api.dependencies import Auth, Conn, Ctx, MaybeUser
from underfit_api.helpers import validate_path
from underfit_api.routes.resolvers import resolve_run
from underfit_api.storage import DirEntry

router = APIRouter()


def _storage_key(storage_key: str, path: str | None = None) -> str:
    if not path:
        return storage_key
    return f"{storage_key}/{validate_path(path)}"


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/files")
def list_files(
    handle: str, project_name: str, run_name: str, conn: Conn, ctx: Ctx, user: MaybeUser,
    path: Annotated[str | None, Query()] = None,
) -> list[dict[str, object]]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    entries: list[DirEntry] = ctx.storage.list_dir(_storage_key(run.storage_key, path))
    return [
        {"name": e.name, "isDirectory": e.is_directory, "size": e.size, "lastModified": e.last_modified}
        for e in entries
    ]


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/files/download")
def download_file(
    handle: str, project_name: str, run_name: str, path: Annotated[str, Query()], ctx: Ctx, auth: Auth,
) -> Response:
    if not path:
        raise HTTPException(400, "Path is required")
    with ctx.engine.begin() as conn:
        user = auth.maybe_user(conn)
        run = resolve_run(conn, handle, project_name, run_name, user)
        key = _storage_key(run.storage_key, path)
    if not ctx.storage.exists(key):
        raise HTTPException(404, "File not found")
    filename = quote(path.rsplit("/", 1)[-1])
    return StreamingResponse(
        ctx.storage.read_stream(key),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"},
    )
