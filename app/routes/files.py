from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.dependencies import Conn, MaybeUser
from app.routes.resolvers import resolve_run
from app.storage import DirEntry, get_storage

router = APIRouter()


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/files")
def list_files(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: MaybeUser,
    path: Annotated[str | None, Query()] = None,
) -> list[dict[str, object]]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    storage = get_storage()
    prefix = str(run.id)
    if path:
        prefix = f"{prefix}/{path}"
    entries: list[DirEntry] = storage.list_dir(prefix)
    return [
        {"name": e.name, "isDirectory": e.is_directory, "size": e.size, "lastModified": e.last_modified}
        for e in entries
    ]


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/files/download")
def download_file(
    handle: str,
    project_name: str,
    run_name: str,
    conn: Conn,
    user: MaybeUser,
    path: Annotated[str, Query()],
) -> Response:
    if not path:
        raise HTTPException(400, "Path is required")
    run = resolve_run(conn, handle, project_name, run_name, user)
    storage = get_storage()
    key = f"{run.id}/{path}"
    if not storage.exists(key):
        raise HTTPException(404, "File not found")
    data = storage.read(key)
    filename = path.rsplit("/", 1)[-1]
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
