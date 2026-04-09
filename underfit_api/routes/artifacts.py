from __future__ import annotations

import json
from uuid import UUID, uuid4
from zipfile import BadZipFile, ZipFile

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

import underfit_api.db as db
import underfit_api.storage as storage_mod
from underfit_api.dependencies import (
    AuthorizationHeader,
    Conn,
    CurrentUser,
    MaybeUser,
    SessionTokenCookie,
    get_current_user,
    get_maybe_user,
)
from underfit_api.helpers import validate_path
from underfit_api.models import Artifact, OkResponse
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import artifacts as artifacts_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.routes.resolvers import resolve_artifact, resolve_project, resolve_run

router = APIRouter()

MAX_JSON_BYTES = 65536


class ManifestReference(BaseModel):
    url: str
    size: int | None = None
    sha256: str | None = None
    etag: str | None = None
    last_modified: str | None = None


class Manifest(BaseModel):
    files: list[str]
    references: list[ManifestReference] = []


class CreateArtifactBody(BaseModel):
    step: int | None = None
    name: str
    type: str
    metadata: dict[str, object] | None = None


class FinalizeArtifactBody(BaseModel):
    manifest: Manifest


class ZipEntry(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    path: str
    size: int
    compressed_size: int


class _ZipStorageFile:
    def __init__(self, key: str) -> None:
        self._key = key
        self._size = storage_mod.storage.size(key)
        self._pos = 0

    def seek(self, offset: int, whence: int = 0) -> int:
        base = [0, self._pos, self._size][whence]
        self._pos = base + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, n: int = -1) -> bytes:
        remaining = self._size - self._pos
        count = remaining if n < 0 else min(n, remaining)
        if count <= 0:
            return b""
        data = storage_mod.storage.read(self._key, self._pos, count)
        self._pos += len(data)
        return data

    def seekable(self) -> bool:
        return True


def _open_zip(artifact_id: UUID, zip_path: str, conn: Conn, user: MaybeUser) -> ZipFile:
    artifact = resolve_artifact(conn, artifact_id, user)
    key = f"{_artifact_prefix(conn, artifact)}/files/{validate_path(zip_path)}"
    if not storage_mod.storage.exists(key):
        raise HTTPException(404, "File not found")
    try:
        return ZipFile(_ZipStorageFile(key))
    except BadZipFile as e:
        raise HTTPException(400, "Not a zip file") from e


def _create_artifact(
    conn: Conn, project_id: UUID, body: CreateArtifactBody, run_id: UUID | None = None,
) -> Artifact:
    if body.step is not None and run_id is None:
        raise HTTPException(400, "step requires run")
    if body.metadata is not None and len(json.dumps(body.metadata)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Metadata too large")
    artifact_id = uuid4()
    return artifacts_repo.create(
        conn, artifact_id, project_id, run_id, body.step, body.name, body.type,
        f"artifacts/{artifact_id}", body.metadata,
    )


def _storage_root(conn: Conn, artifact: Artifact) -> str:
    if artifact.run_id:
        run = runs_repo.get_by_id(conn, artifact.run_id)
        assert run is not None
        return run.storage_key
    project = projects_repo.get_by_id(conn, artifact.project_id)
    assert project is not None
    return project.storage_key


def _artifact_prefix(conn: Conn, artifact: Artifact) -> str:
    return f"{_storage_root(conn, artifact)}/{artifact.storage_key}"


@router.get("/accounts/{handle}/projects/{project_name}/artifacts")
def list_artifacts(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[Artifact]:
    project = resolve_project(conn, handle, project_name, user)
    return artifacts_repo.list_by_project(conn, project.id)


@router.post("/accounts/{handle}/projects/{project_name}/artifacts")
def create_project_artifact(
    handle: str, project_name: str, body: CreateArtifactBody, conn: Conn, user: CurrentUser,
) -> Artifact:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project, user.id)
    return _create_artifact(conn, project.id, body)


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/artifacts")
def create_run_artifact(
    handle: str, project_name: str, run_name: str, body: CreateArtifactBody, conn: Conn, user: CurrentUser,
) -> Artifact:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    return _create_artifact(conn, run.project_id, body, run.id)


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: UUID, conn: Conn, user: MaybeUser) -> Artifact:
    return resolve_artifact(conn, artifact_id, user)


@router.put("/artifacts/{artifact_id}/files/{file_path:path}")
async def upload_file(
    artifact_id: UUID, file_path: str, request: Request,
    authorization: AuthorizationHeader = None, session_token: SessionTokenCookie = None,
) -> Artifact:
    with db.engine.begin() as conn:
        user = get_current_user(conn, authorization, session_token)
        artifact = resolve_artifact(conn, artifact_id, user)
        require_project_contributor(conn, artifact.project_id, user.id)
        if artifact.finalized_at is not None:
            raise HTTPException(409, "Artifact is finalized")
        key = f"{_artifact_prefix(conn, artifact)}/files/{validate_path(file_path)}"
    await storage_mod.storage.write_stream(key, request.stream())
    return artifact


@router.head("/artifacts/{artifact_id}/files/{file_path:path}")
def head_file(artifact_id: UUID, file_path: str, conn: Conn, user: MaybeUser) -> Response:
    artifact = resolve_artifact(conn, artifact_id, user)
    try:
        stat = storage_mod.storage.stat(f"{_artifact_prefix(conn, artifact)}/files/{validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    headers = {"Content-Length": str(stat.size)}
    if stat.last_modified is not None:
        headers["Last-Modified"] = stat.last_modified
    if stat.etag is not None:
        headers["ETag"] = stat.etag
    return Response(headers=headers, media_type="application/octet-stream")


@router.get("/artifacts/{artifact_id}/files/{file_path:path}")
def download_file(
    artifact_id: UUID, file_path: str,
    authorization: AuthorizationHeader = None, session_token: SessionTokenCookie = None,
) -> Response:
    with db.engine.begin() as conn:
        user = get_maybe_user(conn, authorization, session_token)
        artifact = resolve_artifact(conn, artifact_id, user)
        key = f"{_artifact_prefix(conn, artifact)}/files/{validate_path(file_path)}"
    if not storage_mod.storage.exists(key):
        raise HTTPException(404, "File not found")
    return StreamingResponse(storage_mod.storage.read_stream(key), media_type="application/octet-stream")


@router.delete("/artifacts/{artifact_id}/files/{file_path:path}")
def delete_file(artifact_id: UUID, file_path: str, conn: Conn, user: CurrentUser) -> Artifact:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.finalized_at is not None:
        raise HTTPException(409, "Artifact is finalized")
    try:
        storage_mod.storage.delete(f"{_artifact_prefix(conn, artifact)}/files/{validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    return artifact


@router.get("/artifacts/{artifact_id}/zip/entries/{zip_path:path}", response_model=list[ZipEntry])
def list_zip_entries(artifact_id: UUID, zip_path: str, conn: Conn, user: MaybeUser) -> list[ZipEntry]:
    return [
        ZipEntry(path=info.filename, size=info.file_size, compressed_size=info.compress_size)
        for info in _open_zip(artifact_id, zip_path, conn, user).infolist() if not info.is_dir()
    ]


@router.get("/artifacts/{artifact_id}/zip/entry/{zip_path:path}")
def read_zip_entry(artifact_id: UUID, zip_path: str, entry: str, conn: Conn, user: MaybeUser) -> Response:
    try:
        return Response(
            content=_open_zip(artifact_id, zip_path, conn, user).read(entry), media_type="application/octet-stream",
        )
    except KeyError as e:
        raise HTTPException(404, "Entry not found") from e


@router.post("/artifacts/{artifact_id}/finalize")
def finalize_artifact(artifact_id: UUID, body: FinalizeArtifactBody, conn: Conn, user: CurrentUser) -> OkResponse:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.finalized_at is not None:
        raise HTTPException(409, "Already finalized")
    files = list(dict.fromkeys(validate_path(f) for f in body.manifest.files))
    refs = list({ref.url: ref for ref in body.manifest.references}.values())
    declared_paths = set(files)
    artifact_prefix = _artifact_prefix(conn, artifact)
    files_prefix = f"{artifact_prefix}/files"
    uploaded_paths = {path[len(files_prefix) + 1:] for path in storage_mod.storage.list_files(files_prefix)}
    missing = sorted(declared_paths - uploaded_paths)
    extra = sorted(uploaded_paths - declared_paths)
    if missing or extra:
        raise HTTPException(409, {"missing": missing, "extra": extra})
    stored_size_bytes = sum(storage_mod.storage.size(f"{files_prefix}/{path}") for path in uploaded_paths)
    manifest = Manifest(files=files, references=refs)
    storage_mod.storage.write(f"{artifact_prefix}/manifest.json", manifest.model_dump_json().encode())
    artifacts_repo.finalize(conn, artifact.id, stored_size_bytes)
    return OkResponse()
