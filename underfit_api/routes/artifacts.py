from __future__ import annotations

import json
import unicodedata
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

import underfit_api.storage as storage_mod
from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import Artifact
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import artifacts as artifacts_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.routes.resolvers import resolve_artifact, resolve_project

router = APIRouter()

MAX_JSON_BYTES = 65536
MAX_PATH_BYTES = 1024
MAX_PATH_SEGMENT_BYTES = 255


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
    run_id: UUID | None = None
    step: int | None = None
    name: str
    type: str
    metadata: dict[str, object] | None = None


class FinalizeArtifactBody(BaseModel):
    manifest: Manifest


def _validate_path(path: str) -> str:
    path = unicodedata.normalize("NFC", path)
    if not path or path.startswith("/"):
        raise HTTPException(400, "Invalid path")
    if any(ch == "\\" or (ch.isspace() and ch != " ") or unicodedata.category(ch).startswith("C") for ch in path):
        raise HTTPException(400, "Invalid path")
    if len(path.encode()) > MAX_PATH_BYTES:
        raise HTTPException(400, "Invalid path: too long")
    for segment in path.split("/"):
        if not segment or segment in (".", "..") or segment != segment.strip(" ") or segment.endswith("."):
            raise HTTPException(400, "Invalid path segment")
        if len(segment.encode()) > MAX_PATH_SEGMENT_BYTES:
            raise HTTPException(400, "Invalid path: segment too long")
    return path


@router.get("/accounts/{handle}/projects/{project_name}/artifacts")
def list_artifacts(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[Artifact]:
    project = resolve_project(conn, handle, project_name, user)
    return artifacts_repo.list_by_project(conn, project.id)


@router.post("/accounts/{handle}/projects/{project_name}/artifacts")
def create_artifact(
    handle: str, project_name: str, body: CreateArtifactBody, conn: Conn, user: CurrentUser,
) -> Artifact:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project.id, user.id)
    run_id = None
    if body.run_id is not None:
        run = runs_repo.get_by_id(conn, body.run_id)
        if not run:
            raise HTTPException(404, "Run not found")
        if run.project_id != project.id:
            raise HTTPException(400, "Run not in project")
        run_id = run.id
    if body.step is not None and run_id is None:
        raise HTTPException(400, "step requires runId")
    if body.metadata is not None and len(json.dumps(body.metadata)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Metadata too large")
    base = f"{run_id}/artifacts" if run_id else f"{project.id}/artifacts"
    artifact_id = uuid4()
    storage_key = f"{base}/{artifact_id}"
    return artifacts_repo.create(
        conn,
        artifact_id=artifact_id,
        project_id=project.id,
        run_id=run_id,
        step=body.step,
        name=body.name,
        artifact_type=body.type,
        storage_key=storage_key,
        metadata=body.metadata,
    )


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: UUID, conn: Conn, user: MaybeUser) -> Artifact:
    return resolve_artifact(conn, artifact_id, user)


@router.put("/artifacts/{artifact_id}/files/{file_path:path}")
async def upload_file(artifact_id: UUID, file_path: str, request: Request, conn: Conn, user: CurrentUser) -> Artifact:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.finalized_at is not None:
        raise HTTPException(409, "Artifact is finalized")
    key = f"{artifact.storage_key}/files/{_validate_path(file_path)}"
    await storage_mod.storage.write_stream(key, request.stream())
    return artifact


@router.head("/artifacts/{artifact_id}/files/{file_path:path}")
def head_file(artifact_id: UUID, file_path: str, conn: Conn, user: MaybeUser) -> Response:
    artifact = resolve_artifact(conn, artifact_id, user)
    try:
        stat = storage_mod.storage.stat(f"{artifact.storage_key}/files/{_validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    headers = {"Content-Length": str(stat.size)}
    if stat.last_modified is not None:
        headers["Last-Modified"] = stat.last_modified
    if stat.etag is not None:
        headers["ETag"] = stat.etag
    return Response(headers=headers, media_type="application/octet-stream")


@router.get("/artifacts/{artifact_id}/files/{file_path:path}")
def download_file(artifact_id: UUID, file_path: str, conn: Conn, user: MaybeUser) -> Response:
    artifact = resolve_artifact(conn, artifact_id, user)
    key = f"{artifact.storage_key}/files/{_validate_path(file_path)}"
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
        storage_mod.storage.delete(f"{artifact.storage_key}/files/{_validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    return artifact


@router.post("/artifacts/{artifact_id}/finalize")
def finalize_artifact(artifact_id: UUID, body: FinalizeArtifactBody, conn: Conn, user: CurrentUser) -> dict[str, bool]:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.finalized_at is not None:
        raise HTTPException(409, "Already finalized")
    files = list(dict.fromkeys(_validate_path(f) for f in body.manifest.files))
    refs = list({ref.url: ref for ref in body.manifest.references}.values())
    declared_paths = set(files)
    files_prefix = f"{artifact.storage_key}/files"
    uploaded_paths = {path[len(files_prefix) + 1:] for path in storage_mod.storage.list_files(files_prefix)}
    missing = sorted(declared_paths - uploaded_paths)
    extra = sorted(uploaded_paths - declared_paths)
    if missing or extra:
        raise HTTPException(409, {"missing": missing, "extra": extra})
    stored_size_bytes = sum(storage_mod.storage.size(f"{files_prefix}/{path}") for path in uploaded_paths)
    manifest = Manifest(files=files, references=refs)
    storage_mod.storage.write(f"{artifact.storage_key}/manifest.json", manifest.model_dump_json().encode())
    artifacts_repo.finalize(conn, artifact.id, stored_size_bytes)
    return {"success": True}
