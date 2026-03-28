from __future__ import annotations

import json
import re
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from app.dependencies import Conn, CurrentUser, MaybeUser
from app.models import Artifact
from app.permissions import require_project_contributor
from app.repositories import artifacts as artifacts_repo
from app.repositories import runs as runs_repo
from app.routes.resolvers import resolve_artifact, resolve_project
from app.storage import get_storage

router = APIRouter()

MAX_JSON_BYTES = 65536
_BAD_PATH_RE = re.compile(r"[\x00]")


class ManifestFile(BaseModel):
    path: str
    size: int | None = None
    sha256: str | None = None


class Manifest(BaseModel):
    files: list[ManifestFile]
    references: list[str] | None = None


class CreateArtifactBody(BaseModel):
    run_id: UUID | None = None
    step: int | None = None
    name: str
    type: str
    metadata: dict[str, object] | None = None
    manifest: Manifest


def _validate_path(path: str) -> str:
    path = path.replace("\\", "/")
    if _BAD_PATH_RE.search(path):
        raise HTTPException(400, "Invalid path")
    if path.startswith(("/", "\\")):
        raise HTTPException(400, "Invalid path: leading slash")
    segments = path.split("/")
    if any(s in (".", "..") for s in segments):
        raise HTTPException(400, "Invalid path: dot segments")
    return "/".join(s for s in segments if s)


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
        run_id = run.id
    if body.step is not None and run_id is None:
        raise HTTPException(400, "step requires runId")
    if body.metadata is not None and len(json.dumps(body.metadata)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Metadata too large")
    seen: dict[str, ManifestFile] = {}
    for f in body.manifest.files:
        normalized = _validate_path(f.path)
        seen[normalized] = ManifestFile(path=normalized, size=f.size, sha256=f.sha256)
    deduped_files = list(seen.values())
    refs = list(dict.fromkeys(body.manifest.references)) if body.manifest.references else []
    base = f"{run_id}/artifacts" if run_id else f"{project.id}/artifacts"
    artifact_id = uuid4()
    storage_key = f"{base}/{artifact_id}"
    storage = get_storage()
    manifest_data = {
        "files": [{"path": f.path, "size": f.size, "sha256": f.sha256} for f in deduped_files],
        "references": refs,
    }
    storage.write(f"{storage_key}/manifest.json", json.dumps(manifest_data).encode())
    return artifacts_repo.create(
        conn,
        project_id=project.id,
        run_id=run_id,
        step=body.step,
        name=body.name,
        artifact_type=body.type,
        storage_key=storage_key,
        declared_file_count=len(deduped_files),
        metadata=body.metadata,
    )


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: UUID, conn: Conn, user: MaybeUser) -> Artifact:
    return resolve_artifact(conn, artifact_id, user)


@router.put("/artifacts/{artifact_id}/files/{file_path:path}")
async def upload_file(
    artifact_id: UUID, file_path: str, request: Request, conn: Conn, user: CurrentUser,
) -> Artifact:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.status == "finalized":
        raise HTTPException(409, "Artifact is finalized")
    normalized = _validate_path(file_path)
    storage = get_storage()
    manifest_bytes = storage.read(f"{artifact.storage_key}/manifest.json")
    manifest = json.loads(manifest_bytes)
    declared_paths = {f["path"] for f in manifest["files"]}
    if normalized not in declared_paths:
        raise HTTPException(400, "Path not in manifest")
    await storage.write_stream(f"{artifact.storage_key}/files/{normalized}", request.stream())
    marker_key = f"{artifact.storage_key}/.uploaded/{normalized}"
    if not storage.exists(marker_key):
        storage.write(marker_key, b"")
        artifact = artifacts_repo.increment_uploaded(conn, artifact.id)
        assert artifact is not None
    return artifact


@router.get("/artifacts/{artifact_id}/files/{file_path:path}")
def download_file(artifact_id: UUID, file_path: str, conn: Conn, user: MaybeUser) -> Response:
    artifact = resolve_artifact(conn, artifact_id, user)
    normalized = _validate_path(file_path)
    storage = get_storage()
    key = f"{artifact.storage_key}/files/{normalized}"
    if not storage.exists(key):
        raise HTTPException(404, "File not found")
    return StreamingResponse(storage.read_stream(key), media_type="application/octet-stream")


@router.post("/artifacts/{artifact_id}/finalize")
def finalize_artifact(artifact_id: UUID, conn: Conn, user: CurrentUser) -> dict[str, bool]:
    artifact = resolve_artifact(conn, artifact_id, user)
    require_project_contributor(conn, artifact.project_id, user.id)
    if artifact.status == "finalized":
        raise HTTPException(409, "Already finalized")
    if artifact.uploaded_file_count < artifact.declared_file_count:
        raise HTTPException(409, "Not all files uploaded")
    artifacts_repo.finalize(conn, artifact.id)
    return {"success": True}
