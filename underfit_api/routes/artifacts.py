from __future__ import annotations

from contextlib import suppress
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from underfit_api.dependencies import Auth, Conn, Ctx, MaybeUser, RequireUser
from underfit_api.helpers import validate_json_size, validate_path
from underfit_api.models import Artifact, Body, OkResponse, Project
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import artifacts as artifacts_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.routes.resolvers import resolve_artifact, resolve_project, resolve_run

router = APIRouter()


class ManifestReference(BaseModel):
    url: str
    size: int | None = None
    sha256: str | None = None
    etag: str | None = None
    last_modified: str | None = None


class Manifest(BaseModel):
    files: list[str]
    references: list[ManifestReference] = []


class ArtifactDetail(Artifact):
    manifest: Manifest


class CreateArtifactBody(Body):
    step: int | None = None
    name: str
    type: str
    metadata: dict[str, object] | None = None


class FinalizeArtifactBody(Body):
    manifest: Manifest


def _create_artifact(conn: Conn, project_id: UUID, body: CreateArtifactBody, run_id: UUID | None = None) -> Artifact:
    if body.step is not None and run_id is None:
        raise HTTPException(400, "step requires run")
    validate_json_size(body.metadata, "Metadata")
    artifact_id = uuid4()
    return artifacts_repo.create(
        conn, artifact_id, project_id, run_id, body.step, body.name, body.type,
        f"artifacts/{artifact_id}", body.metadata,
    )


def _artifact_prefix(conn: Conn, artifact: Artifact, project: Project) -> str:
    if artifact.run_id:
        run = runs_repo.get_by_id(conn, artifact.run_id)
        assert run is not None
        return f"{run.storage_key}/{artifact.storage_key}"
    return f"{project.storage_key}/{artifact.storage_key}"


def _parse_range(header: str, size: int) -> tuple[int, int]:
    if not header.startswith("bytes=") or "," in header:
        raise HTTPException(416, "Invalid range")
    try:
        start_str, _, end_str = header[6:].partition("-")
        if not start_str:
            start, end = max(0, size - int(end_str)), size - 1
        else:
            start, end = int(start_str), int(end_str) if end_str else size - 1
    except ValueError as e:
        raise HTTPException(416, "Invalid range") from e
    if start > end or start >= size:
        raise HTTPException(416, "Invalid range")
    return start, min(end, size - 1)


@router.get("/accounts/{handle}/projects/{project_name}/artifacts")
def list_artifacts(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[Artifact]:
    project = resolve_project(conn, handle, project_name, user)
    return artifacts_repo.list_by_project(conn, project.id)


@router.post("/accounts/{handle}/projects/{project_name}/artifacts")
def create_project_artifact(
    handle: str, project_name: str, body: CreateArtifactBody, conn: Conn, user: RequireUser,
) -> Artifact:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project, user.id)
    return _create_artifact(conn, project.id, body)


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/artifacts")
def create_run_artifact(
    handle: str, project_name: str, run_name: str, body: CreateArtifactBody, conn: Conn, ctx: Ctx, user: RequireUser,
) -> Artifact:
    project, run = resolve_run(conn, ctx, handle, project_name, run_name, user)
    require_project_contributor(conn, project, user.id)
    return _create_artifact(conn, project.id, body, run.id)


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: UUID, conn: Conn, ctx: Ctx, user: MaybeUser) -> ArtifactDetail:
    project, artifact = resolve_artifact(conn, artifact_id, user)
    raw = ctx.storage.read(f"{_artifact_prefix(conn, artifact, project)}/manifest.json")
    return ArtifactDetail.model_validate({**artifact.__dict__, "manifest": Manifest.model_validate_json(raw)})


@router.put("/artifacts/{artifact_id}/files/{file_path:path}")
async def upload_file(artifact_id: UUID, file_path: str, request: Request, ctx: Ctx, auth: Auth) -> Artifact:
    with ctx.engine.begin() as conn:
        user = auth.require_user(conn)
        project, artifact = resolve_artifact(conn, artifact_id, user, require_finalized=False)
        require_project_contributor(conn, project, user.id)
        if not artifacts_repo.begin_upload(conn, artifact.id):
            raise HTTPException(409, "Artifact is finalized")
        key = f"{_artifact_prefix(conn, artifact, project)}/files/{validate_path(file_path)}"
    try:
        await ctx.storage.write_stream(key, request.stream())
    except Exception:
        with suppress(Exception):
            ctx.storage.delete(key)
        raise
    finally:
        with suppress(Exception), ctx.engine.begin() as conn:
            artifacts_repo.finish_upload(conn, artifact.id)
    return artifact


@router.head("/artifacts/{artifact_id}/files/{file_path:path}")
def head_file(artifact_id: UUID, file_path: str, conn: Conn, ctx: Ctx, user: MaybeUser) -> Response:
    project, artifact = resolve_artifact(conn, artifact_id, user, require_finalized=False)
    try:
        stat = ctx.storage.stat(f"{_artifact_prefix(conn, artifact, project)}/files/{validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    headers = {"Content-Length": str(stat.size)}
    if stat.last_modified is not None:
        headers["Last-Modified"] = stat.last_modified
    if stat.etag is not None:
        headers["ETag"] = stat.etag
    return Response(headers=headers, media_type="application/octet-stream")


@router.get("/artifacts/{artifact_id}/files/{file_path:path}")
def download_file(artifact_id: UUID, file_path: str, request: Request, ctx: Ctx, auth: Auth) -> Response:
    with ctx.engine.begin() as conn:
        user = auth.maybe_user(conn)
        project, artifact = resolve_artifact(conn, artifact_id, user, require_finalized=False)
        key = f"{_artifact_prefix(conn, artifact, project)}/files/{validate_path(file_path)}"
    try:
        size = ctx.storage.size(key)
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    headers = {"Accept-Ranges": "bytes"}
    range_header = request.headers.get("range")
    if range_header is None:
        return StreamingResponse(ctx.storage.read_stream(key), media_type="application/octet-stream", headers=headers)
    start, end = _parse_range(range_header, size)
    length = end - start + 1
    headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    headers["Content-Length"] = str(length)
    stream = ctx.storage.read_stream(key, byte_offset=start, byte_count=length)
    return StreamingResponse(stream, status_code=206, media_type="application/octet-stream", headers=headers)


@router.delete("/artifacts/{artifact_id}/files/{file_path:path}")
def delete_file(artifact_id: UUID, file_path: str, conn: Conn, ctx: Ctx, user: RequireUser) -> Artifact:
    project, artifact = resolve_artifact(conn, artifact_id, user, require_finalized=False)
    require_project_contributor(conn, project, user.id)
    if artifact.finalized_at is not None:
        raise HTTPException(409, "Artifact is finalized")
    try:
        ctx.storage.delete(f"{_artifact_prefix(conn, artifact, project)}/files/{validate_path(file_path)}")
    except FileNotFoundError as e:
        raise HTTPException(404, "File not found") from e
    return artifact


@router.post("/artifacts/{artifact_id}/finalize")
def finalize_artifact(artifact_id: UUID, body: FinalizeArtifactBody, ctx: Ctx, auth: Auth) -> OkResponse:
    with ctx.engine.begin() as conn:
        user = auth.require_user(conn)
        project, artifact = resolve_artifact(conn, artifact_id, user, require_finalized=False)
        require_project_contributor(conn, project, user.id)
        if artifact.finalized_at is not None:
            raise HTTPException(409, "Already finalized")
        artifact_prefix = _artifact_prefix(conn, artifact, project)
        if not artifacts_repo.begin_finalize(conn, artifact.id):
            raise HTTPException(409, "Uploads in progress")
    try:
        files = list(dict.fromkeys(validate_path(f) for f in body.manifest.files))
        refs = list({ref.url: ref for ref in body.manifest.references}.values())
        declared_paths = set(files)
        files_prefix = f"{artifact_prefix}/files"
        uploaded_paths = {path[len(files_prefix) + 1:] for path in ctx.storage.list_files(files_prefix)}
        missing = sorted(declared_paths - uploaded_paths)
        extra = sorted(uploaded_paths - declared_paths)
        if missing or extra:
            raise HTTPException(409, {"missing": missing, "extra": extra})
        stored_size_bytes = sum(ctx.storage.size(f"{files_prefix}/{path}") for path in uploaded_paths)
        manifest = Manifest(files=files, references=refs)
        ctx.storage.write(f"{artifact_prefix}/manifest.json", manifest.model_dump_json().encode())
        with ctx.engine.begin() as write_conn:
            artifacts_repo.finalize(write_conn, artifact.id, stored_size_bytes)
    except Exception:
        with suppress(Exception), ctx.engine.begin() as write_conn:
            artifacts_repo.cancel_finalize(write_conn, artifact.id)
        raise
    return OkResponse()
