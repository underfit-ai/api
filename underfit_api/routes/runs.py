from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from underfit_api.auth import create_worker_token
from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import Run, RunStatus
from underfit_api.permissions import can_view_project, require_project_contributor
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_project, resolve_run

router = APIRouter()

MAX_JSON_BYTES = 65536
RUN_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"


class CreateRunBody(BaseModel):
    name: str | None = Field(default=None, pattern=RUN_NAME_PATTERN)
    worker_label: str = "0"
    status: RunStatus = RunStatus.QUEUED
    config: dict[str, object] | None = None


class UpdateRunBody(BaseModel):
    status: RunStatus | None = None
    config: dict[str, object] | None = None


def _validate_config(config: dict[str, object] | None) -> None:
    if config is not None and len(json.dumps(config)) > MAX_JSON_BYTES:
        raise HTTPException(400, "Config too large")


@router.get("/users/{handle}/runs")
def list_user_runs(handle: str, conn: Conn, user: MaybeUser) -> list[Run]:
    if not (target := users_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "User not found")
    runs = runs_repo.list_by_user(conn, target.id)
    return [r for r in runs if can_view_project(conn, r.project_id, user.id if user else None)]


@router.get("/accounts/{handle}/projects/{project_name}/runs")
def list_project_runs(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[Run]:
    project = resolve_project(conn, handle, project_name, user)
    return runs_repo.list_by_project(conn, project.id)


@router.post("/accounts/{handle}/projects/{project_name}/runs")
def create_run(handle: str, project_name: str, body: CreateRunBody, conn: Conn, user: CurrentUser) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project.id, user.id)
    _validate_config(body.config)
    name = body.name.lower() if body.name is not None else None
    if not (run := runs_repo.create(conn, project.id, user.id, body.status, body.config, name=name)):
        if name is not None:
            raise HTTPException(409, "Run already exists")
        raise HTTPException(500, "Unable to allocate unique run name")
    worker = workers_repo.create(conn, run.id, body.worker_label, body.status, is_primary=True)
    return run.model_copy(update={"worker_token": create_worker_token(worker.id)})


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def get_run(handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser) -> Run:
    return resolve_run(conn, handle, project_name, run_name, user)


@router.put("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def update_run(
    handle: str, project_name: str, run_name: str, body: UpdateRunBody, conn: Conn, user: CurrentUser,
) -> Run:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if config_provided := "config" in body.model_fields_set:
        _validate_config(body.config)
    config = body.config if config_provided else None
    if not (updated := runs_repo.update(conn, run.id, body.status, config, config_provided)):
        raise HTTPException(404, "Run not found")
    return updated
