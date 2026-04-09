from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

import underfit_api.storage as storage_mod
from underfit_api.auth import create_worker_token
from underfit_api.dependencies import Conn, CurrentUser, CurrentWorker, MaybeUser
from underfit_api.helpers import as_conflict
from underfit_api.models import OkResponse, Run, RunTerminalState
from underfit_api.permissions import require_account_admin, require_project_contributor
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_account_and_project_path, resolve_project, resolve_run

router = APIRouter()

MAX_JSON_BYTES = 65536
RUN_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"


class LaunchBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    run_name: str = Field(pattern=RUN_NAME_PATTERN)
    launch_id: str
    worker_label: str = "0"
    config: dict[str, object] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class UpdateRunBody(BaseModel):
    metadata: dict[str, object] | None = None


class UpdateTerminalStateBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    terminal_state: RunTerminalState


def _validate_json(value: dict[str, object] | None, label: str) -> None:
    if value is not None and len(json.dumps(value)) > MAX_JSON_BYTES:
        raise HTTPException(400, f"{label} too large")


def _launch_response(conn: Conn, run: Run, worker_id: object) -> Run:
    refreshed = runs_repo.get_by_id(conn, run.id) or run
    return refreshed.model_copy(update={"worker_token": create_worker_token(worker_id)})


@router.get("/users/{handle}/runs")
def list_user_runs(handle: str, conn: Conn, user: MaybeUser) -> list[Run]:
    if not (target := users_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "User not found")
    return runs_repo.list_visible_by_user(conn, target.id, user.id if user else None)


@router.get("/accounts/{handle}/projects/{project_name}/runs")
def list_project_runs(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[Run]:
    project = resolve_project(conn, handle, project_name, user)
    return runs_repo.list_by_project(conn, project.id)


@router.post("/accounts/{handle}/projects/{project_name}/runs/launch")
def launch(handle: str, project_name: str, body: LaunchBody, conn: Conn, user: CurrentUser) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project, user.id)
    _validate_json(body.config, "Config")
    _validate_json(body.metadata, "Metadata")

    existing = runs_repo.get_by_project_and_launch_id(conn, project.id, body.launch_id)
    if existing:
        if not runs_repo.has_active_worker(conn, existing.id):
            raise HTTPException(409, "Stale run with this launch ID already exists")
        if worker := workers_repo.get(conn, existing.id, body.worker_label):
            return _launch_response(conn, existing, worker.id)
        worker = workers_repo.create(conn, existing.id, body.worker_label)
        return _launch_response(conn, existing, worker.id)

    with as_conflict(conn, "Run already exists"):
        run = runs_repo.create(conn, project.id, user.id, body.launch_id, body.run_name, body.config, body.metadata)
        worker = workers_repo.create(conn, run.id, body.worker_label)
    return _launch_response(conn, run, worker.id)


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def get_run(handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser) -> Run:
    return resolve_run(conn, handle, project_name, run_name, user)


@router.put("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def update_run(
    handle: str, project_name: str, run_name: str, body: UpdateRunBody, conn: Conn, user: CurrentUser,
) -> Run:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    _validate_json(body.metadata, "Metadata")
    if not (updated := runs_repo.update(conn, run.id, body.metadata)):
        raise HTTPException(404, "Run not found")
    return updated


@router.delete("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def delete_run(handle: str, project_name: str, run_name: str, conn: Conn, user: CurrentUser) -> OkResponse:
    _, project = resolve_account_and_project_path(conn, handle, project_name)
    if not (run := runs_repo.get_by_project_and_name(conn, project.id, run_name)):
        raise HTTPException(404, "Run not found")
    if run.user != user.handle:
        assert (owner_account := accounts_repo.get_by_handle(conn, run.project_owner)) is not None
        require_account_admin(conn, owner_account.id, owner_account.type, user.id)
    storage_mod.delete_prefix(run.storage_key)
    runs_repo.delete(conn, run.id)
    return OkResponse()


@router.put("/runs/terminal-state")
def update_terminal_state(body: UpdateTerminalStateBody, conn: Conn, worker_id: CurrentWorker) -> Run:
    if not (worker := workers_repo.get_by_id(conn, worker_id)):
        raise HTTPException(401, "Unauthorized")
    if not (run := runs_repo.update_terminal_state(conn, worker.run_id, body.terminal_state.value)):
        raise HTTPException(404, "Run not found")
    return run
