from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from underfit_api.auth import create_worker_token
from underfit_api.backfill import sync_run_ui_sidecar
from underfit_api.dependencies import Conn, Ctx, CurrentWorker, MaybeUser, RequireUser
from underfit_api.helpers import as_conflict, validate_json_size
from underfit_api.models import OkResponse, Run, RunTerminalState
from underfit_api.permissions import require_account_admin, require_project_contributor
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_project, resolve_run
from underfit_api.storage import delete_prefix

router = APIRouter()

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


class UpdateSummaryBody(BaseModel):
    summary: dict[str, float]


class UpdateRunUIStateBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    ui_state: dict[str, object] | None = None
    is_pinned: bool | None = None
    is_baseline: bool | None = None


def _launch_response(run: Run, worker_id: object) -> Run:
    return run.model_copy(update={"worker_token": create_worker_token(worker_id), "is_active": True})


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
def launch(handle: str, project_name: str, body: LaunchBody, conn: Conn, user: RequireUser) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project, user.id)
    validate_json_size(body.config, "Config")
    validate_json_size(body.metadata, "Metadata")

    existing = runs_repo.get_by_project_and_launch_id(conn, project.id, body.launch_id)
    if existing:
        if not runs_repo.has_active_worker(conn, existing.id):
            raise HTTPException(409, "Stale run with this launch ID already exists")
        worker = workers_repo.get(conn, existing.id, body.worker_label) \
            or workers_repo.create(conn, existing.id, body.worker_label)
        return _launch_response(existing, worker.id)

    with as_conflict(conn, "Run already exists"):
        run = runs_repo.create(conn, project.id, user.id, body.launch_id, body.run_name, body.config, body.metadata)
        worker = workers_repo.create(conn, run.id, body.worker_label)
    return _launch_response(run, worker.id)


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def get_run(handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser) -> Run:
    return resolve_run(conn, handle, project_name, run_name, user)


@router.put("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def update_run(
    handle: str, project_name: str, run_name: str, body: UpdateRunBody, conn: Conn, user: RequireUser,
) -> Run:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    validate_json_size(body.metadata, "Metadata")
    return runs_repo.update(conn, run.id, metadata=body.metadata)


@router.delete("/accounts/{handle}/projects/{project_name}/runs/{run_name}")
def delete_run(
    handle: str, project_name: str, run_name: str, conn: Conn, ctx: Ctx, user: RequireUser,
) -> OkResponse:
    run = resolve_run(conn, handle, project_name, run_name, user)
    if run.user != user.handle:
        require_account_admin(conn, run.project_owner_id, run.project_owner_type, user.id)
    with ctx.engine.begin() as write_conn:
        runs_repo.delete(write_conn, run.id)
    delete_prefix(ctx.storage, run.storage_key)
    return OkResponse()


@router.put("/accounts/{handle}/projects/{project_name}/runs/{run_name}/ui-state")
def update_run_ui_state(
    handle: str, project_name: str, run_name: str, body: UpdateRunUIStateBody,
    conn: Conn, ctx: Ctx, user: RequireUser,
) -> Run:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    validate_json_size(body.ui_state, "UI state")
    if body.is_baseline is not None:
        projects_repo.set_baseline_run(conn, run.project_id, run.id if body.is_baseline else None)
    updated = runs_repo.update(conn, run.id, ui_state=body.ui_state, is_pinned=body.is_pinned)
    sync_run_ui_sidecar(ctx.storage, updated)
    return updated


@router.put("/runs/terminal-state")
def update_terminal_state(body: UpdateTerminalStateBody, conn: Conn, worker_id: CurrentWorker) -> Run:
    if not (worker := workers_repo.get_by_id(conn, worker_id)):
        raise HTTPException(401, "Unauthorized")
    return runs_repo.update(conn, worker.run_id, terminal_state=body.terminal_state.value)


@router.put("/runs/summary")
def update_summary(body: UpdateSummaryBody, conn: Conn, worker_id: CurrentWorker) -> Run:
    if not (worker := workers_repo.get_by_id(conn, worker_id)):
        raise HTTPException(401, "Unauthorized")
    return runs_repo.update(conn, worker.run_id, summary=body.summary)
