from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import Field

from underfit_api import backfill
from underfit_api.backfill.ui_state import write_project
from underfit_api.config import config
from underfit_api.dependencies import Conn, Ctx, MaybeUser, RequireUser
from underfit_api.helpers import as_conflict
from underfit_api.models import Body, OkResponse, Project, ProjectVisibility
from underfit_api.permissions import require_account_admin, require_project_contributor
from underfit_api.repositories import project_collaborators as project_collaborators_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories.projects import ProjectSettings
from underfit_api.routes.resolvers import resolve_account, resolve_account_and_project, resolve_project
from underfit_api.storage import delete_prefix

router = APIRouter()

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"


class CreateProjectBody(Body):
    name: str = Field(pattern=NAME_PATTERN)
    description: str = ""
    metadata: dict[str, object] = Field(default_factory=dict)
    visibility: ProjectVisibility = ProjectVisibility.PRIVATE


class RenameProjectBody(Body):
    name: str = Field(pattern=NAME_PATTERN)


class UpdateProjectUIStateBody(Body):
    ui_state: dict[str, object]


class TransferProjectBody(Body):
    handle: str = Field(min_length=1)
    new_name: str | None = Field(default=None, pattern=NAME_PATTERN)


@router.get("/me/projects")
def list_my_projects(conn: Conn, ctx: Ctx, user: RequireUser) -> list[Project]:
    if config.backfill.enabled:
        backfill.sync(ctx, conn)
    return projects_repo.list_related_to_user(conn, user.id)


@router.get("/accounts/{handle}/projects")
def list_account_projects(handle: str, conn: Conn, ctx: Ctx, user: MaybeUser) -> list[Project]:
    if config.backfill.enabled:
        backfill.sync(ctx, conn)
    account = resolve_account(conn, handle)
    return projects_repo.list_visible_by_account(conn, account.id, user.id if user else None)


@router.post("/accounts/{handle}/projects")
def create_project(handle: str, body: CreateProjectBody, conn: Conn, user: RequireUser) -> Project:
    account = resolve_account(conn, handle)
    require_account_admin(conn, account.id, account.type, user.id)
    with as_conflict(conn, "Project already exists"):
        return projects_repo.create(
            conn, account.id, body.name.lower(), body.description, body.visibility, body.metadata,
        )


@router.get("/accounts/{handle}/projects/{project_name}")
def get_project(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> Project:
    return resolve_project(conn, handle, project_name, user)


@router.put("/accounts/{handle}/projects/{project_name}")
def update_project(handle: str, project_name: str, body: ProjectSettings, conn: Conn, user: RequireUser) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    return projects_repo.update_settings(conn, project.id, body)


@router.delete("/accounts/{handle}/projects/{project_name}")
def delete_project(handle: str, project_name: str, conn: Conn, ctx: Ctx, user: RequireUser) -> OkResponse:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    run_storage_keys = [run.storage_key for run in runs_repo.list_by_project(conn, project.id)]
    with ctx.engine.begin() as write_conn:
        projects_repo.delete(write_conn, project.id)
    for storage_key in run_storage_keys:
        delete_prefix(ctx.storage, storage_key)
    delete_prefix(ctx.storage, project.storage_key)
    return OkResponse()


@router.put("/accounts/{handle}/projects/{project_name}/ui-state")
def update_project_ui_state(
    handle: str, project_name: str, body: UpdateProjectUIStateBody, conn: Conn, ctx: Ctx, user: RequireUser,
) -> Project:
    project = resolve_project(conn, handle, project_name, user)
    require_project_contributor(conn, project.id, user.id)
    updated = projects_repo.update_ui_state(conn, project.id, body.ui_state)
    if config.backfill.enabled:
        write_project(ctx, updated)
    return updated


@router.post("/accounts/{handle}/projects/{project_name}/rename")
def rename_project(handle: str, project_name: str, body: RenameProjectBody, conn: Conn, user: RequireUser) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    with as_conflict(conn, "Project already exists"):
        return projects_repo.rename(conn, project.id, body.name.lower())


@router.post("/accounts/{handle}/projects/{project_name}/transfer")
def transfer_project(
    handle: str, project_name: str, body: TransferProjectBody, conn: Conn, user: RequireUser,
) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    recipient = resolve_account(conn, body.handle)
    if recipient.type != "USER":
        raise HTTPException(400, "Projects can only be transferred to a user")
    if recipient.id == account.id:
        raise HTTPException(400, "Cannot transfer a project to its current owner")
    if not project_collaborators_repo.get(conn, project.id, recipient.id):
        raise HTTPException(400, "Recipient must be a collaborator on the project")
    new_name = (body.new_name or project.name).lower()
    with as_conflict(conn, "Project already exists"):
        project_collaborators_repo.remove(conn, project.id, recipient.id)
        return projects_repo.transfer(conn, project.id, recipient.id, new_name)
