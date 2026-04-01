from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import Project
from underfit_api.permissions import can_view_project, require_account_admin
from underfit_api.repositories import project_aliases as project_aliases_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.routes.resolvers import resolve_account, resolve_account_and_project, resolve_project

router = APIRouter()

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"


class CreateProjectBody(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)
    description: str | None = None
    visibility: str = "private"


class UpdateProjectBody(BaseModel):
    description: str | None = None
    visibility: str | None = None


class RenameProjectBody(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)


@router.get("/me/projects")
def list_my_projects(conn: Conn, user: CurrentUser) -> list[Project]:
    return projects_repo.list_by_user_run_count(conn, user.id)


@router.get("/accounts/{handle}/projects")
def list_account_projects(handle: str, conn: Conn, user: MaybeUser) -> list[Project]:
    account = resolve_account(conn, handle)
    projects = projects_repo.list_by_account(conn, account.id)
    return [p for p in projects if can_view_project(conn, p.id, user.id if user else None)]


@router.post("/accounts/{handle}/projects")
def create_project(handle: str, body: CreateProjectBody, conn: Conn, user: CurrentUser) -> Project:
    account = resolve_account(conn, handle)
    require_account_admin(conn, account.id, account.type, user.id)
    if body.visibility not in ("private", "public"):
        raise HTTPException(400, "Invalid visibility")
    name_lower = body.name.lower()
    if project_aliases_repo.name_exists(conn, account.id, name_lower):
        raise HTTPException(409, "Project already exists")
    project = projects_repo.create(conn, account.id, name_lower, body.description, body.visibility)
    project_aliases_repo.create(conn, project.id, account.id, name_lower)
    return project


@router.get("/accounts/{handle}/projects/{project_name}")
def get_project(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> Project:
    return resolve_project(conn, handle, project_name, user)


@router.put("/accounts/{handle}/projects/{project_name}")
def update_project(
    handle: str, project_name: str, body: UpdateProjectBody, conn: Conn, user: CurrentUser,
) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    if body.visibility is not None and body.visibility not in ("private", "public"):
        raise HTTPException(400, "Invalid visibility")
    if not (updated := projects_repo.update(conn, project.id, body.description, body.visibility)):
        raise HTTPException(404, "Project not found")
    return updated


@router.post("/accounts/{handle}/projects/{project_name}/rename")
def rename_project(
    handle: str, project_name: str, body: RenameProjectBody, conn: Conn, user: CurrentUser,
) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    new_name = body.name.lower()
    if project_aliases_repo.name_exists(conn, account.id, new_name):
        raise HTTPException(409, "Project name already exists")
    project_aliases_repo.create(conn, project.id, account.id, new_name)
    result = projects_repo.rename(conn, project.id, new_name)
    assert result is not None
    return result
