from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, ValidationError

import underfit_api.storage as storage_mod
from underfit_api.auth import create_signed_token, verify_signed_token
from underfit_api.config import config
from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.email import send_email
from underfit_api.helpers import as_conflict
from underfit_api.models import OkResponse, Project, ProjectVisibility
from underfit_api.permissions import require_account_admin
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import project_collaborators as project_collaborators_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_account, resolve_account_and_project, resolve_project

router = APIRouter()

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
TRANSFER_TOKEN_TTL = timedelta(days=7)


class CreateProjectBody(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)
    description: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    visibility: ProjectVisibility = ProjectVisibility.PRIVATE


class UpdateProjectBody(BaseModel):
    description: str | None = None
    metadata: dict[str, object] | None = None
    visibility: ProjectVisibility | None = None


class RenameProjectBody(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)


class InitiateTransferBody(BaseModel):
    email: str = Field(min_length=1)


class AcceptTransferBody(BaseModel):
    token: str = Field(min_length=1)
    new_name: str | None = Field(default=None, pattern=NAME_PATTERN)


class TransferTokenPayload(BaseModel):
    project_id: UUID
    to_account_id: UUID


@router.get("/me/projects")
def list_my_projects(conn: Conn, user: CurrentUser) -> list[Project]:
    return projects_repo.list_related_to_user(conn, user.id)


@router.get("/accounts/{handle}/projects")
def list_account_projects(handle: str, conn: Conn, user: MaybeUser) -> list[Project]:
    account = resolve_account(conn, handle)
    return projects_repo.list_visible_by_account(conn, account.id, user.id if user else None)


@router.post("/accounts/{handle}/projects")
def create_project(handle: str, body: CreateProjectBody, conn: Conn, user: CurrentUser) -> Project:
    account = resolve_account(conn, handle)
    require_account_admin(conn, account.id, account.type, user.id)
    name_lower = body.name.lower()
    with as_conflict(conn, "Project already exists"):
        project = projects_repo.create(conn, account.id, name_lower, body.description, body.visibility, body.metadata)
        projects_repo.create_alias(conn, project.id, account.id, name_lower)
    return project


@router.get("/accounts/{handle}/projects/{project_name}")
def get_project(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> Project:
    return resolve_project(conn, handle, project_name, user)


@router.put("/accounts/{handle}/projects/{project_name}")
def update_project(handle: str, project_name: str, body: UpdateProjectBody, conn: Conn, user: CurrentUser) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    if not (updated := projects_repo.update(conn, project.id, body.description, body.visibility, body.metadata)):
        raise HTTPException(404, "Project not found")
    return updated


@router.delete("/accounts/{handle}/projects/{project_name}")
def delete_project(handle: str, project_name: str, conn: Conn, user: CurrentUser) -> OkResponse:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    for run in runs_repo.list_by_project(conn, project.id):
        storage_mod.delete_prefix(run.storage_key)
    storage_mod.delete_prefix(str(project.id))
    projects_repo.delete(conn, project.id)
    return OkResponse()


@router.post("/accounts/{handle}/projects/{project_name}/rename")
def rename_project(handle: str, project_name: str, body: RenameProjectBody, conn: Conn, user: CurrentUser) -> Project:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    new_name = body.name.lower()
    with as_conflict(conn, "Project already exists"):
        projects_repo.create_alias(conn, project.id, account.id, new_name)
        result = projects_repo.rename(conn, project.id, new_name)
    assert result is not None
    return result


@router.post("/accounts/{handle}/projects/{project_name}/transfer")
def initiate_transfer(
    handle: str, project_name: str, body: InitiateTransferBody, conn: Conn, user: CurrentUser,
) -> OkResponse:
    if not config.email:
        raise HTTPException(400, "Email is not configured")
    if not config.frontend_url:
        raise HTTPException(400, "Frontend URL is not configured")

    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)

    recipient = users_repo.get_by_email(conn, body.email)
    if not recipient:
        raise HTTPException(404, "No user found with that email")
    if recipient.id == account.id:
        raise HTTPException(400, "Cannot transfer a project to its current owner")

    projects_repo.set_pending_transfer(conn, project.id, recipient.id)

    token = create_signed_token({"project_id": str(project.id), "to_account_id": str(recipient.id)}, TRANSFER_TOKEN_TTL)
    base_url = config.frontend_url.rstrip("/")
    transfer_url = f"{base_url}/transfer?token={token}"
    send_email(
        config.email,
        to=recipient.email,
        subject=f"Project transfer: {account.handle}/{project.name}",
        body=(
            f"{account.handle} wants to transfer the project \"{project.name}\" to you.\n\n"
            f"Click the link below to accept:\n\n{transfer_url}\n\n"
            f"This link expires in 7 days."
        ),
    )
    return OkResponse()


@router.delete("/accounts/{handle}/projects/{project_name}/transfer")
def cancel_transfer(handle: str, project_name: str, conn: Conn, user: CurrentUser) -> OkResponse:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    if not project.pending_transfer_to:
        raise HTTPException(400, "No pending transfer")
    projects_repo.set_pending_transfer(conn, project.id, None)
    return OkResponse()


@router.post("/transfer")
def accept_transfer(body: AcceptTransferBody, conn: Conn, user: CurrentUser) -> Project:
    raw = verify_signed_token(body.token)
    if not raw:
        raise HTTPException(400, "Invalid or expired transfer token")
    try:
        payload = TransferTokenPayload.model_validate(raw)
    except (ValueError, ValidationError):
        raise HTTPException(400, "Invalid or expired transfer token") from None

    if user.id != payload.to_account_id:
        raise HTTPException(403, "This transfer is not addressed to you")

    project = projects_repo.get_by_id(conn, payload.project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    if project.pending_transfer_to != payload.to_account_id:
        raise HTTPException(400, "Transfer has been cancelled")

    new_name = (body.new_name or project.name).lower()
    old_account = accounts_repo.get_by_handle(conn, project.owner)
    assert old_account is not None
    with as_conflict(conn, "Project already exists"):
        if not projects_repo.get_alias_by_account_and_name(conn, old_account.id, project.name):
            projects_repo.create_alias(conn, project.id, old_account.id, project.name)
        projects_repo.create_alias(conn, project.id, payload.to_account_id, new_name)
        if project_collaborators_repo.get(conn, project.id, user.id):
            project_collaborators_repo.remove(conn, project.id, user.id)
        result = projects_repo.transfer(conn, payload.project_id, payload.to_account_id, new_name)
    assert result is not None
    return result
