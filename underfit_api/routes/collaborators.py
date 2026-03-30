from __future__ import annotations

from fastapi import APIRouter, HTTPException

from underfit_api.dependencies import Conn, CurrentUser, MaybeUser
from underfit_api.models import Collaborator, User
from underfit_api.permissions import require_account_admin
from underfit_api.repositories import collaborators as collaborators_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_account_and_project

router = APIRouter()


@router.get("/accounts/{handle}/projects/{project_name}/collaborators")
def list_collaborators(handle: str, project_name: str, conn: Conn, user: MaybeUser) -> list[User]:
    _, project = resolve_account_and_project(conn, handle, project_name, user)
    return collaborators_repo.list_by_project(conn, project.id)


@router.put("/accounts/{handle}/projects/{project_name}/collaborators/{user_handle}")
def add_collaborator(
    handle: str, project_name: str, user_handle: str, conn: Conn, user: CurrentUser,
) -> Collaborator:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if collaborators_repo.get(conn, project.id, target.id):
        raise HTTPException(409, "Already a collaborator")
    return collaborators_repo.add(conn, project.id, target.id)


@router.delete("/accounts/{handle}/projects/{project_name}/collaborators/{user_handle}")
def remove_collaborator(
    handle: str, project_name: str, user_handle: str, conn: Conn, user: CurrentUser,
) -> dict[str, bool]:
    account, project = resolve_account_and_project(conn, handle, project_name, user)
    require_account_admin(conn, account.id, account.type, user.id)
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if not collaborators_repo.get(conn, project.id, target.id):
        raise HTTPException(404, "Not a collaborator")
    collaborators_repo.remove(conn, project.id, target.id)
    return {"ok": True}
