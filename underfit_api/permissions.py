from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import Connection

from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import collaborators as collaborators_repo
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import projects as projects_repo


def _project_info(conn: Connection, project_id: UUID) -> tuple[str, UUID, str]:
    if not (project := projects_repo.get_by_id(conn, project_id)):
        raise HTTPException(404, "Project not found")
    account = accounts_repo.get_by_handle(conn, project.owner)
    assert account is not None
    return project.visibility, account.id, account.type


def require_account_admin(conn: Connection, account_id: UUID, account_type: str, user_id: UUID) -> None:
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organizations_repo.is_admin(conn, account_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_contributor(
    conn: Connection,
    project_id: UUID,
    user_id: UUID,
    account_id: UUID | None = None,
    account_type: str | None = None,
) -> None:
    if account_id is None or account_type is None:
        _, account_id, account_type = _project_info(conn, project_id)
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organizations_repo.is_admin(conn, account_id, user_id):
        return
    if collaborators_repo.get(conn, project_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_viewer(conn: Connection, project_id: UUID, user_id: UUID | None) -> None:
    if can_view_project(conn, project_id, user_id):
        return
    if user_id is None:
        raise HTTPException(401, "Unauthorized")
    raise HTTPException(403, "Forbidden")


def can_view_project(conn: Connection, project_id: UUID, user_id: UUID | None) -> bool:
    visibility, account_id, account_type = _project_info(conn, project_id)
    if visibility == "public":
        return True
    if user_id is None:
        return False
    if account_type == "USER" and account_id == user_id:
        return True
    if account_type == "ORGANIZATION" and organizations_repo.is_admin(conn, account_id, user_id):
        return True
    return collaborators_repo.get(conn, project_id, user_id)
