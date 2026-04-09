from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import Connection

from underfit_api.models import Project
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import project_collaborators as project_collaborators_repo
from underfit_api.repositories import projects as projects_repo


def _resolve_project(conn: Connection, project: Project | UUID) -> Project:
    if isinstance(project, Project):
        return project
    resolved = projects_repo.get_by_id(conn, project)
    if not resolved:
        raise HTTPException(404, "Project not found")
    return resolved


def _project_owner(conn: Connection, project: Project | UUID) -> tuple[Project, UUID, str]:
    project = _resolve_project(conn, project)
    account = accounts_repo.get_by_handle(conn, project.owner)
    assert account is not None
    return project, account.id, account.type


def _is_project_contributor(
    conn: Connection, project_id: UUID, account_id: UUID, account_type: str, user_id: UUID,
) -> bool:
    if account_type == "USER" and account_id == user_id:
        return True
    if account_type == "ORGANIZATION" and organization_members_repo.is_admin(conn, account_id, user_id):
        return True
    return project_collaborators_repo.get(conn, project_id, user_id) is not None


def require_account_admin(conn: Connection, account_id: UUID, account_type: str, user_id: UUID) -> None:
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organization_members_repo.is_admin(conn, account_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_contributor(
    conn: Connection,
    project: Project | UUID,
    user_id: UUID,
) -> None:
    project, account_id, account_type = _project_owner(conn, project)
    if _is_project_contributor(conn, project.id, account_id, account_type, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_viewer(conn: Connection, project: Project | UUID, user_id: UUID | None) -> None:
    if can_view_project(conn, project, user_id):
        return
    if user_id is None:
        raise HTTPException(401, "Unauthorized")
    raise HTTPException(403, "Forbidden")


def can_view_project(conn: Connection, project: Project | UUID, user_id: UUID | None) -> bool:
    project, account_id, account_type = _project_owner(conn, project)
    if project.visibility == "public":
        return True
    if user_id is None:
        return False
    return _is_project_contributor(conn, project.id, account_id, account_type, user_id)
