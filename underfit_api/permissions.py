from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import Connection

from underfit_api.models import Project
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import project_collaborators as project_collaborators_repo


def _is_project_contributor(conn: Connection, project: Project, user_id: UUID) -> bool:
    if project.account_type == "USER" and project.account_id == user_id:
        return True
    if project.account_type == "ORGANIZATION" and organization_members_repo.is_admin(conn, project.account_id, user_id):
        return True
    return project_collaborators_repo.get(conn, project.id, user_id) is not None


def require_account_admin(conn: Connection, account_id: UUID, account_type: str, user_id: UUID) -> None:
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organization_members_repo.is_admin(conn, account_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_contributor(conn: Connection, project: Project, user_id: UUID) -> None:
    if not _is_project_contributor(conn, project, user_id):
        raise HTTPException(403, "Forbidden")


def require_project_viewer(conn: Connection, project: Project, user_id: UUID | None) -> None:
    if project.visibility == "public":
        return
    if user_id is None:
        raise HTTPException(401, "Unauthorized")
    if _is_project_contributor(conn, project, user_id):
        return
    raise HTTPException(403, "Forbidden")
