from __future__ import annotations

from uuid import UUID

import sqlalchemy as sa
from fastapi import HTTPException
from sqlalchemy import Connection

from underfit_api.repositories import collaborators as collaborators_repo
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.schema import accounts, projects


def _project_account(conn: Connection, project_id: UUID) -> tuple[UUID, str]:
    row = conn.execute(
        sa.select(accounts.c.id, accounts.c.type)
        .select_from(projects.join(accounts, projects.c.account_id == accounts.c.id))
        .where(projects.c.id == project_id),
    ).first()
    if row is None:
        raise HTTPException(404, "Project not found")
    return row.id, row.type


def require_account_admin(conn: Connection, account_id: UUID, account_type: str, user_id: UUID) -> None:
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organizations_repo.is_admin(conn, account_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def require_project_admin(conn: Connection, account_id: UUID, account_type: str, user_id: UUID) -> None:
    require_account_admin(conn, account_id, account_type, user_id)


def require_project_contributor(
    conn: Connection,
    project_id: UUID,
    user_id: UUID,
    account_id: UUID | None = None,
    account_type: str | None = None,
) -> None:
    if account_id is None or account_type is None:
        account_id, account_type = _project_account(conn, project_id)
    if account_type == "USER" and account_id == user_id:
        return
    if account_type == "ORGANIZATION" and organizations_repo.is_admin(conn, account_id, user_id):
        return
    if collaborators_repo.get(conn, project_id, user_id):
        return
    raise HTTPException(403, "Forbidden")


def _project_info(conn: Connection, project_id: UUID) -> tuple[str, UUID, str]:
    row = conn.execute(
        sa.select(projects.c.visibility, accounts.c.id, accounts.c.type)
        .select_from(projects.join(accounts, projects.c.account_id == accounts.c.id))
        .where(projects.c.id == project_id),
    ).first()
    if row is None:
        raise HTTPException(404, "Project not found")
    return row.visibility, row.id, row.type


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
