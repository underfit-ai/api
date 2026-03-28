from __future__ import annotations

from fastapi import HTTPException

from app.dependencies import Conn
from app.models import Account, Project, Run, User
from app.permissions import require_project_viewer
from app.repositories import accounts as accounts_repo
from app.repositories import projects as projects_repo
from app.repositories import runs as runs_repo


def resolve_account(conn: Conn, handle: str) -> Account:
    account = accounts_repo.get_by_handle(conn, handle)
    if not account:
        raise HTTPException(404, "Account not found")
    return account


def resolve_account_and_project(
    conn: Conn, handle: str, project_name: str, user: User | None = None,
) -> tuple[Account, Project]:
    account = resolve_account(conn, handle)
    if not (project := projects_repo.get_by_account_and_name(conn, account.id, project_name)):
        raise HTTPException(404, "Project not found")
    require_project_viewer(conn, project.id, user.id if user else None)
    return account, project


def resolve_project(conn: Conn, handle: str, project_name: str, user: User | None = None) -> Project:
    return resolve_account_and_project(conn, handle, project_name, user)[1]


def resolve_run(conn: Conn, handle: str, project_name: str, run_name: str, user: User | None = None) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    if not (run := runs_repo.get_by_project_and_name(conn, project.id, run_name)):
        raise HTTPException(404, "Run not found")
    return run
