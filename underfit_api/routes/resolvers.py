from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException

from underfit_api.dependencies import Conn
from underfit_api.models import Account, Artifact, Organization, Project, Run, User
from underfit_api.permissions import require_project_viewer
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import artifacts as artifacts_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo


class AliasRedirectError(Exception):
    def __init__(self, path_segment: str, old_name: str, new_name: str) -> None:
        self.path_segment = path_segment
        self.old_name = old_name
        self.new_name = new_name


def resolve_account(conn: Conn, handle: str) -> Account:
    alias = accounts_repo.get_alias_by_handle(conn, handle)
    if not alias:
        raise HTTPException(404, "Account not found")
    account = accounts_repo.get_by_id(conn, alias.account_id)
    if not account:
        raise HTTPException(404, "Account not found")
    if handle.lower() != account.handle:
        raise AliasRedirectError("/accounts", handle.lower(), account.handle)
    return account


def resolve_organization(conn: Conn, handle: str) -> Organization:
    alias = accounts_repo.get_alias_by_handle(conn, handle)
    if not alias:
        raise HTTPException(404, "Organization not found")
    account = accounts_repo.get_by_id(conn, alias.account_id)
    if not account or account.type != "ORGANIZATION":
        raise HTTPException(404, "Organization not found")
    if handle.lower() != account.handle:
        raise AliasRedirectError("/organizations", handle.lower(), account.handle)
    assert isinstance(account, Organization)
    return account


def resolve_account_and_project_path(conn: Conn, handle: str, project_name: str) -> tuple[Account, Project]:
    account = resolve_account(conn, handle)
    alias = projects_repo.get_alias_by_account_and_name(conn, account.id, project_name)
    if not alias:
        raise HTTPException(404, "Project not found")
    project = projects_repo.get_by_id(conn, alias.project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    if project_name.lower() != project.name:
        raise AliasRedirectError("/projects", project_name.lower(), project.name)
    if handle.lower() != project.owner:
        raise AliasRedirectError("/accounts", handle.lower(), project.owner)
    return account, project


def resolve_account_and_project(
    conn: Conn, handle: str, project_name: str, user: User | None = None,
) -> tuple[Account, Project]:
    account, project = resolve_account_and_project_path(conn, handle, project_name)
    require_project_viewer(conn, project, user.id if user else None)
    return account, project


def resolve_project(conn: Conn, handle: str, project_name: str, user: User | None = None) -> Project:
    return resolve_account_and_project(conn, handle, project_name, user)[1]


def resolve_run(conn: Conn, handle: str, project_name: str, run_name: str, user: User | None = None) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    if not (run := runs_repo.get_by_project_and_name(conn, project.id, run_name)):
        raise HTTPException(404, "Run not found")
    return run


def resolve_artifact(conn: Conn, artifact_id: UUID, user: User | None = None) -> Artifact:
    if not (artifact := artifacts_repo.get_by_id(conn, artifact_id)):
        raise HTTPException(404, "Artifact not found")
    require_project_viewer(conn, artifact.project_id, user.id if user else None)
    return artifact
