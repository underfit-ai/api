from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException

from underfit_api import backfill
from underfit_api.config import config
from underfit_api.dependencies import Conn, Ctx
from underfit_api.models import Account, Artifact, Organization, Project, Run, User
from underfit_api.permissions import require_project_viewer
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import artifacts as artifacts_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo


def resolve_account(conn: Conn, handle: str) -> Account:
    if not (account := accounts_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Account not found")
    return account


def resolve_organization(conn: Conn, handle: str) -> Organization:
    account = accounts_repo.get_by_handle(conn, handle)
    if not account or account.type != "ORGANIZATION":
        raise HTTPException(404, "Organization not found")
    assert isinstance(account, Organization)
    return account


def resolve_account_and_project(
    conn: Conn, handle: str, project_name: str, user: User | None = None,
) -> tuple[Account, Project]:
    account = resolve_account(conn, handle)
    project = projects_repo.get_by_account_and_name(conn, account.id, project_name)
    if not project:
        raise HTTPException(404, "Project not found")
    require_project_viewer(conn, project, user.id if user else None)
    return account, project


def resolve_project(conn: Conn, handle: str, project_name: str, user: User | None = None) -> Project:
    return resolve_account_and_project(conn, handle, project_name, user)[1]


def resolve_run(
    conn: Conn, ctx: Ctx, handle: str, project_name: str, run_name: str, user: User | None = None,
) -> Run:
    project = resolve_project(conn, handle, project_name, user)
    if not (run := runs_repo.get_by_project_and_name(conn, project.id, run_name)):
        raise HTTPException(404, "Run not found")
    if config.backfill.enabled:
        backfill.refresh_run(ctx, conn, run.id)
        if not (run := runs_repo.get_by_project_and_name(conn, project.id, run_name)):
            raise HTTPException(404, "Run not found")
    return run


def resolve_artifact(
    conn: Conn, artifact_id: UUID, user: User | None = None, *, require_finalized: bool = True,
) -> Artifact:
    if not (artifact := artifacts_repo.get_by_id(conn, artifact_id)):
        raise HTTPException(404, "Artifact not found")
    if require_finalized and artifact.finalized_at is None:
        raise HTTPException(404, "Artifact not found")
    require_project_viewer(conn, artifact.project_id, user.id if user else None)
    return artifact
