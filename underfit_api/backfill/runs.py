from __future__ import annotations

import json
import logging
from typing import Literal
from uuid import UUID, uuid4

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection

from underfit_api.backfill.sidecars import ProjectUISidecar, RunUISidecar, load_sidecar
from underfit_api.helpers import utcnow
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import artifacts, projects, runs
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)


class RunMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    project: str
    user: str = "local"
    name: str | None = None
    terminal_state: Literal["finished", "failed", "cancelled"] | None = None
    config: dict[str, object] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    summary: dict[str, float] | None = None


def ensure_run(conn: Connection, storage: Storage, run_uuid: UUID) -> tuple[UUID, UUID, RunMetadata] | None:
    try:
        metadata = RunMetadata.model_validate_json(storage.read(f"{run_uuid}/run.json"))
    except (ValidationError, json.JSONDecodeError, FileNotFoundError):
        logger.warning("Skipping run %s due to invalid run.json", run_uuid)
        return None
    if not metadata.project or not metadata.user:
        return None
    if not (user_id := resolve_user(conn, metadata.user)):
        return None
    project_id = resolve_project(conn, user_id, metadata.project)
    ui = load_sidecar(storage, f"{run_uuid}/ui.json", RunUISidecar)
    values = dict(
        project_id=project_id, user_id=user_id, name=(metadata.name or str(run_uuid)).lower(),
        storage_key=str(run_uuid), terminal_state=metadata.terminal_state,
        config=metadata.config, metadata=metadata.metadata,
        ui_state=ui.ui_state, is_pinned=ui.is_pinned,
    )
    existing = conn.execute(runs.select().where(runs.c.id == run_uuid)).first()
    if not existing:
        now = utcnow()
        conn.execute(runs.insert().values(
            id=run_uuid, launch_id=str(run_uuid),
            summary=metadata.summary or {}, created_at=now, updated_at=now, **values,
        ))
    elif any(getattr(existing, k) != v for k, v in values.items()):
        if existing.project_id != project_id:
            conn.execute(artifacts.delete().where(artifacts.c.run_id == run_uuid))
        conn.execute(runs.update().where(runs.c.id == run_uuid).values(updated_at=utcnow(), **values))
    _sync_baseline(conn, project_id, run_uuid, ui.is_baseline)
    return run_uuid, project_id, metadata


def _sync_baseline(conn: Connection, project_id: UUID, run_uuid: UUID, is_baseline: bool) -> None:
    current = conn.execute(sa.select(projects.c.baseline_run_id).where(projects.c.id == project_id)).scalar()
    if is_baseline and current != run_uuid:
        projects_repo.set_baseline_run(conn, project_id, run_uuid)
    elif not is_baseline and current == run_uuid:
        projects_repo.set_baseline_run(conn, project_id, None)


def resolve_user(conn: Connection, handle: str) -> UUID | None:
    if alias := accounts_repo.get_alias_by_handle(conn, handle):
        if user := users_repo.get_by_id(conn, alias.account_id):
            return user.id
        return None
    if user := users_repo.get_by_handle(conn, handle):
        return user.id
    if handle.lower() != accounts_repo.LOCAL_USER_HANDLE:
        return None
    return accounts_repo.get_or_create_local(conn).id


def resolve_project(conn: Connection, account_id: UUID, name: str) -> UUID:
    name = name.lower()
    if alias := projects_repo.get_alias_by_account_and_name(conn, account_id, name):
        return alias.project_id
    if row := conn.execute(projects.select().where(
        projects.c.account_id == account_id, projects.c.name == name,
    )).first():
        return row.id
    project_id = uuid4()
    now = utcnow()
    conn.execute(projects.insert().values(
        id=project_id, account_id=account_id, name=name, storage_key=str(project_id),
        metadata={}, ui_state={}, visibility="private", created_at=now, updated_at=now,
    ))
    projects_repo.create_alias(conn, project_id, account_id, name)
    return project_id


def reconcile_project_ui(conn: Connection, storage: Storage, project_id: UUID) -> None:
    row = conn.execute(projects.select().where(projects.c.id == project_id)).first()
    account = accounts_repo.get_by_id(conn, row.account_id) if row else None
    if not row or not account:
        return
    ui = load_sidecar(storage, f".projects/{account.handle}/{row.name}/ui.json", ProjectUISidecar)
    if row.ui_state != ui.ui_state:
        conn.execute(projects.update().where(projects.c.id == project_id).values(
            ui_state=ui.ui_state, updated_at=utcnow(),
        ))
