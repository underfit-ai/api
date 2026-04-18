from __future__ import annotations

import json
import logging
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection

from underfit_api.backfill import ui_state as ui_state_mod
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


def ensure_run(
    conn: Connection, storage: Storage, run_uuid: UUID, ui: ui_state_mod.UIState,
) -> tuple[UUID, RunMetadata] | None:
    try:
        metadata = RunMetadata.model_validate_json(storage.read(f"{run_uuid}/run.json"))
    except (ValidationError, json.JSONDecodeError, FileNotFoundError):
        logger.warning("Skipping run %s: invalid or missing run.json", run_uuid)
        return None
    if not (resolved := _resolve_user(conn, metadata.user)):
        return None
    user_id, user_handle = resolved
    project_name = metadata.project.lower()
    project_id = _resolve_project(conn, user_id, project_name)
    run_ui = ui_state_mod.lookup_run(ui, run_uuid)
    project_ui = ui_state_mod.lookup_project(ui, user_handle, project_name)
    values = dict(
        project_id=project_id, user_id=user_id, name=(metadata.name or str(run_uuid)).lower(),
        storage_key=str(run_uuid), terminal_state=metadata.terminal_state,
        config=metadata.config, metadata=metadata.metadata,
        ui_state=run_ui.ui_state, is_pinned=run_ui.is_pinned,
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
    _apply_project_ui(conn, project_id, project_ui)
    _apply_baseline(conn, project_id, run_uuid, run_ui.is_baseline)
    return project_id, metadata


def _resolve_user(conn: Connection, handle: str) -> tuple[UUID, str] | None:
    if user := users_repo.get_by_handle(conn, handle):
        return user.id, user.handle
    if handle.lower() != accounts_repo.LOCAL_USER_HANDLE:
        return None
    local = accounts_repo.get_or_create_local(conn)
    return local.id, local.handle


def _resolve_project(conn: Connection, account_id: UUID, name: str) -> UUID:
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
    return project_id


def _apply_project_ui(conn: Connection, project_id: UUID, entry: ui_state_mod.ProjectEntry) -> None:
    row = conn.execute(projects.select().where(projects.c.id == project_id)).first()
    if row and row.ui_state != entry.ui_state:
        projects_repo.update_ui_state(conn, project_id, entry.ui_state)


def _apply_baseline(conn: Connection, project_id: UUID, run_uuid: UUID, is_baseline: bool) -> None:
    row = conn.execute(projects.select().where(projects.c.id == project_id)).first()
    current = row.baseline_run_id if row else None
    if is_baseline and current != run_uuid:
        projects_repo.set_baseline_run(conn, project_id, run_uuid)
    elif not is_baseline and current == run_uuid:
        projects_repo.set_baseline_run(conn, project_id, None)
