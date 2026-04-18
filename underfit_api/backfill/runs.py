from __future__ import annotations

import json
import logging
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection
from sqlalchemy.engine import Row

from underfit_api.backfill import ui_state
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
    conn: Connection, storage: Storage, run_uuid: UUID, state: ui_state.UIState,
) -> UUID | None:
    try:
        metadata = RunMetadata.model_validate_json(storage.read(f"{run_uuid}/run.json"))
    except (ValidationError, json.JSONDecodeError, FileNotFoundError):
        logger.warning("Skipping run %s: invalid or missing run.json", run_uuid)
        return None
    if not (resolved := _resolve_user(conn, metadata.user)):
        return None
    user_id, user_handle = resolved
    project_name = metadata.project.lower()
    project_row = _resolve_project(conn, user_id, project_name)
    run_ui = ui_state.lookup_run(state, run_uuid)
    project_ui = ui_state.lookup_project(state, user_handle, project_name)
    existing = conn.execute(runs.select().where(runs.c.id == run_uuid)).first()
    values = dict(
        project_id=project_row.id, user_id=user_id, name=(metadata.name or str(run_uuid)).lower(),
        storage_key=str(run_uuid), terminal_state=metadata.terminal_state,
        config=metadata.config, metadata=metadata.metadata,
        ui_state=run_ui.ui_state or {}, is_pinned=bool(run_ui.is_pinned),
        summary=metadata.summary if metadata.summary is not None else (existing.summary if existing else {}),
    )
    if not existing:
        now = utcnow()
        conn.execute(runs.insert().values(
            id=run_uuid, launch_id=str(run_uuid), created_at=now, updated_at=now, **values,
        ))
    elif any(getattr(existing, k) != v for k, v in values.items()):
        if existing.project_id != project_row.id:
            conn.execute(artifacts.delete().where(artifacts.c.run_id == run_uuid))
        conn.execute(runs.update().where(runs.c.id == run_uuid).values(updated_at=utcnow(), **values))
    if project_row.ui_state != project_ui.ui_state:
        projects_repo.update_ui_state(conn, project_row.id, project_ui.ui_state)
    target_baseline = project_ui.baseline_run_id
    if target_baseline == run_uuid and project_row.baseline_run_id != run_uuid:
        projects_repo.set_baseline_run(conn, project_row.id, run_uuid)
    elif target_baseline != run_uuid and project_row.baseline_run_id == run_uuid:
        projects_repo.set_baseline_run(conn, project_row.id, None)
    return project_row.id


def _resolve_user(conn: Connection, handle: str) -> tuple[UUID, str] | None:
    if user := users_repo.get_by_handle(conn, handle):
        return user.id, user.handle
    if handle.lower() != accounts_repo.LOCAL_USER_HANDLE:
        return None
    local = accounts_repo.get_or_create_local(conn)
    return local.id, local.handle


def _resolve_project(conn: Connection, account_id: UUID, name: str) -> Row:
    if row := conn.execute(projects.select().where(
        projects.c.account_id == account_id, projects.c.name == name,
    )).first():
        return row
    project_id = uuid4()
    now = utcnow()
    conn.execute(projects.insert().values(
        id=project_id, account_id=account_id, name=name, storage_key=str(project_id),
        metadata={}, ui_state={}, visibility="private", created_at=now, updated_at=now,
    ))
    row = conn.execute(projects.select().where(projects.c.id == project_id)).first()
    assert row is not None
    return row
