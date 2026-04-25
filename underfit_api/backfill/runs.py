from __future__ import annotations

import json
import logging
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Project, ProjectVisibility
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import artifacts, runs
from underfit_api.storage import Storage

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


def ensure_run(conn: Connection, storage: Storage, run_uuid: UUID) -> Project | None:
    try:
        metadata = RunMetadata.model_validate_json(storage.read(f"{run_uuid}/run.json"))
    except (ValidationError, json.JSONDecodeError):
        logger.warning("Skipping run %s: corrupt run.json", run_uuid)
        return None
    if not (user_id := _resolve_user(conn, metadata.user)):
        return None
    project_name = metadata.project.lower()
    project_row = _resolve_project(conn, user_id, project_name)
    existing = conn.execute(runs.select().where(runs.c.id == run_uuid)).first()
    values: dict[str, object] = dict(
        project_id=project_row.id, user_id=user_id, name=(metadata.name or str(run_uuid)).lower(),
        storage_key=str(run_uuid), terminal_state=metadata.terminal_state,
        config=metadata.config, metadata=metadata.metadata,
    )
    if not existing:
        now = utcnow()
        conn.execute(runs.insert().values(
            id=run_uuid, launch_id=str(run_uuid), created_at=now, updated_at=now,
            summary=metadata.summary or {}, ui_state={}, is_pinned=False, **values,
        ))
    else:
        values["summary"] = metadata.summary if metadata.summary is not None else existing.summary
        if any(getattr(existing, k) != v for k, v in values.items()):
            if existing.project_id != project_row.id:
                conn.execute(artifacts.delete().where(artifacts.c.run_id == run_uuid))
            conn.execute(runs.update().where(runs.c.id == run_uuid).values(updated_at=utcnow(), **values))
    return project_row


def _resolve_user(conn: Connection, handle: str) -> UUID | None:
    if user := users_repo.get_by_handle(conn, handle):
        return user.id
    if handle.lower() != accounts_repo.LOCAL_USER_HANDLE:
        return None
    return accounts_repo.get_or_create_local(conn).id


def _resolve_project(conn: Connection, account_id: UUID, name: str) -> Project:
    if project := projects_repo.get_by_account_and_name(conn, account_id, name):
        return project
    return projects_repo.create(conn, account_id, name, "", ProjectVisibility.PRIVATE, {}, f"projects/{name}")
