from __future__ import annotations

import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Connection

from app.models import Run
from app.schema import accounts, projects, runs

_join = runs.join(projects, runs.c.project_id == projects.c.id).join(accounts, projects.c.account_id == accounts.c.id)
_user_handle = sa.select(accounts.c.handle).where(accounts.c.id == runs.c.user_id).correlate(runs).scalar_subquery()
_columns = [
    runs.c.id,
    runs.c.project_id,
    accounts.c.handle.label("project_owner"),
    projects.c.name.label("project_name"),
    _user_handle.label("user"),
    runs.c.name,
    runs.c.status,
    runs.c.config,
    runs.c.created_at,
    runs.c.updated_at,
]

_wordlists_dir = Path(__file__).resolve().parent.parent / "wordlists"
_adjectives = (_wordlists_dir / "adjectives.txt").read_text().splitlines()
_nouns = (_wordlists_dir / "nouns.txt").read_text().splitlines()


def _generate_name(conn: Connection, project_id: uuid.UUID) -> str | None:
    for _ in range(8):
        adj = _adjectives[secrets.randbelow(len(_adjectives))]
        noun = _nouns[secrets.randbelow(len(_nouns))]
        name = f"{adj}-{noun}"
        exists = conn.execute(
            runs.select().where(runs.c.project_id == project_id, runs.c.name == name),
        ).first()
        if exists is None:
            return name
    return None


def get_by_id(conn: Connection, run_id: uuid.UUID) -> Run | None:
    row = conn.execute(sa.select(*_columns).select_from(_join).where(runs.c.id == run_id)).first()
    return Run.model_validate(row) if row else None


def get_by_project_and_name(conn: Connection, project_id: uuid.UUID, name: str) -> Run | None:
    row = conn.execute(
        sa.select(*_columns).select_from(_join).where(
            runs.c.project_id == project_id, runs.c.name == name.lower(),
        ),
    ).first()
    return Run.model_validate(row) if row else None


def list_by_project(conn: Connection, project_id: uuid.UUID) -> list[Run]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(runs.c.project_id == project_id)
        .order_by(runs.c.created_at.desc()),
    ).all()
    return [Run.model_validate(row) for row in rows]


def list_by_user(conn: Connection, user_id: uuid.UUID) -> list[Run]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(runs.c.user_id == user_id)
        .order_by(runs.c.created_at.desc()),
    ).all()
    return [Run.model_validate(row) for row in rows]


def create(
    conn: Connection, project_id: uuid.UUID, user_id: uuid.UUID,
    status: str, config: dict[str, object] | None,
) -> Run | None:
    if not (name := _generate_name(conn, project_id)):
        return None
    run_id = uuid.uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(runs.insert().values(
        id=run_id, project_id=project_id, user_id=user_id,
        name=name, status=status, config=config, created_at=now, updated_at=now,
    ))
    return get_by_id(conn, run_id)


def update(
    conn: Connection, run_id: uuid.UUID, status: str | None,
    config: dict[str, object] | None, update_config: bool,
) -> Run | None:
    values: dict[str, object] = {"updated_at": datetime.now(timezone.utc).replace(tzinfo=None)}
    if status is not None:
        values["status"] = status
    if update_config:
        values["config"] = config
    conn.execute(runs.update().where(runs.c.id == run_id).values(**values))
    return get_by_id(conn, run_id)
