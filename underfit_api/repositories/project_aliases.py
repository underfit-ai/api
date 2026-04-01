from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, Row

from underfit_api.helpers import utcnow
from underfit_api.schema import project_aliases


def create(conn: Connection, project_id: UUID, account_id: UUID, name: str) -> None:
    conn.execute(project_aliases.insert().values(
        id=uuid4(), project_id=project_id, account_id=account_id, name=name, created_at=utcnow(),
    ))


def get_by_account_and_name(conn: Connection, account_id: UUID, name: str) -> Row | None:
    return conn.execute(
        project_aliases.select().where(
            project_aliases.c.account_id == account_id, project_aliases.c.name == name.lower(),
        ),
    ).first()


def name_exists(conn: Connection, account_id: UUID, name: str) -> bool:
    return get_by_account_and_name(conn, account_id, name) is not None
