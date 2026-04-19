from __future__ import annotations

from uuid import UUID

from sqlalchemy import Connection

from underfit_api.helpers import dialect_insert, utcnow
from underfit_api.schema import account_avatars


def get(conn: Connection, account_id: UUID) -> bytes | None:
    row = conn.execute(account_avatars.select().where(account_avatars.c.account_id == account_id)).first()
    return row.image if row else None


def upsert(conn: Connection, account_id: UUID, image: bytes) -> None:
    now = utcnow()
    conn.execute(dialect_insert(conn, account_avatars).values(
        account_id=account_id, image=image, created_at=now, updated_at=now,
    ).on_conflict_do_update(index_elements=["account_id"], set_={"image": image, "updated_at": now}))


def delete(conn: Connection, account_id: UUID) -> None:
    conn.execute(account_avatars.delete().where(account_avatars.c.account_id == account_id))
