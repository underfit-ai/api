from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import Connection

from underfit_api.schema import account_avatars


def get(conn: Connection, account_id: UUID) -> bytes | None:
    row = conn.execute(
        account_avatars.select().where(account_avatars.c.account_id == account_id),
    ).first()
    return row.image if row else None


def upsert(conn: Connection, account_id: UUID, image: bytes) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    existing = conn.execute(
        account_avatars.select().where(account_avatars.c.account_id == account_id),
    ).first()
    if existing:
        conn.execute(
            account_avatars.update()
            .where(account_avatars.c.account_id == account_id)
            .values(image=image, updated_at=now),
        )
    else:
        conn.execute(account_avatars.insert().values(
            account_id=account_id, image=image, created_at=now, updated_at=now,
        ))


def delete(conn: Connection, account_id: UUID) -> None:
    conn.execute(account_avatars.delete().where(account_avatars.c.account_id == account_id))
