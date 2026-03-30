from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Media
from underfit_api.schema import media


def get_by_id(conn: Connection, media_id: UUID) -> Media | None:
    row = conn.execute(media.select().where(media.c.id == media_id)).first()
    return Media.model_validate(row) if row else None


def list_by_run(
    conn: Connection, run_id: UUID, key: str | None = None, step: int | None = None,
) -> list[Media]:
    query = media.select().where(media.c.run_id == run_id)
    if key is not None:
        query = query.where(media.c.key == key)
    if step is not None:
        query = query.where(media.c.step == step)
    rows = conn.execute(query.order_by(media.c.created_at.asc())).all()
    return [Media.model_validate(row) for row in rows]


def create(
    conn: Connection,
    run_id: UUID,
    key: str,
    step: int | None,
    media_type: str,
    storage_key: str,
    count: int,
    metadata: dict[str, object] | None,
) -> Media:
    media_id = uuid4()
    conn.execute(media.insert().values(
        id=media_id,
        run_id=run_id,
        key=key,
        step=step,
        type=media_type,
        storage_key=storage_key,
        count=count,
        metadata=metadata,
        created_at=utcnow(),
    ))
    result = get_by_id(conn, media_id)
    assert result is not None
    return result
