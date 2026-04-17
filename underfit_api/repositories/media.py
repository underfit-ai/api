from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Media
from underfit_api.schema import media


def get_by_id(conn: Connection, media_id: UUID) -> Media | None:
    row = conn.execute(media.select().where(media.c.id == media_id, media.c.finalized.is_(True))).first()
    return Media.model_validate(row) if row else None


def list_by_run(
    conn: Connection, run_id: UUID, key: str | None = None, step: int | None = None,
) -> list[Media]:
    query = media.select().where(media.c.run_id == run_id)
    if key is not None:
        query = query.where(media.c.key == key)
    if step is not None:
        query = query.where(media.c.step == step)
    query = query.where(media.c.finalized.is_(True))
    rows = conn.execute(query.order_by(media.c.created_at.asc(), media.c.index.asc())).all()
    return [Media.model_validate(row) for row in rows]

def create(
    conn: Connection, run_id: UUID, key: str, step: int, media_type: str,
    index: int, storage_key: str, metadata: dict[str, object] | None,
) -> Media:
    media_id = uuid4()
    conn.execute(media.insert().values(
        id=media_id, run_id=run_id, key=key, step=step, type=media_type,
        index=index, finalized=False, storage_key=storage_key, metadata=metadata, created_at=utcnow(),
    ))
    return Media.model_validate(conn.execute(media.select().where(media.c.id == media_id)).first())


def finalize_group(conn: Connection, run_id: UUID, media_type: str, key: str, step: int) -> None:
    conn.execute(media.update().where(
        media.c.run_id == run_id, media.c.type == media_type, media.c.key == key, media.c.step == step,
    ).values(finalized=True))


def delete_group(conn: Connection, run_id: UUID, media_type: str, key: str, step: int) -> None:
    conn.execute(media.delete().where(
        media.c.run_id == run_id, media.c.type == media_type, media.c.key == key, media.c.step == step,
    ))
