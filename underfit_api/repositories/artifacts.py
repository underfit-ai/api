from __future__ import annotations

from uuid import UUID

from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Artifact
from underfit_api.schema import artifacts

ARTIFACT_FINALIZING = -1


def get_by_id(conn: Connection, artifact_id: UUID) -> Artifact | None:
    row = conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first()
    return Artifact.model_validate(row) if row else None


def list_by_project(conn: Connection, project_id: UUID) -> list[Artifact]:
    rows = conn.execute(
        artifacts.select()
        .where(artifacts.c.project_id == project_id, artifacts.c.finalized_at.is_not(None))
        .order_by(artifacts.c.created_at.desc()),
    ).all()
    return [Artifact.model_validate(row) for row in rows]


def create(
    conn: Connection, artifact_id: UUID, project_id: UUID, run_id: UUID | None, step: int | None,
    name: str, artifact_type: str, storage_key: str, metadata: dict[str, object] | None,
) -> Artifact:
    now = utcnow()
    conn.execute(artifacts.insert().values(
        id=artifact_id,
        project_id=project_id,
        run_id=run_id,
        step=step,
        name=name,
        type=artifact_type,
        storage_key=storage_key,
        stored_size_bytes=None,
        active_uploads=0,
        created_at=now,
        updated_at=now,
        metadata=metadata,
    ))
    result = get_by_id(conn, artifact_id)
    assert result is not None
    return result


def finalize(conn: Connection, artifact_id: UUID, stored_size_bytes: int) -> Artifact | None:
    now = utcnow()
    conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id)
        .values(finalized_at=now, stored_size_bytes=stored_size_bytes, active_uploads=0, updated_at=now),
    )
    return get_by_id(conn, artifact_id)


def begin_upload(conn: Connection, artifact_id: UUID) -> bool:
    return conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id, artifacts.c.finalized_at.is_(None), artifacts.c.active_uploads >= 0)
        .values(active_uploads=artifacts.c.active_uploads + 1, updated_at=utcnow()),
    ).rowcount > 0


def finish_upload(conn: Connection, artifact_id: UUID) -> None:
    conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id, artifacts.c.finalized_at.is_(None), artifacts.c.active_uploads > 0)
        .values(active_uploads=artifacts.c.active_uploads - 1, updated_at=utcnow()),
    )


def begin_finalize(conn: Connection, artifact_id: UUID) -> bool:
    return conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id, artifacts.c.finalized_at.is_(None), artifacts.c.active_uploads == 0)
        .values(active_uploads=ARTIFACT_FINALIZING, updated_at=utcnow()),
    ).rowcount > 0


def cancel_finalize(conn: Connection, artifact_id: UUID) -> None:
    conn.execute(
        artifacts.update()
        .where(
            artifacts.c.id == artifact_id,
            artifacts.c.finalized_at.is_(None),
            artifacts.c.active_uploads == ARTIFACT_FINALIZING,
        )
        .values(active_uploads=0, updated_at=utcnow()),
    )
