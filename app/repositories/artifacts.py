from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Connection

from app.models import Artifact
from app.schema import artifacts


def get_by_id(conn: Connection, artifact_id: uuid.UUID) -> Artifact | None:
    row = conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first()
    return Artifact.model_validate(row) if row else None


def list_by_project(conn: Connection, project_id: uuid.UUID) -> list[Artifact]:
    rows = conn.execute(
        artifacts.select()
        .where(artifacts.c.project_id == project_id)
        .order_by(artifacts.c.created_at.desc()),
    ).all()
    return [Artifact.model_validate(row) for row in rows]


def create(
    conn: Connection,
    project_id: uuid.UUID,
    run_id: uuid.UUID | None,
    step: int | None,
    name: str,
    artifact_type: str,
    storage_key: str,
    declared_file_count: int,
    metadata: dict[str, object] | None,
) -> Artifact:
    artifact_id = uuid.uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(artifacts.insert().values(
        id=artifact_id,
        project_id=project_id,
        run_id=run_id,
        step=step,
        name=name,
        type=artifact_type,
        status="open",
        storage_key=storage_key,
        declared_file_count=declared_file_count,
        uploaded_file_count=0,
        created_at=now,
        updated_at=now,
        metadata=metadata,
    ))
    result = get_by_id(conn, artifact_id)
    assert result is not None
    return result


def increment_uploaded(conn: Connection, artifact_id: uuid.UUID) -> Artifact | None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id)
        .values(uploaded_file_count=artifacts.c.uploaded_file_count + 1, updated_at=now),
    )
    return get_by_id(conn, artifact_id)


def finalize(conn: Connection, artifact_id: uuid.UUID) -> Artifact | None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        artifacts.update()
        .where(artifacts.c.id == artifact_id)
        .values(status="finalized", finalized_at=now, updated_at=now),
    )
    return get_by_id(conn, artifact_id)
