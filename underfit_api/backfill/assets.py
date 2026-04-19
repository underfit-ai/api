from __future__ import annotations

import json
import logging
import re
from uuid import UUID, uuid5

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.helpers import dialect_insert, utcnow
from underfit_api.schema import artifacts, media
from underfit_api.storage import Storage

logger = logging.getLogger(__name__)

_MEDIA = re.compile(r"^media/(image|video|audio|html)/(.+)_(-?\d+)_(\d+)(\.[^/]+)$")


def reconcile_assets(conn: Connection, storage: Storage, run_id: UUID, project_id: UUID) -> None:
    storage_artifact_ids: set[UUID] = set()
    for entry in storage.list_dir(f"{run_id}/artifacts"):
        if not entry.is_directory:
            continue
        try:
            artifact_id = UUID(entry.name)
        except ValueError:
            continue
        if _ingest_artifact(conn, storage, run_id, project_id, artifact_id):
            storage_artifact_ids.add(artifact_id)
    conn.execute(artifacts.delete().where(
        artifacts.c.run_id == run_id, sa.not_(artifacts.c.id.in_(storage_artifact_ids)),
    ))

    prefix = f"{run_id}/"
    storage_media_ids: set[UUID] = set()
    for key in storage.list_files(f"{run_id}/media"):
        rel = key[len(prefix):]
        if not (m := _MEDIA.match(rel)):
            continue
        media_id = uuid5(run_id, rel)
        storage_media_ids.add(media_id)
        conn.execute(dialect_insert(conn, media).values(
            id=media_id, run_id=run_id, type=m.group(1), key=m.group(2),
            step=int(m.group(3)), index=int(m.group(4)),
            storage_key=rel, metadata=None, created_at=utcnow(),
        ).on_conflict_do_nothing(index_elements=["id"]))
    query = media.delete().where(media.c.run_id == run_id)
    if storage_media_ids:
        query = query.where(sa.not_(media.c.id.in_(storage_media_ids)))
    conn.execute(query)


def _ingest_artifact(conn: Connection, storage: Storage, run_id: UUID, project_id: UUID, artifact_id: UUID) -> bool:
    storage_prefix = f"artifacts/{artifact_id}"
    base = f"{run_id}/{storage_prefix}"
    try:
        manifest = json.loads(storage.read(f"{base}/manifest.json"))
        metadata = json.loads(storage.read(f"{base}/artifact.json"))
    except (FileNotFoundError, json.JSONDecodeError) as err:
        logger.warning("Skipping artifact %s: invalid or missing manifest/metadata", artifact_id)
        return isinstance(err, json.JSONDecodeError)
    files_dir = f"{base}/files"
    uploaded_paths = {path[len(files_dir) + 1:] for path in storage.list_files(files_dir)}
    declared_paths = {f for f in manifest.get("files", []) if isinstance(f, str) and f}
    finalized = uploaded_paths == declared_paths
    stored_size_bytes = sum(storage.size(f"{files_dir}/{path}") for path in uploaded_paths)
    now = utcnow()
    values = {
        "project_id": project_id, "step": metadata.get("step"),
        "name": metadata.get("name", str(artifact_id)), "type": metadata.get("type", "dataset"),
        "storage_key": storage_prefix, "finalized_at": now if finalized else None,
        "stored_size_bytes": stored_size_bytes if finalized else None, "metadata": metadata.get("metadata"),
        "updated_at": now,
    }
    conn.execute(dialect_insert(conn, artifacts).values(
        id=artifact_id, run_id=run_id, created_at=now, **values,
    ).on_conflict_do_update(index_elements=["id"], set_=values))
    return True
