from __future__ import annotations

import json
import logging
import re
from uuid import UUID, uuid5

from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.schema import artifacts, media, runs
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

_ARTIFACT = re.compile(r"^([^/]+)/artifacts/([^/]+)/(manifest|artifact)\.json$")
_MEDIA = re.compile(r"^([^/]+)/media/(image|video|audio|html)/(.+)_(-?\d+)_(\d+)(\.[^/]+)$")


def reconcile_assets(conn: Connection, storage: Storage, run_id: UUID, run_keys: list[str]) -> None:
    seen_artifacts: set[UUID] = set()
    seen_media: set[UUID] = set()
    for key in run_keys:
        if (m := _ARTIFACT.match(key)) and m.group(3) == "manifest":
            try:
                artifact_id = UUID(m.group(2))
            except ValueError:
                continue
            if _ingest_artifact(conn, storage, run_id, artifact_id):
                seen_artifacts.add(artifact_id)
        elif m := _MEDIA.match(key):
            storage_key = key[len(f"{run_id}/"):]
            media_id = uuid5(run_id, storage_key)
            seen_media.add(media_id)
            if conn.execute(media.select().where(media.c.id == media_id)).first() is None:
                conn.execute(media.insert().values(
                    id=media_id, run_id=run_id, type=m.group(2), key=m.group(3),
                    step=int(m.group(4)), index=int(m.group(5)),
                    storage_key=storage_key, metadata=None, created_at=utcnow(),
                ))
    conn.execute(artifacts.delete().where(
        artifacts.c.run_id == run_id, artifacts.c.id.not_in(seen_artifacts),
    ))
    conn.execute(media.delete().where(media.c.run_id == run_id, media.c.id.not_in(seen_media)))


def _ingest_artifact(conn: Connection, storage: Storage, run_id: UUID, artifact_id: UUID) -> bool:
    if not (run_row := conn.execute(runs.select().where(runs.c.id == run_id)).first()):
        return False
    try:
        manifest = json.loads(storage.read(f"{run_row.storage_key}/artifacts/{artifact_id}/manifest.json"))
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning("Skipping artifact %s for run %s due to invalid manifest.json", artifact_id, run_id)
        return conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is not None
    storage_prefix = f"artifacts/{artifact_id}"
    metadata: dict[str, object] = {}
    metadata_key = f"{run_row.storage_key}/{storage_prefix}/artifact.json"
    if storage.exists(metadata_key):
        try:
            metadata = json.loads(storage.read(metadata_key))
        except (json.JSONDecodeError, FileNotFoundError):
            logger.warning("Ignoring invalid artifact metadata in %s", metadata_key)
            metadata = {}
    now = utcnow()
    if conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is None:
        conn.execute(artifacts.insert().values(
            id=artifact_id, project_id=run_row.project_id, run_id=run_id,
            step=metadata.get("step"), name=metadata.get("name", str(artifact_id)),
            type=metadata.get("type", "dataset"), storage_key=storage_prefix,
            stored_size_bytes=None, created_at=now, updated_at=now, metadata=metadata.get("metadata"),
        ))
    files_dir = f"{run_row.storage_key}/{storage_prefix}/files"
    uploaded_paths = {path[len(files_dir) + 1:] for path in storage.list_files(files_dir)}
    declared_paths = {file for file in manifest.get("files", []) if isinstance(file, str) and file}
    finalized = uploaded_paths == declared_paths
    stored_size_bytes = sum(storage.size(f"{files_dir}/{path}") for path in uploaded_paths)
    conn.execute(artifacts.update().where(artifacts.c.id == artifact_id).values(
        project_id=run_row.project_id,
        step=metadata.get("step"),
        name=metadata.get("name", str(artifact_id)),
        type=metadata.get("type", "dataset"),
        finalized_at=now if finalized else None,
        stored_size_bytes=stored_size_bytes if finalized else None,
        metadata=metadata.get("metadata"),
        updated_at=now,
    ))
    return True
