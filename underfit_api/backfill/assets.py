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

_ARTIFACT_MANIFEST = re.compile(r"^([^/]+)/artifacts/([^/]+)/manifest\.json$")
_MEDIA = re.compile(r"^([^/]+)/media/(image|video|audio|html)/(.+)_(-?\d+)_(\d+)(\.[^/]+)$")


def reconcile_assets(conn: Connection, storage: Storage, run_id: UUID, run_keys: list[str]) -> None:
    for key in run_keys:
        if m := _ARTIFACT_MANIFEST.match(key):
            try:
                artifact_id = UUID(m.group(2))
            except ValueError:
                continue
            _ingest_artifact(conn, storage, run_id, artifact_id)
        elif m := _MEDIA.match(key):
            storage_key = key[len(f"{run_id}/"):]
            media_id = uuid5(run_id, storage_key)
            if conn.execute(media.select().where(media.c.id == media_id)).first() is None:
                conn.execute(media.insert().values(
                    id=media_id, run_id=run_id, type=m.group(2), key=m.group(3),
                    step=int(m.group(4)), index=int(m.group(5)),
                    storage_key=storage_key, metadata=None, created_at=utcnow(),
                ))


def _ingest_artifact(conn: Connection, storage: Storage, run_id: UUID, artifact_id: UUID) -> None:
    if not (run_row := conn.execute(runs.select().where(runs.c.id == run_id)).first()):
        return
    storage_prefix = f"artifacts/{artifact_id}"
    base = f"{run_row.storage_key}/{storage_prefix}"
    metadata_key = f"{base}/artifact.json"
    try:
        manifest = json.loads(storage.read(f"{base}/manifest.json"))
        metadata = json.loads(storage.read(metadata_key)) if storage.exists(metadata_key) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning("Skipping artifact %s: invalid or missing manifest/metadata", artifact_id)
        return
    files_dir = f"{base}/files"
    uploaded_paths = {path[len(files_dir) + 1:] for path in storage.list_files(files_dir)}
    declared_paths = {f for f in manifest.get("files", []) if isinstance(f, str) and f}
    finalized = uploaded_paths == declared_paths
    stored_size_bytes = sum(storage.size(f"{files_dir}/{path}") for path in uploaded_paths)
    now = utcnow()
    values = dict(
        project_id=run_row.project_id, step=metadata.get("step"),
        name=metadata.get("name", str(artifact_id)), type=metadata.get("type", "dataset"),
        storage_key=storage_prefix, finalized_at=now if finalized else None,
        stored_size_bytes=stored_size_bytes if finalized else None, metadata=metadata.get("metadata"),
        updated_at=now,
    )
    if conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is None:
        conn.execute(artifacts.insert().values(id=artifact_id, run_id=run_id, created_at=now, **values))
    else:
        conn.execute(artifacts.update().where(artifacts.c.id == artifact_id).values(**values))
