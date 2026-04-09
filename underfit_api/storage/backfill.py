from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4, uuid5

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection, Engine

from underfit_api.config import BackfillConfig
from underfit_api.helpers import utcnow
from underfit_api.repositories import projects as projects_repo
from underfit_api.schema import (
    accounts,
    artifacts,
    log_segments,
    media,
    projects,
    run_workers,
    runs,
    scalar_segments,
    users,
)
from underfit_api.storage.file import FileStorage
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^([^/]+)/logs/([^/]+)/segments/(\d+)\.log$")
_SCALAR = re.compile(r"^([^/]+)/scalars/([^/]+)/r(\d+)/(\d+)\.jsonl$")
_ARTIFACT = re.compile(r"^([^/]+)/artifacts/([^/]+)/(manifest|artifact)\.json$")
_MEDIA = re.compile(r"^([^/]+)/media/([^/]+)/(.+)_(none|-?\d+)_(\d+)(\.[^/]+)$")


class RunMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    project: str
    user: str = "local"
    name: str | None = None
    terminal_state: Literal["finished", "failed", "cancelled"] | None = None
    config: dict[str, object] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


def _parse_ts(raw: str | None) -> datetime:
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass
    return utcnow()


class BackfillService:
    def __init__(self, storage: Storage, engine: Engine, backfill_config: BackfillConfig) -> None:
        if not isinstance(storage, FileStorage):
            raise RuntimeError("Backfill is only supported with file storage")
        self._storage = storage
        self._engine = engine
        self._config = backfill_config
        self._pending: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._scan_loop()))
        self._tasks.append(asyncio.create_task(self._process_loop()))
        if self._config.realtime:
            self._storage.watch(self._pending.add)

    async def stop(self) -> None:
        self._storage.stop_watching()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _scan_loop(self) -> None:
        while True:
            try:
                self._pending.update(await asyncio.to_thread(self._storage.list_files, ""))
            except Exception:
                logger.exception("Backfill scan error")
            await asyncio.sleep(self._config.scan_interval_ms / 1000)

    async def _process_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.debounce_ms / 1000)
            if not self._pending:
                continue
            keys = self._pending.copy()
            self._pending.clear()
            try:
                await asyncio.to_thread(self._process_batch, keys)
            except Exception:
                logger.exception("Backfill process error")

    def _process_batch(self, keys: set[str]) -> None:
        run_dirs = {key.split("/", 1)[0] for key in keys if "/" in key}
        with self._engine.begin() as conn:
            for run_dir in sorted(run_dirs):
                try:
                    run_uuid = UUID(run_dir)
                except ValueError:
                    continue
                if not self._storage.exists(f"{run_dir}/run.json"):
                    continue
                if not (run_id := self._ensure_run(conn, run_uuid)):
                    continue
                run_keys = self._storage.list_files(run_dir)
                self._rebuild_segments(conn, run_id, run_keys)
                self._ingest_assets(conn, run_id, run_keys)

    def _ensure_run(self, conn: Connection, run_uuid: UUID) -> UUID | None:
        try:
            metadata = RunMetadata.model_validate_json(self._storage.read(f"{run_uuid}/run.json"))
        except (ValidationError, json.JSONDecodeError, FileNotFoundError):
            return None
        if not metadata.project or not metadata.user:
            return None
        run_name = (metadata.name or str(run_uuid)).lower()
        storage_key = str(run_uuid)
        if not (account_id := self._resolve_account(conn, metadata.user)):
            return None
        project_id = self._resolve_project(conn, account_id, metadata.project)
        if not (existing := conn.execute(runs.select().where(runs.c.id == run_uuid)).first()):
            now = utcnow()
            conn.execute(runs.insert().values(
                id=run_uuid,
                project_id=project_id,
                user_id=account_id,
                launch_id=str(run_uuid),
                name=run_name,
                storage_key=storage_key,
                terminal_state=metadata.terminal_state,
                config=metadata.config,
                metadata=metadata.metadata,
                created_at=now,
                updated_at=now,
            ))
        elif (
            existing.name != run_name
            or existing.storage_key != storage_key
            or existing.terminal_state != metadata.terminal_state
            or existing.config != metadata.config
            or existing.metadata != metadata.metadata
        ):
            conn.execute(runs.update().where(runs.c.id == run_uuid).values(
                name=run_name,
                storage_key=storage_key,
                terminal_state=metadata.terminal_state,
                config=metadata.config,
                metadata=metadata.metadata,
                updated_at=utcnow(),
            ))
        return run_uuid

    def _resolve_account(self, conn: Connection, handle: str) -> UUID | None:
        if row := conn.execute(accounts.select().where(accounts.c.handle == handle)).first():
            return row.id
        if handle != "local":
            return None
        uid = uuid4()
        now = utcnow()
        conn.execute(accounts.insert().values(id=uid, handle="local", type="USER"))
        conn.execute(users.insert().values(
            id=uid, email="local@underfit.local", name="Local User", bio="", created_at=now, updated_at=now,
        ))
        return uid

    def _resolve_project(self, conn: Connection, account_id: UUID, name: str) -> UUID:
        name = name.lower()
        if row := conn.execute(projects.select().where(
            projects.c.account_id == account_id, projects.c.name == name,
        )).first():
            return row.id
        project_id = uuid4()
        now = utcnow()
        conn.execute(projects.insert().values(
            id=project_id, account_id=account_id, name=name, storage_key=str(project_id),
            metadata={}, visibility="private", created_at=now, updated_at=now,
        ))
        projects_repo.create_alias(conn, project_id, account_id, name)
        return project_id

    def _ensure_worker(self, conn: Connection, run_id: UUID, worker_label: str) -> UUID:
        if row := conn.execute(
            run_workers.select().where(run_workers.c.run_id == run_id, run_workers.c.worker_label == worker_label),
        ).first():
            return row.id
        rwid = uuid4()
        conn.execute(run_workers.insert().values(
            id=rwid, run_id=run_id, worker_label=worker_label, last_heartbeat=utcnow(), joined_at=utcnow(),
        ))
        return rwid

    def _rebuild_segments(self, conn: Connection, run_id: UUID, run_keys: list[str]) -> None:
        worker_ids = [row.id for row in conn.execute(run_workers.select().where(run_workers.c.run_id == run_id)).all()]
        if worker_ids:
            conn.execute(sa.delete(log_segments).where(log_segments.c.worker_id.in_(worker_ids)))
            conn.execute(sa.delete(scalar_segments).where(scalar_segments.c.worker_id.in_(worker_ids)))
        for key in sorted(run_keys):
            if m := _LOG.match(key):
                rwid = self._ensure_worker(conn, run_id, m.group(2))
                self._ingest_log_segment(conn, run_id, rwid, key[len(f"{run_id}/"):], int(m.group(3)))
            elif m := _SCALAR.match(key):
                rwid = self._ensure_worker(conn, run_id, m.group(2))
                self._ingest_scalar_segment(
                    conn, run_id, rwid, key[len(f"{run_id}/"):], int(m.group(3)), int(m.group(4)),
                )

    def _ingest_log_segment(
        self, conn: Connection, run_id: UUID, worker_id: UUID, storage_key: str, start_line: int,
    ) -> None:
        lines = self._storage.read(f"{run_id}/{storage_key}").decode().splitlines()
        if not lines:
            return
        now = utcnow()
        conn.execute(log_segments.insert().values(
            id=uuid4(),
            worker_id=worker_id,
            start_line=start_line,
            end_line=start_line + len(lines),
            start_at=now,
            end_at=now,
            storage_key=storage_key,
            created_at=now,
        ))

    def _ingest_scalar_segment(
        self, conn: Connection, run_id: UUID, worker_id: UUID, storage_key: str, resolution: int, start_line: int,
    ) -> None:
        count = 0
        start_at: datetime | None = None
        end_at: datetime | None = None
        for raw_line in self._storage.read(f"{run_id}/{storage_key}").decode().splitlines():
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                break
            ts = _parse_ts(payload.get("timestamp"))
            if start_at is None:
                start_at = ts
            end_at = ts
            count += 1
        if count == 0 or start_at is None or end_at is None:
            return
        conn.execute(scalar_segments.insert().values(
            id=uuid4(),
            worker_id=worker_id,
            resolution=resolution,
            start_line=start_line,
            end_line=start_line + count,
            start_at=start_at,
            end_at=end_at,
            storage_key=storage_key,
            created_at=utcnow(),
        ))

    def _ingest_assets(self, conn: Connection, run_id: UUID, run_keys: list[str]) -> None:
        seen_artifacts: set[str] = set()
        seen_media: set[str] = set()
        for key in run_keys:
            if m := _ARTIFACT.match(key):
                artifact_id = m.group(2)
                if artifact_id not in seen_artifacts:
                    seen_artifacts.add(artifact_id)
                    self._ingest_artifact(conn, run_id, artifact_id)
            elif m := _MEDIA.match(key):
                storage_prefix = f"media/{m.group(2)}/{m.group(3)}_{m.group(4)}"
                if storage_prefix not in seen_media:
                    seen_media.add(storage_prefix)
                    self._ingest_media(
                        conn, run_id, run_keys, storage_prefix, m.group(6), m.group(3),
                        None if m.group(4) == "none" else int(m.group(4)), m.group(2),
                    )

    def _ingest_artifact(self, conn: Connection, run_id: UUID, artifact_id_str: str) -> None:
        try:
            artifact_id = UUID(artifact_id_str)
        except ValueError:
            return
        if not (run_row := conn.execute(runs.select().where(runs.c.id == run_id)).first()):
            return
        try:
            manifest = json.loads(
                self._storage.read(f"{run_row.storage_key}/artifacts/{artifact_id_str}/manifest.json"),
            )
        except (json.JSONDecodeError, FileNotFoundError):
            return
        storage_prefix = f"artifacts/{artifact_id_str}"
        metadata: dict[str, object] = {}
        metadata_key = f"{run_row.storage_key}/{storage_prefix}/artifact.json"
        if self._storage.exists(metadata_key):
            try:
                metadata = json.loads(self._storage.read(metadata_key))
            except (json.JSONDecodeError, FileNotFoundError):
                metadata = {}
        now = utcnow()
        if conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is None:
            conn.execute(artifacts.insert().values(
                id=artifact_id,
                project_id=run_row.project_id,
                run_id=run_id,
                step=metadata.get("step"),
                name=metadata.get("name", artifact_id_str),
                type=metadata.get("type", "dataset"),
                storage_key=storage_prefix,
                stored_size_bytes=None,
                created_at=now,
                updated_at=now,
                metadata=metadata.get("metadata"),
            ))
        files_dir = f"{run_row.storage_key}/{storage_prefix}/files"
        uploaded_paths = {
            path[len(files_dir) + 1:]
            for path in self._storage.list_files(files_dir)
        } if self._storage.exists(files_dir) else set()
        declared_paths = {file for file in manifest.get("files", []) if isinstance(file, str) and file}
        finalized = uploaded_paths == declared_paths
        stored_size_bytes = sum(self._storage.size(f"{files_dir}/{path}") for path in uploaded_paths)
        conn.execute(artifacts.update().where(artifacts.c.id == artifact_id).values(
            finalized_at=now if finalized else None,
            stored_size_bytes=stored_size_bytes if finalized else None,
            updated_at=now,
        ))

    def _ingest_media(
        self, conn: Connection, run_id: UUID, run_keys: list[str], storage_prefix: str, ext: str, key_name: str,
        step: int | None, media_type: str,
    ) -> None:
        media_id = uuid5(run_id, f"{storage_prefix}{ext}")
        prefix = f"{run_id}/{storage_prefix}_"
        file_count = sum(
            1
            for key in run_keys
            if key.startswith(prefix) and key.endswith(ext) and key[len(prefix):-len(ext)].isdigit()
        )
        if not file_count:
            return
        existing = conn.execute(media.select().where(media.c.id == media_id)).first()
        if existing is None:
            conn.execute(media.insert().values(
                id=media_id,
                run_id=run_id,
                key=key_name,
                step=step,
                type=media_type,
                storage_prefix=storage_prefix,
                ext=ext,
                count=file_count,
                metadata=None,
                created_at=utcnow(),
            ))
        elif existing.count != file_count:
            conn.execute(media.update().where(media.c.id == media_id).values(count=file_count))
