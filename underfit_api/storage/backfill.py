from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Literal
from uuid import UUID, uuid4, uuid5

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Connection, Engine

from underfit_api.buffer import ScalarPoint
from underfit_api.config import BackfillConfig
from underfit_api.helpers import utcnow
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import artifacts, log_segments, media, projects, run_workers, runs, scalar_segments
from underfit_api.storage.types import Storage, WatchableStorage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^([^/]+)/logs/([^/]+)/segments/(\d+)\.log$")
_SCALAR = re.compile(r"^([^/]+)/scalars/([^/]+)/r(\d+)/(\d+)\.jsonl$")
_ARTIFACT = re.compile(r"^([^/]+)/artifacts/([^/]+)/(manifest|artifact)\.json$")
_MEDIA = re.compile(r"^([^/]+)/media/(image|video|audio|html)/(.+)_(-?\d+)_(\d+)(\.[^/]+)$")


class RunMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    project: str
    user: str = "local"
    name: str | None = None
    terminal_state: Literal["finished", "failed", "cancelled"] | None = None
    config: dict[str, object] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class BackfillService:
    def __init__(self, storage: Storage, engine: Engine, backfill_config: BackfillConfig) -> None:
        self._storage = storage
        self._watchable = storage if isinstance(storage, WatchableStorage) else None
        self._engine = engine
        self._config = backfill_config
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._tasks.append(asyncio.create_task(self._process_loop()))
        if self._watchable is not None:
            self._watchable.watch(self._watch_enqueue)
            self._pending.update(await asyncio.to_thread(self._collect_pending))
            # Keep periodic scans as a backstop in case filesystem events are missed.
        self._tasks.append(asyncio.create_task(self._scan_loop()))

    async def stop(self) -> None:
        if self._watchable is not None:
            self._watchable.stop_watching()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _enqueue(self, key: str) -> None:
        if "/" in key:
            self._pending.add(key.split("/", 1)[0])

    def _watch_enqueue(self, key: str) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._enqueue, key)

    def _collect_pending(self) -> set[str]:
        pending = {entry.name for entry in self._storage.list_dir("") if entry.is_directory}
        with self._engine.connect() as conn:
            pending.update(str(row.id) for row in conn.execute(sa.select(runs.c.id)))
        return pending

    async def _scan_loop(self) -> None:
        while True:
            try:
                self._pending.update(await asyncio.to_thread(self._collect_pending))
            except Exception:
                logger.exception("Backfill scan error")
            await asyncio.sleep(self._config.scan_interval_s)

    async def _process_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.debounce_ms / 1000)
            if not self._pending:
                continue
            run_dirs = self._pending.copy()
            self._pending.clear()
            try:
                await asyncio.to_thread(self._process_batch, run_dirs)
            except Exception:
                logger.exception("Backfill process error")

    def _process_batch(self, run_dirs: set[str]) -> None:
        for run_dir in sorted(run_dirs):
            try:
                run_uuid = UUID(run_dir)
            except ValueError:
                continue
            try:
                with self._engine.begin() as conn:
                    self._reconcile_run(conn, run_uuid)
            except Exception:
                logger.exception("Backfill reconcile error for run %s", run_uuid)

    def _reconcile_run(self, conn: Connection, run_uuid: UUID) -> None:
        if not self._storage.exists(f"{run_uuid}/run.json"):
            conn.execute(runs.delete().where(runs.c.id == run_uuid))
            return
        # Keep the last good DB state if the file exists but is temporarily unreadable.
        if not (run_id := self._ensure_run(conn, run_uuid)):
            return
        run_keys = self._storage.list_files(str(run_uuid))
        self._reconcile_segments(conn, run_id, run_keys)
        self._reconcile_assets(conn, run_id, run_keys)

    def _ensure_run(self, conn: Connection, run_uuid: UUID) -> UUID | None:
        try:
            metadata = RunMetadata.model_validate_json(self._storage.read(f"{run_uuid}/run.json"))
        except (ValidationError, json.JSONDecodeError, FileNotFoundError):
            logger.warning("Skipping run %s due to invalid run.json", run_uuid)
            return None
        if not metadata.project or not metadata.user:
            return None
        run_name = (metadata.name or str(run_uuid)).lower()
        storage_key = str(run_uuid)
        if not (user_id := self._resolve_user(conn, metadata.user)):
            return None
        project_id = self._resolve_project(conn, user_id, metadata.project)
        if not (existing := conn.execute(runs.select().where(runs.c.id == run_uuid)).first()):
            now = utcnow()
            conn.execute(runs.insert().values(
                id=run_uuid,
                project_id=project_id,
                user_id=user_id,
                launch_id=str(run_uuid),
                name=run_name,
                storage_key=storage_key,
                terminal_state=metadata.terminal_state,
                config=metadata.config,
                metadata=metadata.metadata,
                summary={},
                created_at=now,
                updated_at=now,
            ))
        elif (
            existing.project_id != project_id
            or existing.user_id != user_id
            or existing.name != run_name
            or existing.storage_key != storage_key
            or existing.terminal_state != metadata.terminal_state
            or existing.config != metadata.config
            or existing.metadata != metadata.metadata
        ):
            if existing.project_id != project_id:
                conn.execute(artifacts.delete().where(artifacts.c.run_id == run_uuid))
            conn.execute(runs.update().where(runs.c.id == run_uuid).values(
                project_id=project_id,
                user_id=user_id,
                name=run_name,
                storage_key=storage_key,
                terminal_state=metadata.terminal_state,
                config=metadata.config,
                metadata=metadata.metadata,
                updated_at=utcnow(),
            ))
        return run_uuid

    def _resolve_user(self, conn: Connection, handle: str) -> UUID | None:
        if alias := accounts_repo.get_alias_by_handle(conn, handle):
            if user := users_repo.get_by_id(conn, alias.account_id):
                return user.id
            return None
        if user := users_repo.get_by_handle(conn, handle):
            return user.id
        if handle.lower() != "local":
            return None
        user = users_repo.create(conn, "local@underfit.local", "local", "Local User")
        accounts_repo.create_alias(conn, user.id, "local")
        return user.id

    def _resolve_project(self, conn: Connection, account_id: UUID, name: str) -> UUID:
        name = name.lower()
        if alias := projects_repo.get_alias_by_account_and_name(conn, account_id, name):
            return alias.project_id
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

    def _reconcile_segments(self, conn: Connection, run_id: UUID, run_keys: list[str]) -> None:
        seen_logs: set[str] = set()
        seen_scalars: set[str] = set()
        summary_points: list[ScalarPoint] = []
        for key in sorted(run_keys):
            rel = key[len(f"{run_id}/"):]
            if m := _LOG.match(key):
                rwid = self._ensure_worker(conn, run_id, m.group(2))
                if self._ingest_log_segment(conn, rwid, key, rel, int(m.group(3))):
                    seen_logs.add(rel)
            elif m := _SCALAR.match(key):
                rwid = self._ensure_worker(conn, run_id, m.group(2))
                resolution = int(m.group(3))
                points = self._ingest_scalar_segment(conn, rwid, key, rel, resolution, int(m.group(4)))
                if points is not None:
                    seen_scalars.add(rel)
                    if resolution == 1:
                        summary_points.extend(points)
        worker_ids = sa.select(run_workers.c.id).where(run_workers.c.run_id == run_id).scalar_subquery()
        conn.execute(log_segments.delete().where(
            log_segments.c.worker_id.in_(worker_ids), log_segments.c.storage_key.not_in(seen_logs),
        ))
        conn.execute(scalar_segments.delete().where(
            scalar_segments.c.worker_id.in_(worker_ids), scalar_segments.c.storage_key.not_in(seen_scalars),
        ))
        conn.execute(run_workers.delete().where(
            run_workers.c.run_id == run_id,
            ~sa.exists().where(log_segments.c.worker_id == run_workers.c.id),
            ~sa.exists().where(scalar_segments.c.worker_id == run_workers.c.id),
        ))
        if summary_points:
            runs_repo.update_summary(conn, run_id, summary_points)

    def _ingest_log_segment(
        self, conn: Connection, worker_id: UUID, full_key: str, storage_key: str, start_line: int,
    ) -> bool:
        lines = self._storage.read(full_key).decode().splitlines()
        if not lines:
            return False
        end_line = start_line + len(lines)
        existing = conn.execute(sa.select(log_segments.c.end_line).where(
            log_segments.c.worker_id == worker_id, log_segments.c.start_line == start_line,
        )).scalar()
        if existing == end_line:
            return True
        now = utcnow()
        log_seg_repo.upsert(
            conn, worker_id, start_line=start_line, end_line=end_line,
            start_at=now, end_at=now, storage_key=storage_key,
        )
        conn.execute(run_workers.update().where(run_workers.c.id == worker_id).values(last_heartbeat=now))
        return True

    def _ingest_scalar_segment(
        self, conn: Connection, worker_id: UUID, full_key: str, storage_key: str,
        resolution: int, start_line: int,
    ) -> list[ScalarPoint] | None:
        points: list[ScalarPoint] = []
        for raw_line in self._storage.read(full_key).decode().splitlines():
            if not raw_line:
                continue
            try:
                points.append(ScalarPoint.model_validate_json(raw_line))
            except ValidationError:
                logger.warning("Stopping scalar ingest at invalid line in %s", full_key)
                break
        if not points:
            return None
        end_line = start_line + len(points)
        existing = conn.execute(sa.select(scalar_segments.c.end_line).where(
            scalar_segments.c.worker_id == worker_id,
            scalar_segments.c.resolution == resolution,
            scalar_segments.c.start_line == start_line,
        )).scalar()
        if existing == end_line:
            return []
        scalar_seg_repo.upsert(
            conn, worker_id, resolution, start_line=start_line, end_line=end_line,
            start_at=points[0].timestamp, end_at=points[-1].timestamp, storage_key=storage_key,
        )
        conn.execute(run_workers.update().where(run_workers.c.id == worker_id).values(last_heartbeat=utcnow()))
        return points[(existing - start_line) if existing is not None else 0:]

    def _reconcile_assets(self, conn: Connection, run_id: UUID, run_keys: list[str]) -> None:
        seen_artifacts: set[UUID] = set()
        seen_media: set[UUID] = set()
        for key in run_keys:
            if (m := _ARTIFACT.match(key)) and m.group(3) == "manifest":
                try:
                    artifact_id = UUID(m.group(2))
                except ValueError:
                    continue
                if self._ingest_artifact(conn, run_id, artifact_id):
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

    def _ingest_artifact(self, conn: Connection, run_id: UUID, artifact_id: UUID) -> bool:
        if not (run_row := conn.execute(runs.select().where(runs.c.id == run_id)).first()):
            return False
        try:
            manifest = json.loads(
                self._storage.read(f"{run_row.storage_key}/artifacts/{artifact_id}/manifest.json"),
            )
        except (json.JSONDecodeError, FileNotFoundError):
            logger.warning("Skipping artifact %s for run %s due to invalid manifest.json", artifact_id, run_id)
            return conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is not None
        storage_prefix = f"artifacts/{artifact_id}"
        metadata: dict[str, object] = {}
        metadata_key = f"{run_row.storage_key}/{storage_prefix}/artifact.json"
        if self._storage.exists(metadata_key):
            try:
                metadata = json.loads(self._storage.read(metadata_key))
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("Ignoring invalid artifact metadata in %s", metadata_key)
                metadata = {}
        now = utcnow()
        if conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first() is None:
            conn.execute(artifacts.insert().values(
                id=artifact_id,
                project_id=run_row.project_id,
                run_id=run_id,
                step=metadata.get("step"),
                name=metadata.get("name", str(artifact_id)),
                type=metadata.get("type", "dataset"),
                storage_key=storage_prefix,
                stored_size_bytes=None,
                created_at=now,
                updated_at=now,
                metadata=metadata.get("metadata"),
            ))
        files_dir = f"{run_row.storage_key}/{storage_prefix}/files"
        uploaded_paths = {path[len(files_dir) + 1:] for path in self._storage.list_files(files_dir)}
        declared_paths = {file for file in manifest.get("files", []) if isinstance(file, str) and file}
        finalized = uploaded_paths == declared_paths
        stored_size_bytes = sum(self._storage.size(f"{files_dir}/{path}") for path in uploaded_paths)
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
