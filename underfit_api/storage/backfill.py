from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import Connection, Engine

from underfit_api.config import BackfillConfig, BufferConfig
from underfit_api.schema import accounts, artifacts, log_segments, media, projects, runs, scalar_segments, users
from underfit_api.storage.file import FileStorage
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^([^/]+)/logs/(.+)\.log$")
_SCALAR = re.compile(r"^([^/]+)/scalars/r(\d+)\.jsonl$")
_ARTIFACT = re.compile(r"^([^/]+)/artifacts/([^/]+)/manifest\.json$")
_MEDIA = re.compile(r"^([^/]+)/media/([^/]+)/\d+$")


class RunMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    project: str
    user: str = "local"
    name: str | None = None
    status: Literal["queued", "running", "finished", "failed", "cancelled"] = "finished"
    config: dict[str, object] | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_ts(raw: str | None) -> datetime:
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass
    return _now()


class BackfillService:
    def __init__(
        self, storage: Storage, engine: Engine,
        backfill_config: BackfillConfig, buffer_config: BufferConfig,
    ) -> None:
        self._storage = storage
        self._engine = engine
        self._config = backfill_config
        self._max_segment_bytes = buffer_config.max_segment_bytes
        self._pending: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._scan_loop()))
        self._tasks.append(asyncio.create_task(self._process_loop()))
        if self._config.realtime and isinstance(self._storage, FileStorage):
            self._storage.watch(self._pending.add)

    async def stop(self) -> None:
        if isinstance(self._storage, FileStorage):
            self._storage.stop_watching()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _scan_loop(self) -> None:
        while True:
            try:
                keys = await asyncio.to_thread(self._storage.list_files, "")
                self._pending.update(keys)
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

    # ---- synchronous processing (runs in thread) ----

    def _process_batch(self, keys: set[str]) -> None:
        by_run: dict[str, list[str]] = {}
        for key in keys:
            by_run.setdefault(key.split("/", 1)[0], []).append(key)

        with self._engine.begin() as conn:
            for run_dir, run_keys in sorted(by_run.items()):
                try:
                    run_uuid = UUID(run_dir)
                except ValueError:
                    continue
                if not self._storage.exists(f"{run_dir}/run.json"):
                    continue
                if not (run_id := self._ensure_run(conn, run_uuid)):
                    continue
                seen_media: set[str] = set()
                for key in run_keys:
                    if key.endswith("/run.json"):
                        continue
                    if m := _LOG.match(key):
                        self._ingest_log(conn, run_id, m.group(2), key)
                    elif m := _SCALAR.match(key):
                        self._ingest_scalar(conn, run_id, int(m.group(2)), key)
                    elif m := _ARTIFACT.match(key):
                        self._ingest_artifact(conn, run_id, m.group(2))
                    elif m := _MEDIA.match(key):
                        media_id = m.group(2)
                        if media_id not in seen_media:
                            seen_media.add(media_id)
                            self._ingest_media(conn, run_id, media_id)

    # ---- run management ----

    def _ensure_run(self, conn: Connection, run_uuid: UUID) -> UUID | None:
        try:
            metadata = RunMetadata.model_validate_json(self._storage.read(f"{run_uuid}/run.json"))
        except (ValidationError, json.JSONDecodeError, FileNotFoundError):
            return None
        if not metadata.project or not metadata.user:
            return None

        project_name = metadata.project
        handle = metadata.user
        name = metadata.name or str(run_uuid)
        status = metadata.status
        run_config = metadata.config

        if not (account_id := self._resolve_account(conn, handle)):
            return None
        project_id = self._resolve_project(conn, account_id, project_name)

        if not (existing := conn.execute(runs.select().where(runs.c.id == run_uuid)).first()):
            now = _now()
            conn.execute(runs.insert().values(
                id=run_uuid, project_id=project_id, user_id=account_id,
                name=name, status=status, config=run_config, created_at=now, updated_at=now,
            ))
        elif existing.name != name or existing.status != status or existing.config != run_config:
            conn.execute(runs.update().where(runs.c.id == run_uuid).values(
                name=name, status=status, config=run_config, updated_at=_now(),
            ))
        return run_uuid

    def _resolve_account(self, conn: Connection, handle: str) -> UUID | None:
        if row := conn.execute(accounts.select().where(accounts.c.handle == handle)).first():
            return row.id
        if handle != "local":
            return None
        uid = uuid4()
        now = _now()
        conn.execute(accounts.insert().values(id=uid, handle="local", type="USER"))
        conn.execute(users.insert().values(
            id=uid, email="local@underfit.local", name="Local User", created_at=now, updated_at=now,
        ))
        return uid

    def _resolve_project(self, conn: Connection, account_id: UUID, name: str) -> UUID:
        if row := conn.execute(projects.select().where(
            projects.c.account_id == account_id, projects.c.name == name,
        )).first():
            return row.id
        project_id = uuid4()
        now = _now()
        conn.execute(projects.insert().values(
            id=project_id, account_id=account_id, name=name,
            visibility="private", created_at=now, updated_at=now,
        ))
        return project_id

    # ---- segment building ----

    def _get_position(
        self, conn: Connection, table: sa.Table, file_size: int, scope: sa.ColumnElement[bool],
    ) -> tuple[UUID | None, int, int, int]:
        """Returns (latest_segment_id, latest_byte_count, byte_position, line_position)."""
        latest = conn.execute(
            table.select().where(scope).order_by(table.c.byte_offset.desc()).limit(1),
        ).first()
        if latest is None:
            return None, 0, 0, 0
        byte_end = latest.byte_offset + latest.byte_count
        if file_size < byte_end:
            conn.execute(sa.delete(table).where(scope))
            return None, 0, 0, 0
        return latest.id, latest.byte_count, byte_end, latest.end_line

    def _build_segments(
        self, conn: Connection, table: sa.Table, run_id: UUID, storage_key: str,
        line_sizes: list[int], timestamps: list[datetime], byte_start: int, line_start: int,
        latest_id: UUID | None, latest_byte_count: int, extra_cols: dict[str, object],
    ) -> None:
        i = 0
        byte_cursor = byte_start
        line_cursor = line_start

        if latest_id is not None and latest_byte_count < self._max_segment_bytes:
            room = self._max_segment_bytes - latest_byte_count
            added_bytes = 0
            start_i = i
            while i < len(line_sizes) and added_bytes + line_sizes[i] <= room:
                added_bytes += line_sizes[i]
                i += 1
            if i > start_i:
                conn.execute(table.update().where(table.c.id == latest_id).values(
                    end_line=line_cursor + (i - start_i),
                    end_at=timestamps[i - 1],
                    byte_count=latest_byte_count + added_bytes,
                ))
                byte_cursor += added_bytes
                line_cursor += i - start_i

        now = _now()
        while i < len(line_sizes):
            chunk_bytes = 0
            chunk_start = i
            while i < len(line_sizes):
                if i > chunk_start and chunk_bytes + line_sizes[i] > self._max_segment_bytes:
                    break
                chunk_bytes += line_sizes[i]
                i += 1
            n = i - chunk_start
            conn.execute(table.insert().values(
                id=uuid4(), run_id=run_id,
                start_line=line_cursor, end_line=line_cursor + n,
                start_at=timestamps[chunk_start], end_at=timestamps[i - 1],
                byte_offset=byte_cursor, byte_count=chunk_bytes,
                storage_key=storage_key, created_at=now, **extra_cols,
            ))
            byte_cursor += chunk_bytes
            line_cursor += n

    # ---- log ingestion ----

    def _ingest_log(self, conn: Connection, run_id: UUID, worker_id: str, storage_key: str) -> None:
        file_size = self._storage.size(storage_key)
        scope = sa.and_(log_segments.c.run_id == run_id, log_segments.c.worker_id == worker_id)
        seg_id, seg_bytes, byte_pos, line_pos = self._get_position(conn, log_segments, file_size, scope)
        if byte_pos >= file_size:
            return

        new_data = self._storage.read(storage_key, byte_offset=byte_pos)
        if (last_nl := new_data.rfind(b"\n")) < 0:
            return
        if not (raw_lines := new_data[:last_nl + 1].split(b"\n")[:-1]):
            return

        now = _now()
        self._build_segments(
            conn, log_segments, run_id, storage_key,
            [len(line) + 1 for line in raw_lines], [now] * len(raw_lines),
            byte_pos, line_pos, seg_id, seg_bytes, {"worker_id": worker_id},
        )

    # ---- scalar ingestion ----

    def _ingest_scalar(self, conn: Connection, run_id: UUID, resolution: int, storage_key: str) -> None:
        file_size = self._storage.size(storage_key)
        scope = sa.and_(scalar_segments.c.run_id == run_id, scalar_segments.c.resolution == resolution)
        seg_id, seg_bytes, byte_pos, line_pos = self._get_position(conn, scalar_segments, file_size, scope)
        if byte_pos >= file_size:
            return

        new_data = self._storage.read(storage_key, byte_offset=byte_pos)
        valid_lines: list[bytes] = []
        timestamps: list[datetime] = []
        for raw_line in new_data.split(b"\n"):
            if not raw_line:
                continue
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                break
            valid_lines.append(raw_line)
            timestamps.append(_parse_ts(obj.get("timestamp")))

        if not valid_lines:
            return

        self._build_segments(
            conn, scalar_segments, run_id, storage_key,
            [len(line) + 1 for line in valid_lines], timestamps,
            byte_pos, line_pos, seg_id, seg_bytes, {"resolution": resolution},
        )

    # ---- artifact ingestion ----

    def _ingest_artifact(self, conn: Connection, run_id: UUID, artifact_id_str: str) -> None:
        try:
            artifact_id = UUID(artifact_id_str)
        except ValueError:
            return

        existing = conn.execute(artifacts.select().where(artifacts.c.id == artifact_id)).first()
        if existing is not None and existing.status == "finalized":
            return

        manifest_key = f"{run_id}/artifacts/{artifact_id_str}/manifest.json"
        try:
            manifest = json.loads(self._storage.read(manifest_key))
        except (json.JSONDecodeError, FileNotFoundError):
            return

        if not (run_row := conn.execute(runs.select().where(runs.c.id == run_id)).first()):
            return

        files_list = manifest.get("files", [])
        storage_prefix = f"{run_id}/artifacts/{artifact_id_str}"
        now = _now()

        if existing is None:
            conn.execute(artifacts.insert().values(
                id=artifact_id, project_id=run_row.project_id, run_id=run_id,
                step=manifest.get("step"), name=manifest.get("name", artifact_id_str),
                type=manifest.get("type", "dataset"), status="open",
                storage_key=storage_prefix, declared_file_count=len(files_list),
                uploaded_file_count=0, created_at=now, updated_at=now,
                metadata=manifest.get("metadata"),
            ))

        files_dir = f"{storage_prefix}/files"
        uploaded = len(self._storage.list_files(files_dir)) if self._storage.exists(files_dir) else 0
        declared = len(files_list)
        uploaded = min(uploaded, declared)
        finalized = uploaded >= declared > 0

        conn.execute(artifacts.update().where(artifacts.c.id == artifact_id).values(
            uploaded_file_count=uploaded,
            status="finalized" if finalized else "open",
            finalized_at=now if finalized else None,
            updated_at=now,
        ))

    # ---- media ingestion ----

    def _ingest_media(self, conn: Connection, run_id: UUID, media_id_str: str) -> None:
        try:
            media_id = UUID(media_id_str)
        except ValueError:
            return

        media_dir = f"{run_id}/media/{media_id_str}"
        entries = self._storage.list_dir(media_dir)
        if not (file_count := sum(1 for e in entries if not e.is_directory and e.name.isdigit())):
            return

        existing = conn.execute(media.select().where(media.c.id == media_id)).first()

        key_name = media_id_str
        step: int | None = None
        media_type = "image"
        meta: dict[str, object] | None = None

        metadata_key = f"{media_dir}/metadata.json"
        if self._storage.exists(metadata_key):
            try:
                sidecar = json.loads(self._storage.read(metadata_key))
                key_name = sidecar.get("key", media_id_str)
                step = sidecar.get("step")
                media_type = sidecar.get("type", "image")
                meta = sidecar.get("metadata")
            except (json.JSONDecodeError, FileNotFoundError):
                pass

        now = _now()
        if existing is None:
            conn.execute(media.insert().values(
                id=media_id, run_id=run_id, key=key_name, step=step,
                type=media_type, storage_key=media_dir, count=file_count,
                metadata=meta, created_at=now,
            ))
        elif existing.count != file_count:
            conn.execute(media.update().where(media.c.id == media_id).values(count=file_count))
