from __future__ import annotations

import asyncio
import logging
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection, Engine

from underfit_api.backfill.assets import reconcile_assets
from underfit_api.backfill.runs import ensure_run, reconcile_project_ui
from underfit_api.backfill.segments import reconcile_segments
from underfit_api.config import BackfillConfig
from underfit_api.schema import runs
from underfit_api.storage.types import Storage, WatchableStorage

logger = logging.getLogger(__name__)


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
        if not (ensured := ensure_run(conn, self._storage, run_uuid)):
            return
        run_id, project_id, metadata = ensured
        run_keys = self._storage.list_files(str(run_uuid))
        reconcile_segments(conn, self._storage, run_id, run_keys, metadata.summary)
        reconcile_assets(conn, self._storage, run_id, run_keys)
        reconcile_project_ui(conn, self._storage, project_id)
