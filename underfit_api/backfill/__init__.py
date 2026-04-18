from __future__ import annotations

import logging
import time
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.backfill import ui_state as ui_state_mod
from underfit_api.backfill.assets import reconcile_assets
from underfit_api.backfill.runs import ensure_run
from underfit_api.backfill.segments import reconcile_segments
from underfit_api.backfill.ui_state import write_project as write_project_ui_state
from underfit_api.backfill.ui_state import write_run as write_run_ui_state
from underfit_api.dependencies import AppContext
from underfit_api.schema import runs
from underfit_api.storage.types import Storage

__all__ = ["refresh_run", "sync", "write_project_ui_state", "write_run_ui_state"]

logger = logging.getLogger(__name__)

DEBOUNCE_SECONDS = 1.0


def sync(ctx: AppContext, conn: Connection) -> None:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_full_sync < DEBOUNCE_SECONDS:
            return
        ctx.last_full_sync = now
    _run_sync(conn, ctx.storage)


def refresh_run(ctx: AppContext, conn: Connection, run_id: UUID) -> None:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_run_sync.get(run_id, 0.0) < DEBOUNCE_SECONDS:
            return
        ctx.last_run_sync[run_id] = now
    _run_refresh(conn, ctx.storage, run_id)


def _run_sync(conn: Connection, storage: Storage) -> None:
    ui = ui_state_mod.load(storage)
    storage_ids = _storage_run_ids(storage)
    db_ids = {row.id for row in conn.execute(sa.select(runs.c.id))}
    for run_id in db_ids - storage_ids:
        conn.execute(runs.delete().where(runs.c.id == run_id))
    for run_id in storage_ids:
        try:
            ensure_run(conn, storage, run_id, ui)
        except Exception:
            logger.exception("Backfill ensure_run error for run %s", run_id)


def _run_refresh(conn: Connection, storage: Storage, run_id: UUID) -> None:
    run_keys = storage.list_files(str(run_id))
    if not run_keys:
        conn.execute(runs.delete().where(runs.c.id == run_id))
        return
    ui = ui_state_mod.load(storage)
    if not (ensured := ensure_run(conn, storage, run_id, ui)):
        return
    _, metadata = ensured
    reconcile_segments(conn, storage, run_id, run_keys, metadata.summary)
    reconcile_assets(conn, storage, run_id, run_keys)


def _storage_run_ids(storage: Storage) -> set[UUID]:
    ids: set[UUID] = set()
    for entry in storage.list_dir(""):
        if not entry.is_directory:
            continue
        try:
            run_id = UUID(entry.name)
        except ValueError:
            continue
        if storage.list_files(str(run_id)):
            ids.add(run_id)
    return ids
