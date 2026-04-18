from __future__ import annotations

import logging
import time
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.backfill import ui_state
from underfit_api.backfill.assets import reconcile_assets
from underfit_api.backfill.runs import ensure_run
from underfit_api.backfill.segments import reconcile_segments
from underfit_api.config import config
from underfit_api.dependencies import AppContext
from underfit_api.schema import runs
from underfit_api.storage.types import Storage

__all__ = ["refresh_run", "sync", "ui_state"]

logger = logging.getLogger(__name__)


def sync(ctx: AppContext, conn: Connection) -> None:
    if _debounce(ctx, None):
        _run_sync(ctx, conn)


def refresh_run(ctx: AppContext, conn: Connection, run_id: UUID) -> None:
    if _debounce(ctx, run_id):
        _run_refresh(conn, ctx.storage, run_id)


def _debounce(ctx: AppContext, key: UUID | None) -> bool:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_sync.get(key, 0.0) < config.backfill.debounce_s:
            return False
        ctx.last_sync[key] = now
        return True


def _run_sync(ctx: AppContext, conn: Connection) -> None:
    storage = ctx.storage
    state = ui_state.load(storage)
    storage_ids = _storage_run_ids(storage)
    db_ids = {row.id for row in conn.execute(sa.select(runs.c.id))}
    for run_id in db_ids - storage_ids:
        conn.execute(runs.delete().where(runs.c.id == run_id))
        ctx.last_sync.pop(run_id, None)
    for run_id in storage_ids:
        try:
            ensure_run(conn, storage, run_id, state)
        except Exception:
            logger.exception("Backfill ensure_run error for run %s", run_id)


def _run_refresh(conn: Connection, storage: Storage, run_id: UUID) -> None:
    if not storage.list_dir(str(run_id)):
        conn.execute(runs.delete().where(runs.c.id == run_id))
        return
    state = ui_state.load(storage)
    if not (project_id := ensure_run(conn, storage, run_id, state)):
        return
    reconcile_segments(conn, storage, run_id)
    reconcile_assets(conn, storage, run_id, project_id)


def _storage_run_ids(storage: Storage) -> set[UUID]:
    ids: set[UUID] = set()
    for entry in storage.list_dir(""):
        if not entry.is_directory:
            continue
        try:
            ids.add(UUID(entry.name))
        except ValueError:
            continue
    return ids
