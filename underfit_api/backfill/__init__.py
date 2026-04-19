from __future__ import annotations

import logging
import time
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.backfill.assets import reconcile_assets
from underfit_api.backfill.runs import ensure_run
from underfit_api.backfill.segments import reconcile_segments
from underfit_api.config import config
from underfit_api.dependencies import AppContext
from underfit_api.schema import runs
from underfit_api.storage import Storage

__all__ = ["refresh_run", "sync"]

logger = logging.getLogger(__name__)


def sync(ctx: AppContext, conn: Connection) -> None:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_full_sync < config.backfill.debounce_s:
            return
        ctx.last_full_sync = now
    _run_sync(ctx, conn)


def refresh_run(ctx: AppContext, conn: Connection, run_id: UUID) -> None:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_run_sync.get(run_id, 0.0) < config.backfill.debounce_s:
            return
        ctx.last_run_sync[run_id] = now
    _run_refresh(ctx, conn, run_id)


def _run_sync(ctx: AppContext, conn: Connection) -> None:
    storage = ctx.storage
    storage_ids = _storage_run_ids(storage)
    db_ids = {row.id for row in conn.execute(sa.select(runs.c.id))}
    for run_id in db_ids - storage_ids:
        conn.execute(runs.delete().where(runs.c.id == run_id))
        ctx.last_run_sync.pop(run_id, None)
    for run_id in storage_ids:
        try:
            ensure_run(conn, storage, run_id)
        except Exception:
            logger.exception("Backfill ensure_run error for run %s", run_id)


def _run_refresh(ctx: AppContext, conn: Connection, run_id: UUID) -> None:
    storage = ctx.storage
    if not storage.exists(f"{run_id}/run.json"):
        conn.execute(runs.delete().where(runs.c.id == run_id))
        ctx.last_run_sync.pop(run_id, None)
        return
    try:
        project_id = ensure_run(conn, storage, run_id)
    except Exception:
        logger.exception("Backfill ensure_run error for run %s", run_id)
        return
    if project_id is None:
        return
    reconcile_segments(conn, storage, run_id)
    reconcile_assets(conn, storage, run_id, project_id)


def _storage_run_ids(storage: Storage) -> set[UUID]:
    ids: set[UUID] = set()
    for entry in storage.list_dir(""):
        if not entry.is_directory:
            continue
        try:
            run_id = UUID(entry.name)
        except ValueError:
            continue
        if storage.exists(f"{run_id}/run.json"):
            ids.add(run_id)
    return ids
