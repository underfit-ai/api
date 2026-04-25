from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.backfill.assets import reconcile_assets, reconcile_project_assets
from underfit_api.backfill.runs import ensure_run
from underfit_api.backfill.segments import reconcile_segments
from underfit_api.config import config
from underfit_api.schema import runs
from underfit_api.storage import Storage

if TYPE_CHECKING:
    from underfit_api.dependencies import AppContext

__all__ = ["refresh_run", "sync"]

logger = logging.getLogger(__name__)


def sync(ctx: AppContext, conn: Connection) -> None:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_full_sync < config.backfill.debounce_s:
            return
        ctx.last_full_sync = now
    storage_ids = _storage_run_ids(ctx.storage)
    db_ids = {row.id for row in conn.execute(sa.select(runs.c.id))}
    for run_id in db_ids - storage_ids:
        conn.execute(runs.delete().where(runs.c.id == run_id))
        ctx.last_run_sync.pop(run_id, None)
    for run_id in storage_ids:
        try:
            ensure_run(conn, ctx.storage, run_id)
        except Exception:
            logger.exception("Backfill ensure_run error for run %s", run_id)


def refresh_run(ctx: AppContext, conn: Connection, run_id: UUID) -> bool:
    with ctx.sync_lock:
        now = time.monotonic()
        if now - ctx.last_run_sync.get(run_id, 0.0) < config.backfill.debounce_s:
            return False
        ctx.last_run_sync[run_id] = now
    if not ctx.storage.exists(f"{run_id}/run.json"):
        conn.execute(runs.delete().where(runs.c.id == run_id))
        ctx.last_run_sync.pop(run_id, None)
        return True
    try:
        project = ensure_run(conn, ctx.storage, run_id)
    except Exception:
        logger.exception("Backfill ensure_run error for run %s", run_id)
        return True
    if project is None:
        return True
    reconcile_segments(conn, ctx.storage, run_id)
    reconcile_assets(conn, ctx.storage, run_id, project.id)
    reconcile_project_assets(conn, ctx.storage, project.id, project.name)
    return True


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
