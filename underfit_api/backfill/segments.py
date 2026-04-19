from __future__ import annotations

import logging
import re
from uuid import UUID

import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Scalar
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.schema import log_segments, run_workers, scalar_segments
from underfit_api.storage import Storage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^logs/([^/]+)/segments/(\d+)\.log$")
_SCALAR = re.compile(r"^scalars/([^/]+)/r(\d+)/(\d+)\.jsonl$")


def reconcile_segments(conn: Connection, storage: Storage, run_id: UUID) -> None:
    log_keys_by_worker: dict[UUID, set[str]] = {}
    scalar_keys_by_worker: dict[UUID, set[str]] = {}
    prefix = f"{run_id}/"
    for key in storage.list_files(f"{run_id}/logs"):
        rel = key[len(prefix):]
        if m := _LOG.match(rel):
            worker_id = _ensure_worker(conn, run_id, m.group(1))
            log_keys_by_worker.setdefault(worker_id, set()).add(rel)
            _ingest_log_segment(conn, storage, worker_id, key, rel, int(m.group(2)))

    for key in storage.list_files(f"{run_id}/scalars"):
        rel = key[len(prefix):]
        if m := _SCALAR.match(rel):
            worker_id = _ensure_worker(conn, run_id, m.group(1))
            scalar_keys_by_worker.setdefault(worker_id, set()).add(rel)
            _ingest_scalar_segment(conn, storage, worker_id, key, rel, int(m.group(2)), int(m.group(3)))

    seen_workers = set(log_keys_by_worker) | set(scalar_keys_by_worker)
    for worker in workers_repo.list_by_run(conn, run_id):
        if worker.id not in seen_workers:
            conn.execute(run_workers.delete().where(run_workers.c.id == worker.id))
            continue
        log_keys = log_keys_by_worker.get(worker.id, {""})
        scalar_keys = scalar_keys_by_worker.get(worker.id, {""})
        conn.execute(log_segments.delete().where(
            log_segments.c.worker_id == worker.id, sa.not_(log_segments.c.storage_key.in_(log_keys)),
        ))
        conn.execute(scalar_segments.delete().where(
            scalar_segments.c.worker_id == worker.id, sa.not_(scalar_segments.c.storage_key.in_(scalar_keys)),
        ))


def _ensure_worker(conn: Connection, run_id: UUID, worker_label: str) -> UUID:
    if worker := workers_repo.get(conn, run_id, worker_label):
        return worker.id
    return workers_repo.create(conn, run_id, worker_label).id


def _ingest_log_segment(
    conn: Connection, storage: Storage, worker_id: UUID, full_key: str, storage_key: str, start_line: int,
) -> None:
    lines = storage.read(full_key).decode().splitlines()
    if not lines:
        return
    end_line = start_line + len(lines)
    existing = conn.execute(sa.select(log_segments.c.end_line).where(
        log_segments.c.worker_id == worker_id, log_segments.c.start_line == start_line,
    )).scalar()
    if existing == end_line:
        return
    now = utcnow()
    log_seg_repo.upsert(
        conn, worker_id, start_line=start_line, end_line=end_line,
        start_at=now, end_at=now, storage_key=storage_key,
    )
    workers_repo.touch(conn, worker_id)


def _ingest_scalar_segment(
    conn: Connection, storage: Storage, worker_id: UUID, full_key: str, storage_key: str,
    resolution: int, start_line: int,
) -> None:
    points: list[Scalar] = []
    for raw_line in storage.read(full_key).decode().splitlines():
        if not raw_line:
            continue
        try:
            points.append(Scalar.model_validate_json(raw_line))
        except ValidationError:
            logger.warning("Stopping scalar ingest at invalid line in %s", full_key)
            break
    if not points:
        return
    end_line = start_line + len(points)
    existing = conn.execute(sa.select(scalar_segments.c.end_line).where(
        scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == resolution,
        scalar_segments.c.start_line == start_line,
    )).scalar()
    if existing == end_line:
        return
    scalar_seg_repo.upsert(
        conn, worker_id, resolution, start_line=start_line, end_line=end_line,
        end_step=points[-1].step, start_at=points[0].timestamp, end_at=points[-1].timestamp,
        storage_key=storage_key,
    )
    workers_repo.touch(conn, worker_id)
