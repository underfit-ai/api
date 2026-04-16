from __future__ import annotations

import logging
import re
from uuid import UUID, uuid4

import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import Connection

from underfit_api.buffer import ScalarPoint
from underfit_api.helpers import utcnow
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.schema import log_segments, run_workers, scalar_segments
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^([^/]+)/logs/([^/]+)/segments/(\d+)\.log$")
_SCALAR = re.compile(r"^([^/]+)/scalars/([^/]+)/r(\d+)/(\d+)\.jsonl$")


def reconcile_segments(
    conn: Connection, storage: Storage, run_id: UUID, run_keys: list[str],
    explicit_summary: dict[str, float] | None,
) -> None:
    seen_logs: set[str] = set()
    seen_scalars: set[str] = set()
    last_point: ScalarPoint | None = None
    for key in sorted(run_keys):
        rel = key[len(f"{run_id}/"):]
        if m := _LOG.match(key):
            rwid = _ensure_worker(conn, run_id, m.group(2))
            if _ingest_log_segment(conn, storage, rwid, key, rel, int(m.group(3))):
                seen_logs.add(rel)
        elif m := _SCALAR.match(key):
            rwid = _ensure_worker(conn, run_id, m.group(2))
            resolution = int(m.group(3))
            points = _ingest_scalar_segment(conn, storage, rwid, key, rel, resolution, int(m.group(4)))
            if points is not None:
                seen_scalars.add(rel)
                if resolution == 1 and points and (last_point is None or points[-1].step > last_point.step):
                    last_point = points[-1]
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
    if explicit_summary is not None:
        runs_repo.update(conn, run_id, summary=explicit_summary)
    elif last_point is not None:
        runs_repo.update(conn, run_id, summary=last_point.values)
    else:
        runs_repo.update(conn, run_id, summary={})


def _ensure_worker(conn: Connection, run_id: UUID, worker_label: str) -> UUID:
    if row := conn.execute(
        run_workers.select().where(run_workers.c.run_id == run_id, run_workers.c.worker_label == worker_label),
    ).first():
        return row.id
    rwid = uuid4()
    now = utcnow()
    conn.execute(run_workers.insert().values(
        id=rwid, run_id=run_id, worker_label=worker_label, last_heartbeat=now, joined_at=now,
    ))
    return rwid


def _ingest_log_segment(
    conn: Connection, storage: Storage, worker_id: UUID, full_key: str, storage_key: str, start_line: int,
) -> bool:
    lines = storage.read(full_key).decode().splitlines()
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
    conn: Connection, storage: Storage, worker_id: UUID, full_key: str, storage_key: str,
    resolution: int, start_line: int,
) -> list[ScalarPoint] | None:
    points: list[ScalarPoint] = []
    for raw_line in storage.read(full_key).decode().splitlines():
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
        conn, worker_id, resolution, start_line=start_line, end_line=end_line, end_step=points[-1].step,
        start_at=points[0].timestamp, end_at=points[-1].timestamp, storage_key=storage_key,
    )
    conn.execute(run_workers.update().where(run_workers.c.id == worker_id).values(last_heartbeat=utcnow()))
    return points[(existing - start_line) if existing is not None else 0:]
