from __future__ import annotations

import logging
import re
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Scalar
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

_LOG = re.compile(r"^([^/]+)/logs/([^/]+)/segments/(\d+)\.log$")
_SCALAR = re.compile(r"^([^/]+)/scalars/([^/]+)/r(\d+)/(\d+)\.jsonl$")


def reconcile_segments(
    conn: Connection, storage: Storage, run_id: UUID, run_keys: list[str],
    explicit_summary: dict[str, float] | None,
) -> None:
    last_point: Scalar | None = None
    for key in sorted(run_keys):
        rel = key[len(f"{run_id}/"):]
        if m := _LOG.match(key):
            worker_id = _ensure_worker(conn, run_id, m.group(2))
            _ingest_log_segment(conn, storage, worker_id, key, rel, int(m.group(3)))
        elif m := _SCALAR.match(key):
            worker_id = _ensure_worker(conn, run_id, m.group(2))
            resolution = int(m.group(3))
            new_points = _ingest_scalar_segment(conn, storage, worker_id, key, rel, resolution, int(m.group(4)))
            if resolution == 1 and new_points and (last_point is None or new_points[-1].step > last_point.step):
                last_point = new_points[-1]
    summary = explicit_summary if explicit_summary is not None else (last_point.values if last_point else {})
    runs_repo.update_summary(conn, run_id, summary)


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
    now = utcnow()
    log_seg_repo.upsert(
        conn, worker_id, start_line=start_line, end_line=end_line,
        start_at=now, end_at=now, storage_key=storage_key,
    )
    workers_repo.touch(conn, worker_id)


def _ingest_scalar_segment(
    conn: Connection, storage: Storage, worker_id: UUID, full_key: str, storage_key: str,
    resolution: int, start_line: int,
) -> list[Scalar]:
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
        return []
    scalar_seg_repo.upsert(
        conn, worker_id, resolution, start_line=start_line, end_line=start_line + len(points),
        end_step=points[-1].step, start_at=points[0].timestamp, end_at=points[-1].timestamp,
        storage_key=storage_key,
    )
    workers_repo.touch(conn, worker_id)
    return points
