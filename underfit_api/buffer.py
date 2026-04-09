from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import UTCDatetime
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.storage import Storage

K = TypeVar("K")
V = TypeVar("V")


def _log_storage_key(run_id: UUID, worker_label: str, start_line: int) -> str:
    return f"{run_id}/logs/{worker_label}/segments/{start_line}.log"


def _scalar_storage_key(run_id: UUID, worker_label: str, resolution: int, start_line: int) -> str:
    return f"{run_id}/scalars/{worker_label}/r{resolution}/{start_line}.jsonl"


def get_scalar_resolutions() -> list[int]:
    return sorted(list({1, *config.buffer.scalar_resolutions}))


class LogLine(BaseModel):
    timestamp: UTCDatetime
    content: str


class ScalarPoint(BaseModel):
    step: int | None = None
    values: dict[str, float]
    timestamp: UTCDatetime


@dataclass
class _LineBuffer(Generic[V]):
    lines: list[V] = field(default_factory=list)
    byte_count: int = 0
    start_line: int = 0
    last_persisted_at: datetime = field(default_factory=utcnow)

    @property
    def end_line(self) -> int:
        return self.start_line + len(self.lines)


@dataclass
class _Accumulator:
    sums: dict[str, float] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    n: int = 0
    last_step: int | None = None
    last_timestamp: datetime | None = None


@dataclass
class _WorkerLockState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    refs: int = 0


# Locking invariant: self._dict_lock guards both the buffer dict and the per-worker lock dict.
# Never hold it while acquiring a per-worker lock or doing IO — snapshot, release, then iterate.
class _BaseBuffer(Generic[K, V]):
    def __init__(self) -> None:
        self._dict_lock = threading.Lock()
        self._worker_locks: dict[UUID, _WorkerLockState] = {}
        self._buffers: dict[K, _LineBuffer[V]] = {}

    def _worker_id_of(self, key: K) -> UUID: raise NotImplementedError
    def flush(self, conn: Connection, storage: Storage, worker_id: UUID) -> None: raise NotImplementedError
    def persist(self, conn: Connection, storage: Storage, worker_id: UUID) -> None: raise NotImplementedError

    @contextmanager
    def _worker_lock(self, worker_id: UUID) -> Iterator[None]:
        with self._dict_lock:
            state = self._worker_locks.setdefault(worker_id, _WorkerLockState())
            state.refs += 1
        with state.lock:
            yield
        with self._dict_lock:
            state.refs -= 1

    def persist_due(self, conn: Connection, storage: Storage) -> None:
        cutoff = utcnow() - timedelta(milliseconds=config.buffer.persist_interval_ms)
        with self._dict_lock:
            ids = {self._worker_id_of(k) for k, b in self._buffers.items()
                   if b.lines and b.last_persisted_at < cutoff}
        for wid in ids:
            self.persist(conn, storage, wid)

    def flush_all(self, conn: Connection, storage: Storage) -> None:
        with self._dict_lock:
            ids = {self._worker_id_of(k) for k, b in self._buffers.items() if b.lines}
        for wid in ids:
            self.flush(conn, storage, wid)

    def flush_inactive(self, conn: Connection, storage: Storage) -> None:
        with self._dict_lock:
            active = {self._worker_id_of(k) for k, b in self._buffers.items() if b.lines}
        for wid in workers_repo.get_inactive_ids(conn, active):
            self.flush(conn, storage, wid)
            with self._worker_lock(wid), self._dict_lock:
                for k, b in list(self._buffers.items()):
                    if self._worker_id_of(k) == wid and not b.lines:
                        self._buffers.pop(k)
                has_buffers = wid in {self._worker_id_of(k) for k in self._buffers}
                if not has_buffers and (state := self._worker_locks.get(wid)) and state.refs == 1:
                    self._worker_locks.pop(wid, None)


class LogBuffer(_BaseBuffer[UUID, LogLine]):
    def _worker_id_of(self, key: UUID) -> UUID:
        return key

    def get_end_line(self, conn: Connection, worker_id: UUID) -> int:
        with self._worker_lock(worker_id):
            buf = self._buffers.get(worker_id)
            return buf.end_line if buf and buf.lines else log_seg_repo.get_end_line(conn, worker_id)

    def append(self, conn: Connection, worker_id: UUID, start_line: int, lines: list[LogLine]) -> int | None:
        with self._worker_lock(worker_id):
            expected = self.get_end_line(conn, worker_id)
            if start_line != expected:
                return expected
            with self._dict_lock:
                buf = self._buffers.setdefault(worker_id, _LineBuffer(start_line=start_line))
            for line in lines:
                buf.lines.append(line)
                buf.byte_count += len(line.content.encode()) + 1
            return None

    def persist(self, conn: Connection, storage: Storage, worker_id: UUID, *, clear: bool = False) -> None:
        with self._worker_lock(worker_id):
            buf = self._buffers.get(worker_id)
            if not buf or not buf.lines:
                return
            if not (worker := workers_repo.get_by_id(conn, worker_id)):
                raise RuntimeError("Worker not found")
            content = "".join(f"{line.content}\n" for line in buf.lines)
            storage_key = _log_storage_key(worker.run_id, worker.worker_label, buf.start_line)
            storage.write(storage_key, content.encode())
            log_seg_repo.upsert(
                conn, worker_id,
                start_line=buf.start_line, end_line=buf.end_line,
                start_at=buf.lines[0].timestamp, end_at=buf.lines[-1].timestamp,
                storage_key=storage_key,
            )
            buf.last_persisted_at = utcnow()
            if clear:
                buf.start_line = buf.end_line
                buf.lines.clear()
                buf.byte_count = 0

    def flush(self, conn: Connection, storage: Storage, worker_id: UUID) -> None:
        self.persist(conn, storage, worker_id, clear=True)

    def buffer_start_line(self, worker_id: UUID) -> int | None:
        with self._worker_lock(worker_id):
            buf = self._buffers.get(worker_id)
            return buf.start_line if buf and buf.lines else None

    def flush_if_needed(self, conn: Connection, storage: Storage, worker_id: UUID) -> None:
        with self._worker_lock(worker_id):
            buf = self._buffers.get(worker_id)
            if buf and buf.byte_count >= config.buffer.max_segment_bytes:
                self.flush(conn, storage, worker_id)

    def read_buffered(self, worker_id: UUID, cursor: int, count: int) -> list[LogLine]:
        with self._worker_lock(worker_id):
            buf = self._buffers.get(worker_id)
            if not buf or not buf.lines:
                return []
            start = max(0, cursor - buf.start_line)
            end = min(len(buf.lines), cursor + count - buf.start_line)
            return buf.lines[start:end]


class ScalarBuffer(_BaseBuffer[tuple[UUID, int], ScalarPoint]):
    def __init__(self) -> None:
        super().__init__()
        self._accumulators: dict[tuple[UUID, int], _Accumulator] = {}

    def _worker_id_of(self, key: tuple[UUID, int]) -> UUID:
        return key[0]

    def get_end_line(self, conn: Connection, worker_id: UUID, resolution: int = 1) -> int:
        with self._worker_lock(worker_id):
            buf = self._buffers.get((worker_id, resolution))
            return buf.end_line if buf and buf.lines else scalar_seg_repo.get_end_line(conn, worker_id, resolution)

    def append(self, conn: Connection, worker_id: UUID, start_line: int, scalars: list[ScalarPoint]) -> int | None:
        with self._worker_lock(worker_id):
            expected = self.get_end_line(conn, worker_id, 1)
            if start_line != expected:
                return expected
            with self._dict_lock:
                buf = self._buffers.setdefault((worker_id, 1), _LineBuffer(start_line=start_line))
            for scalar in scalars:
                buf.lines.append(scalar)
                buf.byte_count += len(scalar.model_dump_json().encode()) + 1
                self._feed_accumulators(conn, worker_id, scalar)
            return None

    def _feed_accumulators(self, conn: Connection, worker_id: UUID, scalar: ScalarPoint) -> None:
        for resolution in get_scalar_resolutions()[1:]:
            k = (worker_id, resolution)
            acc = self._accumulators.setdefault(k, _Accumulator())
            for key, val in scalar.values.items():
                acc.sums[key] = acc.sums.get(key, 0.0) + val
                acc.counts[key] = acc.counts.get(key, 0) + 1
            acc.n += 1
            acc.last_step = scalar.step
            acc.last_timestamp = scalar.timestamp
            if acc.n >= resolution:
                self._emit_accumulator(conn, worker_id, resolution, acc)
                self._accumulators.pop(k)

    def _emit_accumulator(self, conn: Connection, worker_id: UUID, resolution: int, acc: _Accumulator) -> None:
        averaged = {k: acc.sums[k] / acc.counts[k] for k in acc.sums}
        point = ScalarPoint(step=acc.last_step, values=averaged, timestamp=acc.last_timestamp or utcnow())
        start_line = scalar_seg_repo.get_end_line(conn, worker_id, resolution)
        with self._dict_lock:
            buf = self._buffers.setdefault((worker_id, resolution), _LineBuffer(start_line=start_line))
        buf.lines.append(point)
        buf.byte_count += len(point.model_dump_json().encode()) + 1

    def flush(self, conn: Connection, storage: Storage, worker_id: UUID, *, emit_partial: bool = True) -> None:
        with self._worker_lock(worker_id):
            if emit_partial:
                for resolution in get_scalar_resolutions()[1:]:
                    if (acc := self._accumulators.get((worker_id, resolution))) and acc.n > 0:
                        self._emit_accumulator(conn, worker_id, resolution, acc)
                        self._accumulators.pop((worker_id, resolution))
            for resolution in get_scalar_resolutions():
                self._persist_resolution(conn, storage, worker_id, resolution, clear=True)

    def _persist_resolution(
        self, conn: Connection, storage: Storage, worker_id: UUID, resolution: int, *, clear: bool = False,
    ) -> None:
        buf = self._buffers.get((worker_id, resolution))
        if not buf or not buf.lines:
            return
        if not (worker := workers_repo.get_by_id(conn, worker_id)):
            raise RuntimeError("Worker not found")
        content = "".join(line.model_dump_json() + "\n" for line in buf.lines)
        storage_key = _scalar_storage_key(worker.run_id, worker.worker_label, resolution, buf.start_line)
        storage.write(storage_key, content.encode())
        scalar_seg_repo.upsert(
            conn, worker_id, resolution,
            start_line=buf.start_line, end_line=buf.end_line,
            start_at=buf.lines[0].timestamp, end_at=buf.lines[-1].timestamp,
            storage_key=storage_key,
        )
        buf.last_persisted_at = utcnow()
        if clear:
            buf.start_line = buf.end_line
            buf.lines.clear()
            buf.byte_count = 0

    def persist(self, conn: Connection, storage: Storage, worker_id: UUID) -> None:
        with self._worker_lock(worker_id):
            for resolution in get_scalar_resolutions():
                self._persist_resolution(conn, storage, worker_id, resolution)

    def buffer_start_line(self, worker_id: UUID, resolution: int) -> int | None:
        with self._worker_lock(worker_id):
            buf = self._buffers.get((worker_id, resolution))
            return buf.start_line if buf and buf.lines else None

    def flush_if_needed(self, conn: Connection, storage: Storage, worker_id: UUID) -> None:
        with self._worker_lock(worker_id):
            buf = self._buffers.get((worker_id, 1))
            if buf and buf.byte_count >= config.buffer.max_segment_bytes:
                self.flush(conn, storage, worker_id, emit_partial=False)

    def read_buffered(self, worker_id: UUID, resolution: int) -> list[ScalarPoint]:
        with self._worker_lock(worker_id):
            buf = self._buffers.get((worker_id, resolution))
            return list(buf.lines) if buf and buf.lines else []

    def resolution_line_count(self, conn: Connection, worker_id: UUID, resolution: int) -> int:
        with self._worker_lock(worker_id):
            end = self.get_end_line(conn, worker_id, resolution)
            if buf := self._buffers.get((worker_id, resolution)):
                end = max(end, buf.end_line)
            return end


log_buffer = LogBuffer()
scalar_buffer = ScalarBuffer()
