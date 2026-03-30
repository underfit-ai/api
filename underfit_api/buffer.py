from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, NamedTuple, TypeVar
from uuid import UUID

from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.storage import Storage

T = TypeVar("T")

RESOLUTIONS = [1, 10, 100, 1000, 10000]


class LogLine(NamedTuple):
    timestamp: datetime
    content: str


class ScalarPoint(NamedTuple):
    step: int | None
    values: dict[str, float]
    timestamp: datetime


@dataclass
class _LineBuffer(Generic[T]):
    lines: list[T] = field(default_factory=list)
    byte_count: int = 0
    start_line: int = 0
    created_at: float | None = None

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


class LogBuffer:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._buffers: dict[tuple[UUID, str], _LineBuffer[LogLine]] = {}
        self._total_bytes = 0

    def get_end_line(self, conn: Connection, run_id: UUID, worker_id: str) -> int:
        with self._lock:
            k = (run_id, worker_id)
            buf = self._buffers.get(k)
            if buf and buf.lines:
                return buf.end_line
            return log_seg_repo.get_end_line(conn, run_id, worker_id)

    def append(
        self, conn: Connection, run_id: UUID, worker_id: str, start_line: int, lines: list[LogLine],
    ) -> int | None:
        with self._lock:
            expected = self.get_end_line(conn, run_id, worker_id)
            if start_line != expected:
                return expected
            buf = self._buffers.setdefault((run_id, worker_id), _LineBuffer(start_line=start_line))
            if not buf.lines:
                buf.created_at = time.monotonic()
            for line in lines:
                for sub in line.content.split("\n"):
                    size = len(sub.encode()) + 1
                    buf.lines.append(LogLine(timestamp=line.timestamp, content=sub))
                    buf.byte_count += size
                    self._total_bytes += size
            return None

    def flush(self, conn: Connection, storage: Storage, run_id: UUID, worker_id: str) -> None:
        with self._lock:
            buf = self._buffers.get((run_id, worker_id))
            if not buf or not buf.lines:
                return
            content = "".join(f"{line.content}\n" for line in buf.lines)
            storage_key = f"{run_id}/logs/{worker_id}.log"
            result = storage.append(storage_key, content.encode())
            log_seg_repo.insert(
                conn, run_id, worker_id,
                start_line=buf.start_line, end_line=buf.end_line,
                start_at=buf.lines[0].timestamp, end_at=buf.lines[-1].timestamp,
                byte_offset=result.byte_offset, byte_count=result.byte_count,
                storage_key=storage_key,
            )
            self._total_bytes -= buf.byte_count
            buf.start_line = buf.end_line
            buf.lines.clear()
            buf.byte_count = 0
            buf.created_at = None

    def flush_if_needed(self, conn: Connection, storage: Storage, run_id: UUID, worker_id: str) -> None:
        with self._lock:
            buf = self._buffers.get((run_id, worker_id))
            if buf and buf.byte_count >= config.buffer.max_segment_bytes:
                self.flush(conn, storage, run_id, worker_id)
            while self._total_bytes > config.buffer.max_buffer_bytes:
                nonempty = (k for k, b in self._buffers.items() if b.lines)
                largest = max(nonempty, key=lambda k: self._buffers[k].byte_count, default=None)
                if largest is None:
                    break
                self.flush(conn, storage, largest[0], largest[1])

    def flush_all(self, conn: Connection, storage: Storage) -> None:
        with self._lock:
            for k, buf in list(self._buffers.items()):
                if buf.lines:
                    self.flush(conn, storage, k[0], k[1])

    def flush_stale(self, conn: Connection, storage: Storage) -> None:
        with self._lock:
            cutoff = time.monotonic() - config.buffer.max_segment_age_ms / 1000
            for k, buf in list(self._buffers.items()):
                if buf.lines and buf.created_at is not None and buf.created_at < cutoff:
                    self.flush(conn, storage, k[0], k[1])
            self._buffers = {k: b for k, b in self._buffers.items() if b.lines}

    def read_buffered(self, run_id: UUID, worker_id: str, cursor: int, count: int) -> list[LogLine]:
        with self._lock:
            buf = self._buffers.get((run_id, worker_id))
            if not buf or not buf.lines:
                return []
            start = max(0, cursor - buf.start_line)
            end = min(len(buf.lines), cursor + count - buf.start_line)
            if start >= end:
                return []
            return buf.lines[start:end]


class ScalarBuffer:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._buffers: dict[tuple[UUID, int], _LineBuffer[ScalarPoint]] = {}
        self._accumulators: dict[tuple[UUID, int], _Accumulator] = {}
        self._total_bytes = 0

    def get_end_line(self, conn: Connection, run_id: UUID, resolution: int = 0) -> int:
        with self._lock:
            k = (run_id, resolution)
            buf = self._buffers.get(k)
            if buf and buf.lines:
                return buf.end_line
            return scalar_seg_repo.get_end_line(conn, run_id, resolution)

    def append(
        self, conn: Connection, run_id: UUID, start_line: int, scalars: list[ScalarPoint],
    ) -> int | None:
        with self._lock:
            expected = self.get_end_line(conn, run_id, 0)
            if start_line != expected:
                return expected
            buf = self._buffers.setdefault((run_id, 0), _LineBuffer(start_line=start_line))
            if not buf.lines:
                buf.created_at = time.monotonic()
            for scalar in scalars:
                buf.lines.append(scalar)
                size = len(self._serialize_scalar(scalar).encode()) + 1
                buf.byte_count += size
                self._total_bytes += size
                self._feed_accumulators(run_id, scalar)
            return None

    def _serialize_scalar(self, s: ScalarPoint) -> str:
        return json.dumps({"step": s.step, "values": s.values, "timestamp": s.timestamp.isoformat() + "Z"})

    def _feed_accumulators(self, run_id: UUID, scalar: ScalarPoint) -> None:
        for tier, stride in enumerate(RESOLUTIONS[:4], start=1):
            k = (run_id, tier)
            acc = self._accumulators.setdefault(k, _Accumulator())
            for key, val in scalar.values.items():
                acc.sums[key] = acc.sums.get(key, 0.0) + val
                acc.counts[key] = acc.counts.get(key, 0) + 1
            acc.n += 1
            acc.last_step = scalar.step
            acc.last_timestamp = scalar.timestamp
            if acc.n >= stride:
                self._emit_accumulator(run_id, tier, acc)
                self._accumulators[k] = _Accumulator()

    def _emit_accumulator(self, run_id: UUID, resolution: int, acc: _Accumulator) -> None:
        averaged = {k: acc.sums[k] / acc.counts[k] for k in acc.sums}
        ts = acc.last_timestamp or utcnow()
        point = ScalarPoint(step=acc.last_step, values=averaged, timestamp=ts)
        buf = self._buffers.setdefault((run_id, resolution), _LineBuffer(start_line=0))
        if not buf.lines:
            buf.created_at = time.monotonic()
        size = len(self._serialize_scalar(point).encode()) + 1
        buf.lines.append(point)
        buf.byte_count += size
        self._total_bytes += size

    def flush(self, conn: Connection, storage: Storage, run_id: UUID, *, emit_partial: bool = True) -> None:
        with self._lock:
            if emit_partial:
                for (rid, tier), acc in list(self._accumulators.items()):
                    if rid == run_id and acc.n > 0:
                        self._emit_accumulator(run_id, tier, acc)
                        self._accumulators[(rid, tier)] = _Accumulator()
            for resolution in range(5):
                self._flush_tier(conn, storage, run_id, resolution)

    def _flush_tier(self, conn: Connection, storage: Storage, run_id: UUID, resolution: int) -> None:
        buf = self._buffers.get((run_id, resolution))
        if not buf or not buf.lines:
            return
        lines_data = [self._serialize_scalar(line) for line in buf.lines]
        content = "".join(s + "\n" for s in lines_data)
        storage_key = f"{run_id}/scalars/r{resolution}.jsonl"
        result = storage.append(storage_key, content.encode())
        scalar_seg_repo.insert(
            conn, run_id, resolution,
            start_line=buf.start_line, end_line=buf.end_line,
            start_at=buf.lines[0].timestamp, end_at=buf.lines[-1].timestamp,
            byte_offset=result.byte_offset, byte_count=result.byte_count,
            storage_key=storage_key,
        )
        self._total_bytes -= buf.byte_count
        new_start = buf.end_line
        buf.lines.clear()
        buf.byte_count = 0
        buf.created_at = None
        buf.start_line = new_start

    def flush_if_needed(self, conn: Connection, storage: Storage, run_id: UUID) -> None:
        with self._lock:
            buf = self._buffers.get((run_id, 0))
            if buf and buf.byte_count >= config.buffer.max_segment_bytes:
                self.flush(conn, storage, run_id, emit_partial=False)
            while self._total_bytes > config.buffer.max_buffer_bytes:
                nonempty = (k for k, b in self._buffers.items() if b.lines)
                largest = max(nonempty, key=lambda k: self._buffers[k].byte_count, default=None)
                if largest is None:
                    break
                self.flush(conn, storage, largest[0], emit_partial=False)

    def flush_all(self, conn: Connection, storage: Storage) -> None:
        with self._lock:
            run_ids = {run_id for (run_id, _res), buf in self._buffers.items() if buf.lines}
            for run_id in run_ids:
                self.flush(conn, storage, run_id)

    def flush_stale(self, conn: Connection, storage: Storage) -> None:
        with self._lock:
            cutoff = time.monotonic() - config.buffer.max_segment_age_ms / 1000
            stale_runs: set[UUID] = set()
            for (run_id, _res), buf in self._buffers.items():
                if buf.lines and buf.created_at is not None and buf.created_at < cutoff:
                    stale_runs.add(run_id)
            for run_id in stale_runs:
                self.flush(conn, storage, run_id)
            self._buffers = {k: b for k, b in self._buffers.items() if b.lines}
            self._accumulators = {k: a for k, a in self._accumulators.items() if a.n > 0}

    def read_buffered(self, run_id: UUID, resolution: int) -> list[ScalarPoint]:
        with self._lock:
            buf = self._buffers.get((run_id, resolution))
            if not buf or not buf.lines:
                return []
            return list(buf.lines)

    def tier_line_count(self, conn: Connection, run_id: UUID, resolution: int) -> int:
        with self._lock:
            end = self.get_end_line(conn, run_id, resolution)
            if buf := self._buffers.get((run_id, resolution)):
                end = max(end, buf.end_line)
            return end


log_buffer = LogBuffer()
scalar_buffer = ScalarBuffer()
