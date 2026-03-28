from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

from app.storage.types import AppendResult, DirEntry


class _StorageHandler(FileSystemEventHandler):
    def __init__(self, base: Path, callback: Callable[[str], None]) -> None:
        self._base = base.resolve()
        self._callback = callback

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        try:
            key = str(Path(str(event.src_path)).resolve().relative_to(self._base))
        except ValueError:
            return
        self._callback(key)


class FileStorage:
    def __init__(self, base: str) -> None:
        self.base = Path(base)
        self._observer: BaseObserver | None = None

    def _resolve(self, key: str) -> Path:
        resolved = (self.base / key).resolve()
        if not resolved.is_relative_to(self.base.resolve()):
            raise ValueError("Path traversal detected")
        return resolved

    def write(self, key: str, content: bytes) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        with path.open("wb") as f:
            async for chunk in stream:
                f.write(chunk)
                total += len(chunk)
        return total

    def read(self, key: str, byte_offset: int = 0, byte_count: int | None = None) -> bytes:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        with path.open("rb") as f:
            f.seek(byte_offset)
            if byte_count is not None:
                return f.read(byte_count)
            return f.read()

    def read_stream(self, key: str, chunk_size: int = 262144) -> Iterator[bytes]:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                yield chunk

    def append(self, key: str, content: bytes) -> AppendResult:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            offset = f.tell()
            f.write(content)
            return AppendResult(byte_offset=offset, byte_count=len(content))

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def size(self, key: str) -> int:
        return self._resolve(key).stat().st_size

    def list_dir(self, prefix: str) -> list[DirEntry]:
        path = self._resolve(prefix)
        if not path.is_dir():
            return []
        entries: list[DirEntry] = []
        for item in path.iterdir():
            stat = item.stat()
            entries.append(DirEntry(
                name=item.name,
                is_directory=item.is_dir(),
                size=stat.st_size if not item.is_dir() else 0,
                last_modified=_format_mtime(stat.st_mtime),
            ))
        entries.sort(key=lambda e: (not e.is_directory, e.name))
        return entries

    def list_files(self, prefix: str) -> list[str]:
        path = self._resolve(prefix)
        if not path.is_dir():
            return []
        base = self.base.resolve()
        return sorted(str(p.resolve().relative_to(base)) for p in path.rglob("*") if p.is_file())

    def watch(self, callback: Callable[[str], None]) -> None:
        self.base.mkdir(parents=True, exist_ok=True)
        handler = _StorageHandler(self.base, callback)
        observer = Observer()
        observer.schedule(handler, str(self.base), recursive=True)
        observer.start()
        self._observer = observer

    def stop_watching(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()
            self._observer = None


def _format_mtime(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).replace(tzinfo=None).isoformat() + "Z"
