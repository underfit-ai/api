from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

from underfit_api.config import FileStorageConfig
from underfit_api.storage import DirEntry, FileStat


class FileStorage:
    def __init__(self, config: FileStorageConfig) -> None:
        self.base = Path(config.base)
        self._tmp = self.base.parent / f".{self.base.name}.tmp"

    def _resolve(self, key: str) -> Path:
        resolved = (self.base / key).resolve()
        if not resolved.is_relative_to(self.base.resolve()):
            raise ValueError("Path traversal detected")
        return resolved

    def _prepare_write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._tmp.mkdir(parents=True, exist_ok=True)

    def write(self, key: str, content: bytes) -> None:
        path = self._resolve(key)
        self._prepare_write(path)
        with tempfile.NamedTemporaryFile(dir=self._tmp, delete=False) as f:
            f.write(content)
        Path(f.name).replace(path)

    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int:
        path = self._resolve(key)
        self._prepare_write(path)
        total = 0
        tmp: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._tmp, delete=False) as f:
                tmp = Path(f.name)
                async for chunk in stream:
                    f.write(chunk)
                    total += len(chunk)
            tmp.replace(path)
        except Exception:
            if tmp:
                tmp.unlink(missing_ok=True)
            raise
        return total

    def read(self, key: str) -> bytes:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        return path.read_bytes()

    def read_stream(
        self, key: str, chunk_size: int = 262144, byte_offset: int = 0, byte_count: int | None = None,
    ) -> Iterator[bytes]:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        remaining = byte_count
        with path.open("rb") as f:
            f.seek(byte_offset)
            while remaining is None or remaining > 0:
                chunk = f.read(chunk_size if remaining is None else min(chunk_size, remaining))
                if not chunk:
                    break
                yield chunk
                if remaining is not None:
                    remaining -= len(chunk)

    def delete(self, key: str) -> None:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        path.unlink()
        base = self.base.resolve()
        parent = path.parent
        while parent != base and parent.is_relative_to(base):
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def size(self, key: str) -> int:
        return self._resolve(key).stat().st_size

    def stat(self, key: str) -> FileStat:
        path = self._resolve(key)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        stat = path.stat()
        last_modified = format_datetime(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc), usegmt=True)
        return FileStat(size=stat.st_size, last_modified=last_modified, etag=None)

    def list_dir(self, prefix: str) -> list[DirEntry]:
        path = self._resolve(prefix)
        if not path.is_dir():
            return []
        entries = [DirEntry(name=item.name, is_directory=item.is_dir()) for item in path.iterdir()]
        entries.sort(key=lambda e: (not e.is_directory, e.name))
        return entries

    def list_files(self, prefix: str) -> list[str]:
        path = self._resolve(prefix)
        if not path.is_dir():
            return []
        base = self.base.resolve()
        return sorted(str(p.resolve().relative_to(base)) for p in path.rglob("*") if p.is_file())
