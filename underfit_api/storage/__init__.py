from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from typing import NamedTuple, Protocol

from underfit_api.config import FileStorageConfig, S3StorageConfig, config

logger = logging.getLogger(__name__)


class DirEntry(NamedTuple):
    name: str
    is_directory: bool
    size: int
    last_modified: str


class FileStat(NamedTuple):
    size: int
    last_modified: str | None
    etag: str | None


class Storage(Protocol):
    def write(self, key: str, content: bytes) -> None: ...
    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int: ...
    def read(self, key: str) -> bytes: ...
    def read_stream(
        self, key: str, chunk_size: int = 262144, byte_offset: int = 0, byte_count: int | None = None,
    ) -> Iterator[bytes]: ...
    def delete(self, key: str) -> None: ...
    def exists(self, key: str) -> bool: ...
    def size(self, key: str) -> int: ...
    def stat(self, key: str) -> FileStat: ...
    def list_dir(self, prefix: str) -> list[DirEntry]: ...
    def list_files(self, prefix: str) -> list[str]: ...


def build_storage() -> Storage:
    if isinstance(config.storage, FileStorageConfig):
        from underfit_api.storage.file import FileStorage  # noqa: PLC0415
        return FileStorage(config.storage)
    if isinstance(config.storage, S3StorageConfig):
        from underfit_api.storage.s3 import S3Storage  # noqa: PLC0415
        return S3Storage(config.storage)
    raise ValueError(f"Unsupported storage type: {config.storage.type}")


def delete_prefix(storage: Storage, prefix: str) -> None:
    # NOTE: Deletion is DB-authoritative, partially-deleted artifacts aren't acceptible so we always
    # delete from the DB first and make storage cleanup best-effort. We can always garbage-collect
    # orphaned files if necessary.
    try:
        keys = storage.list_files(prefix)
    except Exception:
        logger.exception("Failed to list storage keys for deletion: %s", prefix)
        return
    for key in keys:
        try:
            storage.delete(key)
        except Exception:
            logger.exception("Failed to delete storage key: %s", key)


__all__ = ["Storage", "build_storage", "delete_prefix"]
