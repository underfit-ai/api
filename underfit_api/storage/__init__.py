from __future__ import annotations

from underfit_api.config import FileStorageConfig, S3StorageConfig, config
from underfit_api.storage.types import AppendResult, DirEntry, Storage

_storage: Storage | None = None


def get_storage() -> Storage:
    global _storage  # noqa: PLW0603
    if _storage is None:
        if isinstance(config.storage, FileStorageConfig):
            from underfit_api.storage.file import FileStorage  # noqa: PLC0415
            _storage = FileStorage(config.storage)
        elif isinstance(config.storage, S3StorageConfig):
            from underfit_api.storage.s3 import S3Storage  # noqa: PLC0415
            _storage = S3Storage(config.storage)
        else:
            raise ValueError(f"Unsupported storage type: {config.storage.type}")
    return _storage


__all__ = ["AppendResult", "DirEntry", "Storage", "get_storage"]
