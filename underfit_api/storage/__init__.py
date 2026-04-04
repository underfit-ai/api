from __future__ import annotations

from underfit_api.config import FileStorageConfig, S3StorageConfig, config
from underfit_api.storage.types import AppendResult, DirEntry, FileStat, Storage


def build_storage() -> Storage:
    if isinstance(config.storage, FileStorageConfig):
        from underfit_api.storage.file import FileStorage  # noqa: PLC0415
        return FileStorage(config.storage)
    if isinstance(config.storage, S3StorageConfig):
        from underfit_api.storage.s3 import S3Storage  # noqa: PLC0415
        return S3Storage(config.storage)
    raise ValueError(f"Unsupported storage type: {config.storage.type}")


storage = build_storage()

__all__ = ["AppendResult", "DirEntry", "FileStat", "Storage", "build_storage", "storage"]
