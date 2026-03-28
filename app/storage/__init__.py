from __future__ import annotations

from app.config import FileStorageConfig, S3StorageConfig, config
from app.storage.types import AppendResult, DirEntry, Storage


def get_storage() -> Storage:
    if isinstance(config.storage, FileStorageConfig):
        from app.storage.file import FileStorage  # noqa: PLC0415
        return FileStorage(config.storage)
    if isinstance(config.storage, S3StorageConfig):
        from app.storage.s3 import S3Storage  # noqa: PLC0415
        return S3Storage(config.storage)
    raise ValueError(f"Unsupported storage type: {config.storage.type}")


__all__ = ["AppendResult", "DirEntry", "Storage", "get_storage"]
