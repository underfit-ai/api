from __future__ import annotations

import logging

from underfit_api.config import FileStorageConfig, S3StorageConfig, config
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)


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
