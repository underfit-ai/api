from __future__ import annotations

from app.config import config
from app.storage.types import AppendResult, DirEntry, Storage


def get_storage() -> Storage:
    if config.storage.type == "file":
        from app.storage.file import FileStorage  # noqa: PLC0415
        return FileStorage(config.storage.base)
    raise ValueError(f"Unsupported storage type: {config.storage.type}")


__all__ = ["AppendResult", "DirEntry", "Storage", "get_storage"]
