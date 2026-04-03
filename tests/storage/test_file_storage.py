from __future__ import annotations

from pathlib import Path

import pytest

from underfit_api.config import FileStorageConfig
from underfit_api.storage.file import FileStorage


def test_path_traversal_is_blocked(tmp_path: Path) -> None:
    base = tmp_path / "storage"
    storage = FileStorage(FileStorageConfig(base=str(base)))
    with pytest.raises(ValueError, match="Path traversal"):
        storage.write("../escape.txt", b"nope")
    with pytest.raises(ValueError, match="Path traversal"):
        storage.list_dir("../")
    with pytest.raises(ValueError, match="Path traversal"):
        storage.exists("../escape.txt")
