from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
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


def test_file_storage_crud_and_listing(tmp_path: Path) -> None:
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))

    async def stream() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"cd"

    storage.write("root.txt", b"x")
    assert asyncio.run(storage.write_stream("dir/file.bin", stream())) == 4
    assert storage.read("dir/file.bin", 1, 2) == b"bc"
    assert b"".join(storage.read_stream("dir/file.bin", chunk_size=3)) == b"abcd"
    assert [(e.name, e.is_directory) for e in storage.list_dir("")] == [("dir", True), ("root.txt", False)]
    assert storage.list_files("dir") == ["dir/file.bin"] and storage.stat("dir/file.bin").size == 4
    storage.delete("root.txt")
    assert not storage.exists("root.txt")
