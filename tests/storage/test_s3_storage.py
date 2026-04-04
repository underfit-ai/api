from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import boto3
import pytest
from moto import mock_aws

from underfit_api.config import S3StorageConfig
from underfit_api.storage.s3 import _CHUNK_SIZE, S3Storage

BUCKET = "test-bucket"


def _count_parts(storage: S3Storage, key: str) -> int:
    return len(storage._list_parts(key))  # noqa: SLF001


@pytest.fixture
def storage() -> Iterator[S3Storage]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield S3Storage(S3StorageConfig(bucket=BUCKET, prefix="pfx", region="us-east-1"))


def test_write_stays_plain(storage: S3Storage) -> None:
    storage.write("f", b"hello")
    assert storage.read("f") == b"hello"
    assert _count_parts(storage, "f") == 0

    big = b"x" * (_CHUNK_SIZE * 2 + 100)
    storage.write("big", big)
    assert storage.read("big") == big
    assert _count_parts(storage, "big") == 0

    async def gen() -> AsyncIterator[bytes]:
        for i in range(0, len(big), 1000):
            yield big[i : i + 1000]

    total = asyncio.run(storage.write_stream("streamed", gen()))
    assert total == len(big)
    assert storage.read("streamed") == big
    assert _count_parts(storage, "streamed") == 0


def test_append_and_chunking(storage: S3Storage) -> None:
    # First append creates a single chunk
    result = storage.append("f", b"aaa")
    assert result.byte_offset == 0
    assert _count_parts(storage, "f") == 1

    # Append within the same chunk
    result = storage.append("f", b"bbb")
    assert result.byte_offset == 3
    assert storage.read("f") == b"aaabbb"
    assert _count_parts(storage, "f") == 1

    # Append that overflows into a second chunk
    storage.append("log", b"a" * (_CHUNK_SIZE - 10))
    storage.append("log", b"b" * 100)
    assert _count_parts(storage, "log") == 2
    assert storage.read("log") == b"a" * (_CHUNK_SIZE - 10) + b"b" * 100

    # Ranged read spanning the chunk boundary
    data = storage.read("log")
    offset = _CHUNK_SIZE - 10
    assert storage.read("log", byte_offset=offset, byte_count=20) == data[offset : offset + 20]
    assert storage.read("log", byte_offset=offset) == data[offset:]

    # Streaming read across chunks
    assert b"".join(storage.read_stream("log")) == data

    # Single append that creates many chunks at once
    storage.append("big", b"x")
    storage.append("big", b"y" * (_CHUNK_SIZE * 2 + 50))
    assert storage.read("big") == b"x" + b"y" * (_CHUNK_SIZE * 2 + 50)
    assert _count_parts(storage, "big") == 3


def test_format_migration(storage: S3Storage) -> None:
    # Plain → chunked: append to a file created with write
    storage.write("f", b"aaa")
    assert _count_parts(storage, "f") == 0
    result = storage.append("f", b"bbb")
    assert result.byte_offset == 3
    assert storage.read("f") == b"aaabbb"
    assert _count_parts(storage, "f") == 1

    # Chunked → plain: write overwrites a chunked file
    storage.append("g", b"a" * (_CHUNK_SIZE + 1))
    assert _count_parts(storage, "g") == 2
    storage.write("g", b"replaced")
    assert _count_parts(storage, "g") == 0
    assert storage.read("g") == b"replaced"


def test_chunked_metadata(storage: S3Storage) -> None:
    data = b"z" * (_CHUNK_SIZE * 2 + 42)
    storage.append("f", data)

    assert storage.exists("f")
    assert storage.size("f") == len(data)
    s = storage.stat("f")
    assert s.size == len(data)
    assert s.last_modified is not None

    storage.delete("f")
    assert not storage.exists("f")

    with pytest.raises(FileNotFoundError):
        storage.read("gone")
    with pytest.raises(FileNotFoundError):
        storage.delete("gone")


def test_listing_collapses_parts(storage: S3Storage) -> None:
    storage.write("dir/a.txt", b"hello")
    storage.append("dir/b.txt", b"x" * (_CHUNK_SIZE + 1))
    entries = storage.list_dir("dir")
    names = [e.name for e in entries]
    assert "a.txt" in names
    assert "b.txt" in names
    assert not any(".part" in n for n in names)
    b_entry = next(e for e in entries if e.name == "b.txt")
    assert b_entry.size == _CHUNK_SIZE + 1
    assert storage.list_files("dir") == ["dir/a.txt", "dir/b.txt"]
