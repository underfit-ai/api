from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import boto3
import pytest
from moto import mock_aws

from underfit_api.config import S3StorageConfig
from underfit_api.storage.s3 import S3Storage

BUCKET = "test-bucket"


@pytest.fixture
def storage() -> Iterator[S3Storage]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield S3Storage(S3StorageConfig(bucket=BUCKET, prefix="pfx", region="us-east-1"))


def test_s3_storage_reads_and_streams(storage: S3Storage) -> None:
    data = b"x" * 5000
    storage.write("plain.txt", data)
    assert storage.read("plain.txt") == data
    assert storage.read("plain.txt", byte_offset=100, byte_count=20) == data[100:120]

    async def gen() -> AsyncIterator[bytes]:
        for i in range(0, len(data), 1000):
            yield data[i : i + 1000]

    assert asyncio.run(storage.write_stream("streamed.txt", gen())) == len(data)
    assert b"".join(storage.read_stream("streamed.txt")) == data


def test_s3_storage_metadata_and_listing(storage: S3Storage) -> None:
    storage.write("dir/a.txt", b"a")
    storage.write("dir/sub/b.txt", b"bb")

    entries = storage.list_dir("dir")
    assert [(entry.name, entry.is_directory) for entry in entries] == [("sub", True), ("a.txt", False)]
    assert storage.list_files("dir") == ["dir/a.txt", "dir/sub/b.txt"]
    assert storage.size("dir/a.txt") == 1
    assert storage.stat("dir/a.txt").size == 1

    storage.delete("dir/a.txt")
    assert not storage.exists("dir/a.txt")
    with pytest.raises(FileNotFoundError):
        storage.read("dir/a.txt")
