from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import boto3
import pytest
from moto import mock_aws

from underfit_api.config import S3StorageConfig
from underfit_api.storage.s3 import MULTIPART_PART_SIZE, S3Storage
from underfit_api.storage.types import WatchableStorage

BUCKET = "test-bucket"


@pytest.fixture
def storage() -> Iterator[S3Storage]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield S3Storage(S3StorageConfig(bucket=BUCKET, prefix="pfx", region="us-east-1"))


def test_s3_storage_reads_and_streams(storage: S3Storage, monkeypatch: pytest.MonkeyPatch) -> None:
    data = b"x" * (MULTIPART_PART_SIZE + 1024)
    storage.write("plain.txt", data)
    assert storage.read("plain.txt") == data
    assert storage.read("plain.txt", byte_offset=100, byte_count=20) == data[100:120]
    upload_part_calls = 0
    client = vars(storage)["_client"]
    upload_part = client.upload_part

    def counting_upload_part(*args: object, **kwargs: object) -> dict[str, str]:
        nonlocal upload_part_calls
        upload_part_calls += 1
        return upload_part(*args, **kwargs)

    monkeypatch.setattr(client, "upload_part", counting_upload_part)

    async def gen() -> AsyncIterator[bytes]:
        for i in range(0, len(data), 262144):
            yield data[i : i + 262144]

    assert asyncio.run(storage.write_stream("streamed.txt", gen())) == len(data)
    assert upload_part_calls == 2
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
    assert not isinstance(storage, WatchableStorage)
