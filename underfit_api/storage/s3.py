from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import timezone
from email.utils import format_datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from underfit_api.config import S3StorageConfig
from underfit_api.storage import DirEntry, FileStat

MULTIPART_PART_SIZE = 8 * 1024 * 1024


class S3Storage:
    def __init__(self, config: S3StorageConfig) -> None:
        self._bucket = config.bucket
        self._prefix = config.prefix.strip("/")
        client_kwargs: dict[str, str] = {}
        if config.region:
            client_kwargs["region_name"] = config.region
        if config.endpoint_url:
            client_kwargs["endpoint_url"] = config.endpoint_url
        self._client = boto3.client("s3", **client_kwargs)  # type: ignore[arg-type]

    def _key(self, key: str) -> str:
        if not self._prefix:
            return key
        return f"{self._prefix}/{key}" if key else self._prefix

    def _list_prefix(self, prefix: str) -> str:
        key = self._key(prefix)
        return f"{key}/" if key else ""

    def _relative_key(self, key: str) -> str:
        if not self._prefix:
            return key
        return key[len(self._prefix) + 1:]

    def _head(self, key: str) -> dict[str, Any] | None:
        try:
            return self._client.head_object(Bucket=self._bucket, Key=self._key(key))  # type: ignore[no-any-return]
        except ClientError as e:
            if e.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def write(self, key: str, content: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=content)

    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int:
        kwargs = {"Bucket": self._bucket, "Key": self._key(key)}
        total = 0
        buf = bytearray()
        upload_id: str | None = None
        parts: list[dict[str, object]] = []
        try:
            async for chunk in stream:
                if not chunk:
                    continue
                buf.extend(chunk)
                total += len(chunk)
                if upload_id is None and len(buf) >= MULTIPART_PART_SIZE:
                    upload_id = (await asyncio.to_thread(self._client.create_multipart_upload, **kwargs))["UploadId"]
                while len(buf) >= MULTIPART_PART_SIZE and upload_id is not None:
                    part_number = len(parts) + 1
                    part = bytes(buf[:MULTIPART_PART_SIZE])
                    resp = await asyncio.to_thread(
                        self._client.upload_part, **kwargs, UploadId=upload_id, PartNumber=part_number, Body=part,
                    )
                    parts.append({"ETag": resp["ETag"], "PartNumber": len(parts) + 1})
                    del buf[:MULTIPART_PART_SIZE]
            if upload_id is None:
                await asyncio.to_thread(self._client.put_object, **kwargs, Body=bytes(buf))
                return total
            if buf:
                part_number = len(parts) + 1
                resp = await asyncio.to_thread(
                    self._client.upload_part, **kwargs, UploadId=upload_id, PartNumber=part_number, Body=bytes(buf),
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
            await asyncio.to_thread(
                self._client.complete_multipart_upload, **kwargs, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return total
        except Exception:
            if upload_id is not None:
                await asyncio.to_thread(self._client.abort_multipart_upload, **kwargs, UploadId=upload_id)
            raise

    def read(self, key: str) -> bytes:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        return resp["Body"].read()  # type: ignore[no-any-return]

    def read_stream(
        self, key: str, chunk_size: int = 262144, byte_offset: int = 0, byte_count: int | None = None,
    ) -> Iterator[bytes]:
        kwargs: dict[str, object] = {"Bucket": self._bucket, "Key": self._key(key)}
        if byte_offset or byte_count is not None:
            end = "" if byte_count is None else str(byte_offset + byte_count - 1)
            kwargs["Range"] = f"bytes={byte_offset}-{end}"
        try:
            body = self._client.get_object(**kwargs)["Body"]
        except ClientError as e:
            if e.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        while data := body.read(chunk_size):
            yield data

    def delete(self, key: str) -> None:
        if not self._head(key):
            raise FileNotFoundError(f"File not found: {key}")
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

    def exists(self, key: str) -> bool:
        return self._head(key) is not None

    def size(self, key: str) -> int:
        if not (head := self._head(key)):
            raise FileNotFoundError(f"File not found: {key}")
        return head["ContentLength"]  # type: ignore[no-any-return]

    def stat(self, key: str) -> FileStat:
        if not (head := self._head(key)):
            raise FileNotFoundError(f"File not found: {key}")
        last_modified = format_datetime(head["LastModified"].astimezone(timezone.utc), usegmt=True)
        return FileStat(size=head["ContentLength"], last_modified=last_modified, etag=head.get("ETag"))

    def list_dir(self, prefix: str) -> list[DirEntry]:
        full_prefix = self._list_prefix(prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        entries: list[DirEntry] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"][len(full_prefix):].rstrip("/")
                if name:
                    entries.append(DirEntry(name=name, is_directory=True))
            for obj in page.get("Contents", []):
                name = obj["Key"][len(full_prefix):]
                if name and "/" not in name:
                    entries.append(DirEntry(name=name, is_directory=False))
        entries.sort(key=lambda e: (not e.is_directory, e.name))
        return entries

    def list_files(self, prefix: str) -> list[str]:
        full_prefix = self._list_prefix(prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        files: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                files.append(self._relative_key(obj["Key"]))
        return sorted(files)
