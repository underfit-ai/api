from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from underfit_api.config import S3StorageConfig
from underfit_api.storage.types import AppendResult, DirEntry


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
        if self._prefix:
            return f"{self._prefix}/{key}"
        return key

    def write(self, key: str, content: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=content)

    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int:
        chunks: list[bytes] = []
        total = 0
        async for chunk in stream:
            chunks.append(chunk)
            total += len(chunk)
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=b"".join(chunks))
        return total

    def read(self, key: str, byte_offset: int = 0, byte_count: int | None = None) -> bytes:
        kwargs: dict[str, object] = {"Bucket": self._bucket, "Key": self._key(key)}
        if byte_offset or byte_count is not None:
            end = "" if byte_count is None else str(byte_offset + byte_count - 1)
            kwargs["Range"] = f"bytes={byte_offset}-{end}"
        try:
            resp = self._client.get_object(**kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        return resp["Body"].read()  # type: ignore[no-any-return]

    def read_stream(self, key: str, chunk_size: int = 262144) -> Iterator[bytes]:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        body = resp["Body"]
        while chunk := body.read(chunk_size):
            yield chunk

    def append(self, key: str, content: bytes) -> AppendResult:
        try:
            existing = self.read(key)
        except FileNotFoundError:
            existing = b""
        offset = len(existing)
        self.write(key, existing + content)
        return AppendResult(byte_offset=offset, byte_count=len(content))

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
        return True

    def size(self, key: str) -> int:
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        return resp["ContentLength"]  # type: ignore[no-any-return]

    def list_dir(self, prefix: str) -> list[DirEntry]:
        full_prefix = self._key(prefix).rstrip("/") + "/"
        paginator = self._client.get_paginator("list_objects_v2")
        entries: list[DirEntry] = []
        seen_dirs: set[str] = set()
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"][len(full_prefix):].rstrip("/")
                if name and name not in seen_dirs:
                    seen_dirs.add(name)
                    entries.append(DirEntry(name=name, is_directory=True, size=0, last_modified=""))
            for obj in page.get("Contents", []):
                name = obj["Key"][len(full_prefix):]
                if not name or "/" in name:
                    continue
                entries.append(DirEntry(
                    name=name,
                    is_directory=False,
                    size=obj["Size"],
                    last_modified=_format_dt(obj["LastModified"]),
                ))
        entries.sort(key=lambda e: (not e.is_directory, e.name))
        return entries

    def list_files(self, prefix: str) -> list[str]:
        if full_prefix := self._key(prefix).rstrip("/"):
            full_prefix += "/"
        paginator = self._client.get_paginator("list_objects_v2")
        files: list[str] = []
        strip = len(self._key(""))
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                if rel := obj["Key"][strip:]:
                    files.append(rel)
        return sorted(files)

    def watch(self, callback: Callable[[str], None]) -> None:
        pass

    def stop_watching(self) -> None:
        pass


def _format_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
