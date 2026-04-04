from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from underfit_api.config import S3StorageConfig
from underfit_api.storage.types import AppendResult, DirEntry, FileStat

_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
_PART_RE = re.compile(r"\.part\d{5}$")


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

    # ── internal ─────────────────────────────────────────────────────────

    def _key(self, key: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{key}"
        return key

    def _part_key(self, key: str, part: int) -> str:
        return self._key(f"{key}.part{part:05d}")

    def _list_parts(self, key: str) -> list[str]:
        prefix = self._key(key) + ".part"
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                suffix = k[len(prefix):]
                if len(suffix) == 5 and suffix.isdigit():
                    keys.append(k)
        return sorted(keys)

    def _delete_parts(self, key: str) -> None:
        parts = self._list_parts(key)
        for i in range(0, len(parts), 1000):
            batch = parts[i : i + 1000]
            self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": [{"Key": k} for k in batch]})

    def _delete_plain(self, key: str) -> None:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError:
            return
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

    def _is_chunked(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._part_key(key, 0))
            return True
        except ClientError:
            return False

    def _head_plain(self, key: str) -> dict[str, Any] | None:
        try:
            return self._client.head_object(Bucket=self._bucket, Key=self._key(key))  # type: ignore[no-any-return]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            raise

    def _chunked_size(self, parts: list[str], n: int) -> int:
        if n == 0:
            return 0
        last_head = self._client.head_object(Bucket=self._bucket, Key=parts[-1])
        last_size: int = last_head["ContentLength"]
        return (n - 1) * _CHUNK_SIZE + last_size

    # ── write (plain objects) ────────────────────────────────────────────

    def write(self, key: str, content: bytes) -> None:
        self._delete_parts(key)
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=content)

    async def write_stream(self, key: str, stream: AsyncIterator[bytes]) -> int:
        self._delete_parts(key)
        buf = bytearray()
        async for chunk in stream:
            if not chunk:
                continue
            buf.extend(chunk)
        await asyncio.to_thread(self._client.put_object, Bucket=self._bucket, Key=self._key(key), Body=bytes(buf))
        return len(buf)

    # ── read (auto-detect format) ────────────────────────────────────────

    def read(self, key: str, byte_offset: int = 0, byte_count: int | None = None) -> bytes:
        parts = self._list_parts(key)
        if parts:
            return self._read_chunked(parts, byte_offset, byte_count)
        return self._read_plain(key, byte_offset, byte_count)

    def _read_plain(self, key: str, byte_offset: int, byte_count: int | None) -> bytes:
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

    def _read_chunked(self, parts: list[str], byte_offset: int, byte_count: int | None) -> bytes:
        n = len(parts)
        if byte_count is None:
            byte_count = self._chunked_size(parts, n) - byte_offset
        if byte_count <= 0:
            return b""
        end_byte = byte_offset + byte_count - 1
        start_chunk = byte_offset // _CHUNK_SIZE
        end_chunk = min(end_byte // _CHUNK_SIZE, n - 1)
        result = bytearray()
        for ci in range(start_chunk, end_chunk + 1):
            chunk_start = ci * _CHUNK_SIZE
            lo = max(byte_offset, chunk_start) - chunk_start
            hi = min(end_byte, chunk_start + _CHUNK_SIZE - 1) - chunk_start
            resp = self._client.get_object(Bucket=self._bucket, Key=parts[ci], Range=f"bytes={lo}-{hi}")
            result.extend(resp["Body"].read())
        return bytes(result)

    def read_stream(self, key: str, chunk_size: int = 262144) -> Iterator[bytes]:
        parts = self._list_parts(key)
        if parts:
            for part_key in parts:
                body = self._client.get_object(Bucket=self._bucket, Key=part_key)["Body"]
                while data := body.read(chunk_size):
                    yield data
            return
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        body = resp["Body"]
        while data := body.read(chunk_size):
            yield data

    # ── append (chunked) ─────────────────────────────────────────────────

    def append(self, key: str, content: bytes) -> AppendResult:
        parts = self._list_parts(key)
        if not parts:
            return self._append_first(key, content)
        return self._append_to_parts(key, parts, content)

    def _append_first(self, key: str, content: bytes) -> AppendResult:
        # Migrate a plain object to chunked format, or create the first chunk.
        existing = b""
        try:
            existing = self._client.get_object(Bucket=self._bucket, Key=self._key(key))["Body"].read()
            self._client.delete_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise
        offset = len(existing)
        combined = existing + content
        pos = 0
        part = 0
        while pos < len(combined):
            self._client.put_object(
                Bucket=self._bucket, Key=self._part_key(key, part), Body=combined[pos : pos + _CHUNK_SIZE],
            )
            pos += _CHUNK_SIZE
            part += 1
        if not combined:
            self._client.put_object(Bucket=self._bucket, Key=self._part_key(key, 0), Body=b"")
        return AppendResult(byte_offset=offset, byte_count=len(content))

    def _append_to_parts(self, key: str, parts: list[str], content: bytes) -> AppendResult:
        n = len(parts)
        last_head = self._client.head_object(Bucket=self._bucket, Key=parts[-1])
        last_size: int = last_head["ContentLength"]
        offset = (n - 1) * _CHUNK_SIZE + last_size

        last_data = self._client.get_object(Bucket=self._bucket, Key=parts[-1])["Body"].read()
        combined = last_data + content
        part_idx = n - 1
        pos = 0
        while pos < len(combined):
            self._client.put_object(
                Bucket=self._bucket, Key=self._part_key(key, part_idx), Body=combined[pos : pos + _CHUNK_SIZE],
            )
            pos += _CHUNK_SIZE
            part_idx += 1
        return AppendResult(byte_offset=offset, byte_count=len(content))

    # ── metadata (auto-detect format) ────────────────────────────────────

    def delete(self, key: str) -> None:
        parts = self._list_parts(key)
        if parts:
            for i in range(0, len(parts), 1000):
                batch = parts[i : i + 1000]
                self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": [{"Key": k} for k in batch]})
            return
        if not self._head_plain(key):
            raise FileNotFoundError(f"File not found: {key}")
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

    def exists(self, key: str) -> bool:
        if self._is_chunked(key):
            return True
        return self._head_plain(key) is not None

    def size(self, key: str) -> int:
        parts = self._list_parts(key)
        if parts:
            return self._chunked_size(parts, len(parts))
        head = self._head_plain(key)
        if not head:
            raise FileNotFoundError(f"File not found: {key}")
        return head["ContentLength"]  # type: ignore[no-any-return]

    def stat(self, key: str) -> FileStat:
        parts = self._list_parts(key)
        if parts:
            resp = self._client.head_object(Bucket=self._bucket, Key=parts[-1])
            last_modified = format_datetime(resp["LastModified"].astimezone(timezone.utc), usegmt=True)
            return FileStat(
                size=self._chunked_size(parts, len(parts)), last_modified=last_modified, etag=resp.get("ETag"),
            )
        head = self._head_plain(key)
        if not head:
            raise FileNotFoundError(f"File not found: {key}")
        last_modified = format_datetime(head["LastModified"].astimezone(timezone.utc), usegmt=True)
        return FileStat(size=head["ContentLength"], last_modified=last_modified, etag=head.get("ETag"))

    # ── listing ──────────────────────────────────────────────────────────

    def list_dir(self, prefix: str) -> list[DirEntry]:
        full_prefix = self._key(prefix).rstrip("/") + "/"
        paginator = self._client.get_paginator("list_objects_v2")
        entries: list[DirEntry] = []
        seen_dirs: set[str] = set()
        logical_files: dict[str, DirEntry] = {}
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"][len(full_prefix) :].rstrip("/")
                if name and name not in seen_dirs:
                    seen_dirs.add(name)
                    entries.append(DirEntry(name=name, is_directory=True, size=0, last_modified=""))
            for obj in page.get("Contents", []):
                raw_name = obj["Key"][len(full_prefix) :]
                if not raw_name or "/" in raw_name:
                    continue
                logical_name = _PART_RE.sub("", raw_name)
                prev = logical_files.get(logical_name)
                size = (prev.size if prev else 0) + obj["Size"]
                last_mod = _format_dt(obj["LastModified"])
                if prev and prev.last_modified > last_mod:
                    last_mod = prev.last_modified
                logical_files[logical_name] = DirEntry(
                    name=logical_name, is_directory=False, size=size, last_modified=last_mod,
                )
        entries.extend(logical_files.values())
        entries.sort(key=lambda e: (not e.is_directory, e.name))
        return entries

    def list_files(self, prefix: str) -> list[str]:
        if full_prefix := self._key(prefix).rstrip("/"):
            full_prefix += "/"
        paginator = self._client.get_paginator("list_objects_v2")
        files: set[str] = set()
        strip = len(self._key(""))
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                if rel := obj["Key"][strip:]:
                    files.add(_PART_RE.sub("", rel))
        return sorted(files)

    def watch(self, callback: Callable[[str], None]) -> None:
        pass

    def stop_watching(self) -> None:
        pass


def _format_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
