from __future__ import annotations

import asyncio
import tempfile
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from email.utils import format_datetime

import boto3
from botocore.exceptions import ClientError

from underfit_api.config import S3StorageConfig
from underfit_api.storage.types import AppendResult, DirEntry, FileStat


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
        total = 0
        with tempfile.TemporaryFile() as tmp:
            async for chunk in stream:
                if not chunk:
                    continue
                tmp.write(chunk)
                total += len(chunk)
            tmp.seek(0)
            await asyncio.to_thread(self._client.upload_fileobj, tmp, self._bucket, self._key(key))
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
        min_part = 5 * 1024 * 1024
        new_size = len(content)
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=self._key(key))
            existing_size = head["ContentLength"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.write(key, content)
                return AppendResult(byte_offset=0, byte_count=new_size)
            raise

        offset = existing_size
        # Small file + small append: simple re-upload to avoid multipart edge cases.
        if existing_size + new_size < min_part:
            combined = self.read(key) + content
            self.write(key, combined)
            return AppendResult(byte_offset=offset, byte_count=new_size)

        # Existing object too small to serve as a multipart part: re-upload combined payload.
        if existing_size < min_part:
            combined = self.read(key) + content
            self.write(key, combined)
            return AppendResult(byte_offset=offset, byte_count=new_size)

        upload_id = None
        parts: list[dict[str, object]] = []
        part_number = 1
        try:
            upload_id = self._client.create_multipart_upload(
                Bucket=self._bucket, Key=self._key(key),
            )["UploadId"]
            # Copy existing object as first part (size >= min_part here)
            copy_resp = self._client.upload_part_copy(
                Bucket=self._bucket,
                Key=self._key(key),
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource={"Bucket": self._bucket, "Key": self._key(key)},
                CopySourceRange=f"bytes=0-{existing_size - 1}",
            )
            parts.append({"ETag": copy_resp["ETag"], "PartNumber": part_number})
            part_number += 1

            # Upload new content as subsequent parts
            idx = 0
            while idx < new_size:
                chunk = content[idx: idx + min_part]
                resp = self._client.upload_part(
                    Bucket=self._bucket,
                    Key=self._key(key),
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                part_number += 1
                idx += len(chunk)

            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key(key),
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return AppendResult(byte_offset=offset, byte_count=new_size)
        except Exception:
            if upload_id is not None:
                self._client.abort_multipart_upload(
                    Bucket=self._bucket, Key=self._key(key), UploadId=upload_id,
                )
            raise

    def delete(self, key: str) -> None:
        if not self.exists(key):
            raise FileNotFoundError(f"File not found: {key}")
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

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

    def stat(self, key: str) -> FileStat:
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"File not found: {key}") from e
            raise
        last_modified = format_datetime(resp["LastModified"].astimezone(timezone.utc), usegmt=True)
        return FileStat(
            size=resp["ContentLength"],
            last_modified=last_modified,
            etag=resp.get("ETag"),
        )

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
