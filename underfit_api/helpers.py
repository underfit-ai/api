from __future__ import annotations

import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import Connection
from sqlalchemy.exc import IntegrityError

MAX_PATH_BYTES = 1024
MAX_PATH_SEGMENT_BYTES = 255


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def validate_path(path: str) -> str:
    path = unicodedata.normalize("NFC", path)
    if not path or path.startswith("/"):
        raise HTTPException(400, "Invalid path")
    if any(ch == "\\" or (ch.isspace() and ch != " ") or unicodedata.category(ch).startswith("C") for ch in path):
        raise HTTPException(400, "Invalid path")
    if len(path.encode()) > MAX_PATH_BYTES:
        raise HTTPException(400, "Invalid path: too long")
    for segment in path.split("/"):
        if not segment or segment in (".", "..") or segment != segment.strip(" ") or segment.endswith("."):
            raise HTTPException(400, "Invalid path segment")
        if len(segment.encode()) > MAX_PATH_SEGMENT_BYTES:
            raise HTTPException(400, "Invalid path: segment too long")
    return path


@contextmanager
def as_conflict(conn: Connection, message: str) -> Iterator[None]:
    try:
        yield
    except IntegrityError:
        conn.rollback()
        raise HTTPException(409, message) from None
