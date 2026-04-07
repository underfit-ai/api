from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import Connection
from sqlalchemy.exc import IntegrityError


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@contextmanager
def as_conflict(conn: Connection, message: str) -> Iterator[None]:
    try:
        yield
    except IntegrityError:
        conn.rollback()
        raise HTTPException(409, message) from None
