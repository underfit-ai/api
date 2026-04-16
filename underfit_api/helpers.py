from __future__ import annotations

import json
import smtplib
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any

from fastapi import HTTPException
from sqlalchemy import Connection
from sqlalchemy.exc import IntegrityError

from underfit_api.auth import create_signed_token
from underfit_api.config import EmailConfig, config

MAX_PATH_BYTES = 1024
MAX_PATH_SEGMENT_BYTES = 255
MAX_JSON_BYTES = 65536


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def send_email(cfg: EmailConfig, to: str, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = cfg.from_address
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
        if cfg.starttls:
            server.starttls()
        if cfg.smtp_user:
            server.login(cfg.smtp_user, cfg.smtp_password)
        server.send_message(msg)


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


def validate_json_size(value: dict[str, object] | None, label: str) -> None:
    if value is not None and len(json.dumps(value)) > MAX_JSON_BYTES:
        raise HTTPException(400, f"{label} too large")


def ensure_email_configured() -> EmailConfig:
    if not config.email:
        raise HTTPException(400, "Email is not configured")
    if not config.frontend_url:
        raise HTTPException(400, "Frontend URL is not configured")
    return config.email


def signed_link_url(payload: dict[str, Any], ttl: timedelta, path: str) -> str:
    assert config.frontend_url is not None
    token = create_signed_token(payload, ttl)
    return f"{config.frontend_url.rstrip('/')}{path}?token={token}"
