from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, NamedTuple

from underfit_api.config import config

PBKDF2_ITERATIONS = 310_000
PBKDF2_DIGEST = "sha256"


class PasswordHash(NamedTuple):
    hash: str
    salt: str
    iterations: int
    digest: str


def _missing_app_secret_message() -> str:
    candidate = base64.urlsafe_b64encode(os.urandom(32)).decode()
    return (
        "UNDERFIT_APP_SECRET is required when auth is enabled. Set it as an environment variable, for example:\n"
        f"export UNDERFIT_APP_SECRET={candidate}"
    )


@lru_cache(maxsize=1)
def get_app_secret() -> bytes:
    raw = os.getenv("UNDERFIT_APP_SECRET")
    if not raw:
        raise RuntimeError(_missing_app_secret_message())
    decoded = base64.urlsafe_b64decode(raw.encode())
    if len(decoded) < 32:
        raise RuntimeError("UNDERFIT_APP_SECRET must be at least 32 bytes of entropy")
    return decoded


def hash_token(token: str) -> str:
    secret = get_app_secret()
    return hmac.new(secret, token.encode(), hashlib.sha256).hexdigest()


def hash_password(password: str) -> PasswordHash:
    salt = os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac(PBKDF2_DIGEST, password.encode(), salt.encode(), PBKDF2_ITERATIONS, dklen=32)
    return PasswordHash(dk.hex(), salt, PBKDF2_ITERATIONS, PBKDF2_DIGEST)


def verify_password(password: str, stored: PasswordHash) -> bool:
    dk = hashlib.pbkdf2_hmac(stored.digest, password.encode(), stored.salt.encode(), stored.iterations, dklen=32)
    return hmac.compare_digest(dk.hex(), stored.hash)


def create_signed_token(payload: dict[str, Any], expires_in: timedelta, kind: str) -> str:
    data_payload = {**payload, "kind": kind, "exp": (datetime.now(timezone.utc) + expires_in).isoformat()}
    data = base64.urlsafe_b64encode(json.dumps(data_payload).encode()).decode()
    sig = hmac.new(get_app_secret(), data.encode(), hashlib.sha256).hexdigest()
    return f"{data}.{sig}"


def create_worker_token(worker_id: object) -> str:
    if not config.auth_enabled:
        return str(worker_id)
    return create_signed_token({"worker_id": str(worker_id)}, timedelta(days=3650), "worker")


def verify_signed_token(token: str, kind: str) -> dict[str, Any] | None:
    if "." not in token:
        return None
    data, sig = token.split(".", 1)
    expected_sig = hmac.new(get_app_secret(), data.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(data))
        exp = datetime.fromisoformat(payload["exp"])
    except (json.JSONDecodeError, ValueError, KeyError, TypeError):
        return None
    if payload.get("kind") != kind or datetime.now(timezone.utc) > exp:
        return None
    return payload
