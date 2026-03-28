from __future__ import annotations

import base64
import hashlib
import hmac
import os
from functools import lru_cache

PBKDF2_ITERATIONS = 310_000
PBKDF2_DIGEST = "sha256"


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


def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS, digest: str = PBKDF2_DIGEST) -> tuple[str, str]:
    salt = os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac(digest, password.encode(), salt.encode(), iterations, dklen=32)
    return dk.hex(), salt


def verify_password(password: str, password_hash: str, password_salt: str, iterations: int, digest: str) -> bool:
    dk = hashlib.pbkdf2_hmac(digest, password.encode(), password_salt.encode(), iterations, dklen=32)
    return hmac.compare_digest(dk.hex(), password_hash)
