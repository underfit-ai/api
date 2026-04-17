from __future__ import annotations

import os
from base64 import urlsafe_b64encode
from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.auth import hash_token
from underfit_api.helpers import utcnow
from underfit_api.models import ApiKey, ApiKeyWithToken
from underfit_api.schema import api_keys


def get_by_token_hash(conn: Connection, token_hash: str) -> ApiKey | None:
    row = conn.execute(api_keys.select().where(api_keys.c.token_hash == token_hash)).first()
    return ApiKey.model_validate(row) if row else None


def list_by_user(conn: Connection, user_id: UUID) -> list[ApiKey]:
    rows = conn.execute(api_keys.select().where(api_keys.c.user_id == user_id)).all()
    return [ApiKey.model_validate(r) for r in rows]


def create(conn: Connection, user_id: UUID, label: str | None) -> ApiKeyWithToken:
    key_id = uuid4()
    token = urlsafe_b64encode(os.urandom(32)).decode()
    prefix = token[:8]
    token_hash = hash_token(token)
    now = utcnow()
    conn.execute(api_keys.insert().values(
        id=key_id, user_id=user_id, label=label, token_prefix=prefix, token_hash=token_hash, created_at=now,
    ))
    return ApiKeyWithToken(id=key_id, user_id=user_id, label=label, token_prefix=prefix, token=token, created_at=now)


def delete(conn: Connection, key_id: UUID, user_id: UUID) -> bool:
    result = conn.execute(api_keys.delete().where(api_keys.c.id == key_id, api_keys.c.user_id == user_id))
    return result.rowcount > 0
