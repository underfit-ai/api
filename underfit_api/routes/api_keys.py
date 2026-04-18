from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException

from underfit_api.dependencies import Conn, RequireUser
from underfit_api.models import ApiKey, ApiKeyWithToken, Body, OkResponse
from underfit_api.repositories import api_keys as api_keys_repo

router = APIRouter(prefix="/me/api-keys")


class CreateApiKeyBody(Body):
    label: str = ""


@router.get("")
def list_api_keys(conn: Conn, user: RequireUser) -> list[ApiKey]:
    return api_keys_repo.list_by_user(conn, user.id)


@router.post("")
def create_api_key(body: CreateApiKeyBody, conn: Conn, user: RequireUser) -> ApiKeyWithToken:
    return api_keys_repo.create(conn, user.id, body.label)


@router.delete("/{key_id}")
def delete_api_key(key_id: str, conn: Conn, user: RequireUser) -> OkResponse:
    try:
        parsed_id = UUID(key_id)
    except ValueError:
        raise HTTPException(404, "API key not found") from None
    if not api_keys_repo.delete(conn, parsed_id, user.id):
        raise HTTPException(404, "API key not found")
    return OkResponse()
