from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from underfit_api.dependencies import Conn, CurrentUser
from underfit_api.models import Account
from underfit_api.permissions import require_account_admin
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.routes.resolvers import resolve_account

router = APIRouter(prefix="/accounts")

HANDLE_PATTERN = r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$"


class RenameAccountBody(BaseModel):
    handle: str = Field(pattern=HANDLE_PATTERN)


@router.get("/{handle}/exists")
def account_exists(handle: str, conn: Conn) -> dict[str, bool]:
    return {"exists": accounts_repo.alias_handle_exists(conn, handle)}


@router.get("/{handle}")
def get_account(handle: str, conn: Conn) -> Account:
    return resolve_account(conn, handle)


@router.post("/{handle}/rename")
def rename_account(handle: str, body: RenameAccountBody, conn: Conn, user: CurrentUser) -> Account:
    account = resolve_account(conn, handle)
    require_account_admin(conn, account.id, account.type, user.id)
    new_handle = body.handle.lower()
    if accounts_repo.alias_handle_exists(conn, new_handle):
        raise HTTPException(409, "Handle already exists")
    accounts_repo.rename(conn, account.id, new_handle)
    accounts_repo.create_alias(conn, account.id, new_handle)
    result = accounts_repo.get_by_id(conn, account.id)
    assert result is not None
    return result
