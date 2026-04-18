from __future__ import annotations

from fastapi import APIRouter
from pydantic import Field

from underfit_api.dependencies import Conn, RequireUser
from underfit_api.helpers import as_conflict
from underfit_api.models import Account, Body, ExistsResponse
from underfit_api.permissions import require_account_admin
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.routes.resolvers import resolve_account

router = APIRouter(prefix="/accounts")

HANDLE_PATTERN = r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$"


class RenameAccountBody(Body):
    handle: str = Field(pattern=HANDLE_PATTERN)


@router.get("/{handle}/exists")
def account_exists(handle: str, conn: Conn) -> ExistsResponse:
    return ExistsResponse(exists=accounts_repo.get_by_handle(conn, handle) is not None)


@router.get("/{handle}")
def get_account(handle: str, conn: Conn) -> Account:
    return resolve_account(conn, handle)


@router.post("/{handle}/rename")
def rename_account(handle: str, body: RenameAccountBody, conn: Conn, user: RequireUser) -> Account:
    account = resolve_account(conn, handle)
    require_account_admin(conn, account.id, account.type, user.id)
    with as_conflict(conn, "Handle already exists"):
        return accounts_repo.rename(conn, account.id, body.handle.lower())
