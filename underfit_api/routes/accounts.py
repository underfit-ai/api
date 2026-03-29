from __future__ import annotations

from fastapi import APIRouter, HTTPException

from underfit_api.dependencies import Conn
from underfit_api.models import Account
from underfit_api.repositories import accounts as accounts_repo

router = APIRouter(prefix="/accounts")


@router.get("/{handle}/exists")
def account_exists(handle: str, conn: Conn) -> dict[str, bool]:
    return {"exists": accounts_repo.exists(conn, handle)}


@router.get("/{handle}")
def get_account(handle: str, conn: Conn) -> Account:
    if not (account := accounts_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Account not found")
    return account
