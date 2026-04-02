from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from underfit_api.dependencies import Conn, CurrentUser
from underfit_api.models import User, UserMembership
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo

router = APIRouter()


class UpdateMeBody(BaseModel):
    name: str | None = None
    bio: str | None = None


@router.get("/emails/exists")
def email_exists(conn: Conn, email: Annotated[str, Query()] = "") -> dict[str, bool]:
    if not email:
        raise HTTPException(400, "Missing email")
    return {"exists": users_repo.email_exists(conn, email)}


@router.get("/me")
def get_me(user: CurrentUser) -> User:
    return user


@router.patch("/me")
def update_me(body: UpdateMeBody, conn: Conn, user: CurrentUser) -> User:
    if body.name is not None and not body.name.strip():
        raise HTTPException(400, "Name cannot be empty")
    if not (updated := users_repo.update(conn, user.id, body.name, body.bio)):
        raise HTTPException(404, "User not found")
    return updated


@router.get("/users/search")
def search_users(conn: Conn, user: CurrentUser, query: Annotated[str, Query()] = "") -> list[User]:
    if not query:
        raise HTTPException(400, "Missing query")
    return users_repo.search(conn, query)


@router.get("/users/{handle}/memberships")
def list_user_memberships(handle: str, conn: Conn) -> list[UserMembership]:
    if not (user := users_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "User not found")
    return organizations_repo.list_user_memberships(conn, user.id)
