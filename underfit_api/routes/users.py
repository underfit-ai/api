from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from underfit_api.dependencies import Conn, RequireUser
from underfit_api.models import ExistsResponse, User, UserMembership
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import users as users_repo
from underfit_api.repositories.users import UserSettings

router = APIRouter()


@router.get("/emails/exists")
def email_exists(conn: Conn, email: Annotated[str, Query()] = "") -> ExistsResponse:
    if not email:
        raise HTTPException(400, "Missing email")
    return ExistsResponse(exists=users_repo.get_by_email(conn, email) is not None)


@router.get("/me")
def get_me(user: RequireUser) -> User:
    return user


@router.patch("/me")
def update_me(body: UserSettings, conn: Conn, user: RequireUser) -> User:
    if body.name is not None and not body.name.strip():
        raise HTTPException(400, "Name cannot be empty")
    return users_repo.update_settings(conn, user.id, body)


@router.get("/users/search")
def search_users(conn: Conn, user: RequireUser, query: Annotated[str, Query()] = "") -> list[User]:
    if not query:
        raise HTTPException(400, "Missing query")
    return users_repo.search(conn, query)


@router.get("/users/{handle}/memberships")
def list_user_memberships(handle: str, conn: Conn) -> list[UserMembership]:
    if not (user := users_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "User not found")
    return organization_members_repo.list_user_memberships(conn, user.id)
