from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field, field_validator

from underfit_api.auth import hash_password
from underfit_api.config import config
from underfit_api.dependencies import Conn, SessionTokenCookie
from underfit_api.models import AuthResponse
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import sessions as sessions_repo
from underfit_api.repositories import user_auth as user_auth_repo
from underfit_api.repositories import users as users_repo
from underfit_api.repositories.sessions import SESSION_TTL_DAYS

router = APIRouter(prefix="/auth")

HASH_DIGEST = "sha256"
HASH_ITERATIONS = 310_000
SESSION_TTL_SECONDS = SESSION_TTL_DAYS * 24 * 60 * 60


def _cookie_secure(request: Request) -> bool:
    if config.secure_cookies is not None:
        return config.secure_cookies
    if request.url.hostname in {"localhost", "127.0.0.1", "::1"}:
        return False
    scheme_secure = request.url.scheme == "https"
    frontend_secure = bool(config.frontend_url and config.frontend_url.startswith("https://"))
    return scheme_secure or frontend_secure


class RegisterBody(BaseModel):
    email: str = Field(pattern=r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
    handle: str = Field(pattern=r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$")
    password: str = Field(min_length=8)

    @field_validator("password")
    @classmethod
    def password_complexity(cls, v: str) -> str:
        if not re.search(r"[A-Za-z]", v) or not re.search(r"[0-9]", v):
            raise ValueError("Password must contain at least one letter and one number")
        return v


class LoginBody(BaseModel):
    email: str = Field(min_length=1)
    password: str = Field(min_length=1)


@router.post("/register")
def register(body: RegisterBody, response: Response, request: Request, conn: Conn) -> AuthResponse:
    handle_lower = body.handle.lower()
    if accounts_repo.exists(conn, handle_lower):
        raise HTTPException(409, "Handle already exists")
    if users_repo.email_exists(conn, body.email):
        raise HTTPException(409, "Email already exists")

    user = users_repo.create(conn, body.email, handle_lower, body.handle)
    pw_hash, pw_salt = hash_password(body.password)
    user_auth_repo.create(conn, user.id, pw_hash, pw_salt, HASH_ITERATIONS, HASH_DIGEST)

    session = sessions_repo.create(conn, user.id)
    response.set_cookie(
        "session_token",
        session.token,
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(request),
        max_age=SESSION_TTL_SECONDS,
        expires=session.expires_at,
    )
    return AuthResponse(user=user, session=session)


@router.post("/login")
def login(body: LoginBody, response: Response, request: Request, conn: Conn) -> AuthResponse:
    if not (user := users_repo.get_by_email(conn, body.email)):
        raise HTTPException(401, "Invalid credentials")
    if not user_auth_repo.verify(conn, user.id, body.password):
        raise HTTPException(401, "Invalid credentials")

    session = sessions_repo.create(conn, user.id)
    response.set_cookie(
        "session_token",
        session.token,
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(request),
        max_age=SESSION_TTL_SECONDS,
        expires=session.expires_at,
    )
    return AuthResponse(user=user, session=session)


@router.post("/logout")
def logout(
    response: Response,
    request: Request,
    conn: Conn,
    session_token: SessionTokenCookie = None,
) -> dict[str, str]:
    if session_token:
        sessions_repo.delete_by_token(conn, session_token)
    response.delete_cookie("session_token", samesite="lax", secure=_cookie_secure(request))
    return {"status": "ok"}
