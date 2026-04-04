from __future__ import annotations

import re
from datetime import timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import AfterValidator, BaseModel, Field

from underfit_api.auth import PBKDF2_DIGEST, PBKDF2_ITERATIONS, create_signed_token, hash_password, verify_signed_token
from underfit_api.config import config
from underfit_api.dependencies import Conn, SessionTokenCookie
from underfit_api.email import send_email
from underfit_api.models import AuthResponse, OkResponse, Session
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import sessions as sessions_repo
from underfit_api.repositories import user_auth as user_auth_repo
from underfit_api.repositories import users as users_repo
from underfit_api.repositories.sessions import SESSION_TTL_DAYS

router = APIRouter(prefix="/auth")

SESSION_TTL_SECONDS = SESSION_TTL_DAYS * 24 * 60 * 60
RESET_TOKEN_TTL = timedelta(minutes=30)


def _validate_password(v: str) -> str:
    if not re.search(r"[A-Za-z]", v) or not re.search(r"[0-9]", v):
        raise ValueError("Password must contain at least one letter and one number")
    return v


def _cookie_secure(request: Request) -> bool:
    if config.secure_cookies is not None:
        return config.secure_cookies
    if request.url.hostname in {"localhost", "127.0.0.1", "::1"}:
        return False
    scheme_secure = request.url.scheme == "https"
    frontend_secure = bool(config.frontend_url and config.frontend_url.startswith("https://"))
    return scheme_secure or frontend_secure


def _set_session_cookie(response: Response, request: Request, session: Session) -> None:
    response.set_cookie(
        "session_token",
        session.token,
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(request),
        max_age=SESSION_TTL_SECONDS,
        expires=session.expires_at.replace(tzinfo=timezone.utc),
    )


Password = Annotated[str, Field(min_length=8), AfterValidator(_validate_password)]


class RegisterBody(BaseModel):
    email: str = Field(pattern=r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
    handle: str = Field(pattern=r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$")
    password: Password


class LoginBody(BaseModel):
    email: str = Field(min_length=1)
    password: str = Field(min_length=1)


class ForgotPasswordBody(BaseModel):
    email: str = Field(min_length=1)


class ResetPasswordBody(BaseModel):
    token: str = Field(min_length=1)
    password: Password


@router.post("/register")
def register(body: RegisterBody, response: Response, request: Request, conn: Conn) -> AuthResponse:
    handle_lower = body.handle.lower()
    if accounts_repo.alias_handle_exists(conn, handle_lower):
        raise HTTPException(409, "Handle already exists")
    if users_repo.email_exists(conn, body.email):
        raise HTTPException(409, "Email already exists")

    user = users_repo.create(conn, body.email, handle_lower, body.handle)
    accounts_repo.create_alias(conn, user.id, handle_lower)
    pw_hash, pw_salt = hash_password(body.password)
    user_auth_repo.create(conn, user.id, pw_hash, pw_salt, PBKDF2_ITERATIONS, PBKDF2_DIGEST)

    session = sessions_repo.create(conn, user.id)
    _set_session_cookie(response, request, session)
    return AuthResponse(user=user, session=session)


@router.post("/login")
def login(body: LoginBody, response: Response, request: Request, conn: Conn) -> AuthResponse:
    if not (user := users_repo.get_by_email(conn, body.email)):
        raise HTTPException(401, "Invalid credentials")
    if not user_auth_repo.verify(conn, user.id, body.password):
        raise HTTPException(401, "Invalid credentials")

    session = sessions_repo.create(conn, user.id)
    _set_session_cookie(response, request, session)
    return AuthResponse(user=user, session=session)


@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordBody, conn: Conn) -> OkResponse:
    if not config.email:
        raise HTTPException(400, "Email is not configured")
    elif not config.frontend_url:
        raise HTTPException(400, "Frontend URL is not configured")
    elif user := users_repo.get_by_email(conn, body.email):
        pw_prefix = user_auth_repo.get_password_hash_prefix(conn, user.id)
        token = create_signed_token({"user_id": str(user.id), "pw": pw_prefix}, RESET_TOKEN_TTL)
        base_url = config.frontend_url.rstrip("/")
        reset_url = f"{base_url}/reset-password?token={token}"
        send_email(
            config.email,
            to=user.email,
            subject="Reset your password",
            body=f"Click the link below to reset your password:\n\n{reset_url}\n\nThis link expires in 30 minutes.",
        )
    return OkResponse()


@router.post("/reset-password")
def reset_password(body: ResetPasswordBody, conn: Conn) -> OkResponse:
    payload = verify_signed_token(body.token)
    if not payload:
        raise HTTPException(400, "Invalid or expired reset token")
    user_id = UUID(payload["user_id"])
    if user_auth_repo.get_password_hash_prefix(conn, user_id) != payload["pw"]:
        raise HTTPException(400, "Invalid or expired reset token")
    pw_hash, pw_salt = hash_password(body.password)
    user_auth_repo.update_password(conn, user_id, pw_hash, pw_salt, PBKDF2_ITERATIONS, PBKDF2_DIGEST)
    return OkResponse()


@router.post("/logout")
def logout(
    response: Response,
    request: Request,
    conn: Conn,
    session_token: SessionTokenCookie = None,
) -> OkResponse:
    if session_token:
        sessions_repo.delete_by_token(conn, session_token)
    response.delete_cookie("session_token", samesite="lax", secure=_cookie_secure(request))
    return OkResponse()
