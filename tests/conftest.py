from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from app.config import config
from app.db import get_engine, shutdown_engine
from app.main import app
from app.models import User
from app.repositories import sessions as sessions_repo
from app.repositories import users as users_repo

os.environ.setdefault("UNDERFIT_APP_SECRET", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

RegisterUser = Callable[..., Response]
CreateUser = Callable[..., User]
SessionForUser = Callable[[User], dict[str, str]]
OwnerHeaders = dict[str, str]
OutsiderHeaders = dict[str, str]


@pytest.fixture(autouse=True)
def _reset_state(tmp_path: Path) -> Iterator[None]:
    shutdown_engine()
    config.database.path = str(tmp_path / "test.sqlite")
    config.storage.base = str(tmp_path / "storage")
    config.auth_enabled = True
    yield
    shutdown_engine()


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def register_user(client: TestClient) -> RegisterUser:
    def _register(
        email: str = "sam@example.com",
        handle: str = "sam",
        password: str = "",
    ) -> Response:
        return client.post(
            "/api/v1/auth/register",
            json={"email": email, "handle": handle, "password": password or "password123"},
        )

    return _register


@pytest.fixture
def create_user() -> CreateUser:
    def _create_user(email: str, handle: str, name: str = "Test User") -> User:
        with get_engine().begin() as conn:
            return users_repo.create(conn, email, handle, name)

    return _create_user


@pytest.fixture
def session_for_user() -> SessionForUser:
    def _session_for_user(user: User) -> dict[str, str]:
        with get_engine().begin() as conn:
            session = sessions_repo.create(conn, user.id)
        return {"Cookie": f"session_token={session.token}"}

    return _session_for_user


@pytest.fixture
def owner_headers(create_user: CreateUser, session_for_user: SessionForUser) -> OwnerHeaders:
    owner = create_user(email="owner@example.com", handle="owner", name="Owner")
    return session_for_user(owner)


@pytest.fixture
def outsider_headers(create_user: CreateUser, session_for_user: SessionForUser) -> OutsiderHeaders:
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    return session_for_user(outsider)
