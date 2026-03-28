from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import config
from app.db import get_engine, shutdown_engine
from app.main import app
from app.models import User
from app.repositories import users as users_repo

os.environ.setdefault("UNDERFIT_APP_SECRET", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

CreateUser = Callable[..., User]


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
def create_user() -> CreateUser:
    def _create_user(email: str, handle: str, name: str = "Test User") -> User:
        with get_engine().begin() as conn:
            return users_repo.create(conn, email, handle, name)

    return _create_user
