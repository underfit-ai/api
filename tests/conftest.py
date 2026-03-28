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

OwnerHeaders = dict[str, str]
OutsiderHeaders = dict[str, str]
RegisterUser = Callable[..., Response]
CreateUser = Callable[..., User]
SessionForUser = Callable[[User], dict[str, str]]
AddCollaborator = Callable[..., Response]
CreateProject = Callable[..., dict[str, object]]
CreateRun = Callable[..., dict[str, object]]
SetupTuple = tuple[OwnerHeaders, str]


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


@pytest.fixture
def create_project(client: TestClient) -> CreateProject:
    def _create(
        headers: dict[str, str],
        handle: str = "owner",
        name: str = "underfit",
        description: str = "tracking",
        visibility: str = "private",
    ) -> dict[str, object]:
        response = client.post(
            f"/api/v1/accounts/{handle}/projects",
            headers=headers,
            json={"name": name, "description": description, "visibility": visibility},
        )
        assert response.status_code == 200
        return response.json()

    return _create


@pytest.fixture
def create_run(create_project: CreateProject, client: TestClient) -> CreateRun:
    def _create(
        headers: dict[str, str],
        handle: str = "owner",
        project_name: str = "underfit",
        status: str = "running",
    ) -> dict[str, object]:
        create_project(headers, handle=handle, name=project_name)
        response = client.post(
            f"/api/v1/accounts/{handle}/projects/{project_name}/runs",
            headers=headers,
            json={"status": status},
        )
        assert response.status_code == 200
        return response.json()

    return _create


@pytest.fixture
def add_collaborator(client: TestClient) -> AddCollaborator:
    def _add(
        headers: dict[str, str],
        account: str = "owner",
        project: str = "underfit",
        user_handle: str = "outsider",
        expected_status: int = 200,
    ) -> Response:
        response = client.put(
            f"/api/v1/accounts/{account}/projects/{project}/collaborators/{user_handle}",
            headers=headers,
        )
        assert response.status_code == expected_status
        return response

    return _add


def _run_endpoint(owner_headers: OwnerHeaders, create_run: CreateRun, suffix: str) -> SetupTuple:
    run_name = create_run(owner_headers)["name"]
    base = "/api/v1/accounts/owner/projects/underfit/runs"
    return owner_headers, f"{base}/{run_name}/{suffix}"


@pytest.fixture
def logs_setup(owner_headers: OwnerHeaders, create_run: CreateRun) -> SetupTuple:
    return _run_endpoint(owner_headers, create_run, "logs")


@pytest.fixture
def scalars_setup(owner_headers: OwnerHeaders, create_run: CreateRun) -> SetupTuple:
    return _run_endpoint(owner_headers, create_run, "scalars")
