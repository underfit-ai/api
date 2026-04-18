from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import Engine

from underfit_api.config import (
    FileStorageConfig,
    MysqlDatabaseConfig,
    PostgresqlDatabaseConfig,
    SqliteDatabaseConfig,
    config,
)
from underfit_api.db import build_engine
from underfit_api.dependencies import AppContext
from underfit_api.main import app
from underfit_api.models import Project, ProjectCollaborator, Run, User, Worker
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import project_collaborators as project_collaborators_repo
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import run_workers as run_workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import sessions as sessions_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import metadata
from underfit_api.storage import Storage, build_storage

os.environ.setdefault("UNDERFIT_APP_SECRET", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

Headers = dict[str, str]
RegisterUser = Callable[..., Response]
CreateUser = Callable[..., User]
SessionForUser = Callable[[User], Headers]
AddCollaborator = Callable[..., ProjectCollaborator]
CreateProject = Callable[..., Project]
CreateRun = Callable[..., Run]
CreateWorker = Callable[..., Worker]
CreateOrg = Callable[..., dict[str, object]]
CreateOrgMember = Callable[..., Headers]

TEST_DATABASE_ENV = "UNDERFIT_TEST_DATABASES"
TEST_DATABASE_NAME = "underfit"
SUPPORTED_DATABASES = ("sqlite", "postgresql", "mysql")


def _selected_db_backends() -> list[str]:
    value = os.environ.get(TEST_DATABASE_ENV, '')
    requested = [backend.strip().lower() for backend in value.split(",") if backend.strip()]
    if not requested:
        return ["sqlite"]
    if "all" in requested:
        return list(SUPPORTED_DATABASES)
    if any(backend not in SUPPORTED_DATABASES for backend in requested):
        supported = ", ".join([*SUPPORTED_DATABASES, "all"])
        raise pytest.UsageError(f"{TEST_DATABASE_ENV} must list {supported}; got {value!r}")
    return list(dict.fromkeys(requested))


SELECTED_DB_BACKENDS = _selected_db_backends()
pytest_plugins = (
    *(["pytest_postgresql.plugin"] if "postgresql" in SELECTED_DB_BACKENDS else []),
    *(["pytest_mysql.plugin"] if "mysql" in SELECTED_DB_BACKENDS else []),
)

if "postgresql" in SELECTED_DB_BACKENDS:
    from pytest_postgresql import factories as postgresql_factories
    _underfit_postgresql_proc = postgresql_factories.postgresql_proc(dbname=TEST_DATABASE_NAME, port=15432)
    _underfit_postgresql = postgresql_factories.postgresql("_underfit_postgresql_proc", dbname=TEST_DATABASE_NAME)

if "mysql" in SELECTED_DB_BACKENDS:
    from pytest_mysql import factories as mysql_factories
    _underfit_mysql_proc = mysql_factories.mysql_proc(port=13306)
    _underfit_mysql = mysql_factories.mysql("_underfit_mysql_proc", dbname=TEST_DATABASE_NAME, passwd="")


@pytest.fixture(params=SELECTED_DB_BACKENDS, ids=SELECTED_DB_BACKENDS)
def db_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _database_config(
    request: pytest.FixtureRequest, db_backend: str, tmp_path: Path,
) -> SqliteDatabaseConfig | PostgresqlDatabaseConfig | MysqlDatabaseConfig:
    if db_backend == "sqlite":
        return SqliteDatabaseConfig(path=str(tmp_path / "test.sqlite"))
    elif db_backend == "postgresql":
        request.getfixturevalue("_underfit_postgresql")
        proc = request.getfixturevalue("_underfit_postgresql_proc")
        return PostgresqlDatabaseConfig(
            host=proc.host, port=proc.port, user=proc.user, password=proc.password, database=TEST_DATABASE_NAME,
        )
    elif db_backend == "mysql":
        request.getfixturevalue("_underfit_mysql")
        proc = request.getfixturevalue("_underfit_mysql_proc")
        return MysqlDatabaseConfig(
            host=proc.host, port=proc.port, user=proc.user, password="", database=TEST_DATABASE_NAME,
        )
    raise Exception(f"Unsupported database type: {db_backend}")


@pytest.fixture(autouse=True)
def _reset_state(request: pytest.FixtureRequest, tmp_path: Path, db_backend: str) -> Iterator[None]:
    snapshot = config.model_copy(deep=True)
    config.database = _database_config(request, db_backend, tmp_path)
    config.storage = FileStorageConfig(base=str(tmp_path / "storage"))
    ctx = AppContext(engine=build_engine(), storage=build_storage())
    app.state.ctx = ctx
    metadata.drop_all(ctx.engine)
    metadata.create_all(ctx.engine)
    yield
    ctx.engine.dispose()
    for field in type(config).model_fields:
        setattr(config, field, getattr(snapshot, field))


@pytest.fixture
def engine() -> Engine:
    return app.state.ctx.engine


@pytest.fixture
def storage() -> Storage:
    return app.state.ctx.storage


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def register_user(client: TestClient) -> RegisterUser:
    def _register(email: str = "sam@example.com", handle: str = "sam", password: str | None = None) -> Response:
        payload = {"email": email, "handle": handle, "password": password or "password123"}
        return client.post("/api/v1/auth/register", json=payload)

    return _register


@pytest.fixture
def create_user(engine: Engine) -> CreateUser:
    def _create_user(email: str, handle: str, name: str = "Test User") -> User:
        with engine.begin() as conn:
            return users_repo.create(conn, email, handle, name)

    return _create_user


@pytest.fixture
def session_for_user(engine: Engine) -> SessionForUser:
    def _session_for_user(user: User) -> dict[str, str]:
        with engine.begin() as conn:
            session = sessions_repo.create(conn, user.id)
        return {"Cookie": f"session_token={session.token}"}

    return _session_for_user


@pytest.fixture
def owner_headers(create_user: CreateUser, session_for_user: SessionForUser) -> Headers:
    return session_for_user(create_user(email="owner@example.com", handle="owner", name="Owner"))


@pytest.fixture
def outsider_headers(create_user: CreateUser, session_for_user: SessionForUser) -> Headers:
    return session_for_user(create_user(email="outsider@example.com", handle="outsider", name="Outsider"))


@pytest.fixture
def create_org(client: TestClient) -> CreateOrg:
    def _create(headers: Headers, handle: str = "core", name: str = "Core") -> dict[str, object]:
        response = client.post("/api/v1/organizations", headers=headers, json={"handle": handle, "name": name})
        assert response.status_code == 201
        return response.json()

    return _create


@pytest.fixture
def create_org_member(
    engine: Engine, create_user: CreateUser, session_for_user: SessionForUser,
) -> CreateOrgMember:
    def _create(org_id: str, email: str, handle: str, name: str, *, role: str = "MEMBER") -> Headers:
        user = create_user(email=email, handle=handle, name=name)
        with engine.begin() as conn:
            organization_members_repo.add_member(conn, UUID(org_id), user.id, role)
        return session_for_user(user)

    return _create


@pytest.fixture
def create_project(engine: Engine) -> CreateProject:
    def _create(handle: str, name: str, description: str = "tracking", visibility: str = "private") -> Project:
        with engine.begin() as conn:
            account = accounts_repo.get_by_handle(conn, handle) or users_repo.create(
                conn, f"{handle}@example.com", handle, handle,
            )
            return projects_repo.create(conn, account.id, name.lower(), description, visibility, {})

    return _create


@pytest.fixture
def add_collaborator(engine: Engine) -> AddCollaborator:
    def _add(handle: str, project_name: str, user_handle: str) -> ProjectCollaborator:
        with engine.begin() as conn:
            assert (account := accounts_repo.get_by_handle(conn, handle)) is not None
            assert (project := projects_repo.get_by_account_and_name(conn, account.id, project_name)) is not None
            assert (user := users_repo.get_by_handle(conn, user_handle)) is not None
            return project_collaborators_repo.add(conn, project.id, user.id)

    return _add


@pytest.fixture
def create_run(engine: Engine, create_project: CreateProject) -> CreateRun:
    def _create(handle: str, project_name: str, name: str = "test-run", launch_id: str = "test-launch-id") -> Run:
        project = create_project(handle=handle, name=project_name)
        with engine.begin() as conn:
            assert (user := users_repo.get_by_handle(conn, handle)) is not None
            run = runs_repo.create(conn, project.id, user.id, launch_id, name, None, {})
            run_workers_repo.create(conn, run.id, worker_label="0")
            return run

    return _create


@pytest.fixture
def run(create_run: CreateRun) -> Run:
    return create_run(handle="owner", project_name="underfit", name="r", launch_id="1")


@pytest.fixture
def create_worker(engine: Engine, run: Run) -> CreateWorker:
    def _create(label: str = "0") -> Worker:
        with engine.begin() as conn:
            return run_workers_repo.get(conn, run.id, label) or run_workers_repo.create(conn, run.id, label)

    return _create


@pytest.fixture
def worker(create_worker: CreateWorker) -> Worker:
    return create_worker()


@pytest.fixture
def worker_headers(worker: Worker) -> Headers:
    return {"Authorization": f"Bearer {worker.id}"}
