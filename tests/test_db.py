from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config as AlembicConfig

import underfit_api.db as db
from underfit_api.config import PostgresqlDatabaseConfig, SqliteDatabaseConfig, config
from underfit_api.schema import users


def test_ensure_local_cache_schema_recreates_sqlite_cache(tmp_path: Path) -> None:
    config.database = SqliteDatabaseConfig(path=str(tmp_path / "cache.sqlite"))
    db.engine.dispose()
    db.engine = db.build_engine()
    db.ensure_local_cache_schema()
    with db.engine.begin() as conn:
        users.drop(conn)
        conn.exec_driver_sql("PRAGMA user_version = 0")
    db.ensure_local_cache_schema()
    with db.engine.connect() as conn:
        assert "users" in sa.inspect(conn).get_table_names()
        assert conn.exec_driver_sql("PRAGMA user_version").scalar_one() == db.LOCAL_CACHE_SCHEMA_VERSION


def test_ensure_local_cache_schema_requires_sqlite() -> None:
    config.database = PostgresqlDatabaseConfig()
    with pytest.raises(RuntimeError, match="sqlite"):
        db.ensure_local_cache_schema()


def test_alembic_upgrade_creates_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "migrated.sqlite"
    monkeypatch.setattr(config, "database", SqliteDatabaseConfig(path=str(db_path)))
    command.upgrade(AlembicConfig(toml_file="pyproject.toml"), "head")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            tables = set(sa.inspect(conn).get_table_names())
        assert {"accounts", "runs", "alembic_version"} <= tables
    finally:
        engine.dispose()
