from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config as AlembicConfig

from underfit_api.config import PostgresqlDatabaseConfig, SqliteDatabaseConfig, config
from underfit_api.db import LOCAL_CACHE_SCHEMA_VERSION, ensure_local_cache_schema
from underfit_api.schema import users


def test_ensure_local_cache_schema_recreates_sqlite_cache(tmp_path: Path) -> None:
    config.database = PostgresqlDatabaseConfig()
    with pytest.raises(RuntimeError, match="sqlite"):
        ensure_local_cache_schema()

    config.database = SqliteDatabaseConfig(path=str(tmp_path / "cache.sqlite"))
    engine = ensure_local_cache_schema()
    with engine.begin() as conn:
        users.drop(conn)
        conn.exec_driver_sql("PRAGMA user_version = 0")
    engine.dispose()
    engine = ensure_local_cache_schema()
    with engine.connect() as conn:
        assert "users" in sa.inspect(conn).get_table_names()
        assert conn.exec_driver_sql("PRAGMA user_version").scalar_one() == LOCAL_CACHE_SCHEMA_VERSION


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
