from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import URL

from underfit_api.config import PostgresqlDatabaseConfig, SqliteDatabaseConfig, config
from underfit_api.schema import metadata

LOCAL_CACHE_SCHEMA_VERSION = 2


def database_url() -> str | URL:
    db = config.database
    if isinstance(db, SqliteDatabaseConfig):
        return f"sqlite:///{db.path}" if db.path != ":memory:" else "sqlite://"
    if isinstance(db, PostgresqlDatabaseConfig):
        return URL.create(
            "postgresql+psycopg",
            username=db.user or None,
            password=db.password or None,
            host=db.host,
            port=db.port,
            database=db.database,
        )
    raise ValueError(f"Unsupported database type: {db.type}")


def build_engine() -> Engine:
    if isinstance(config.database, SqliteDatabaseConfig):
        if config.database.path != ":memory:":
            Path(config.database.path).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(database_url(), connect_args={"check_same_thread": False})
        event.listen(engine, "connect", lambda conn, _: conn.execute("PRAGMA foreign_keys=ON"))
        return engine
    return create_engine(database_url())


def ensure_local_cache_schema() -> Engine:
    if not isinstance(config.database, SqliteDatabaseConfig):
        raise RuntimeError("Local backfill mode requires a sqlite database")
    engine = build_engine()
    with engine.connect() as conn:
        if conn.exec_driver_sql("PRAGMA user_version").scalar_one() == LOCAL_CACHE_SCHEMA_VERSION:
            return engine
    engine.dispose()
    if config.database.path != ":memory:":
        Path(config.database.path).unlink(missing_ok=True)
    engine = build_engine()
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"PRAGMA user_version = {LOCAL_CACHE_SCHEMA_VERSION}")
    return engine
