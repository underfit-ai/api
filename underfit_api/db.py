from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.engine import URL

from underfit_api.config import MysqlDatabaseConfig, PostgresqlDatabaseConfig, SqliteDatabaseConfig, config
from underfit_api.schema import metadata

engine: Engine | None = None


def get_engine() -> Engine:
    global engine  # noqa: PLW0603
    if engine is None:
        db = config.database
        if isinstance(db, SqliteDatabaseConfig):
            if db.path != ":memory:":
                Path(db.path).parent.mkdir(parents=True, exist_ok=True)
            url = f"sqlite:///{db.path}" if db.path != ":memory:" else "sqlite://"
            engine = create_engine(url, connect_args={"check_same_thread": False})
        elif isinstance(db, PostgresqlDatabaseConfig):
            url = URL.create(
                "postgresql",
                username=db.user or None,
                password=db.password or None,
                host=db.host,
                port=db.port,
                database=db.database,
            )
            engine = create_engine(url)
        elif isinstance(db, MysqlDatabaseConfig):
            url = URL.create(
                "mysql",
                username=db.user or None,
                password=db.password or None,
                host=db.host,
                port=db.port,
                database=db.database,
            )
            engine = create_engine(url)
        else:
            raise ValueError(f"Unsupported database type: {db.type}")
        metadata.create_all(engine)
    return engine


def get_conn() -> Iterator[Connection]:
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


def shutdown_engine() -> None:
    global engine  # noqa: PLW0603
    if engine is not None:
        engine.dispose()
        engine = None
