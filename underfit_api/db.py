from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.engine import URL

from underfit_api.config import MysqlDatabaseConfig, PostgresqlDatabaseConfig, SqliteDatabaseConfig, config


def build_engine() -> Engine:
    db = config.database
    if isinstance(db, SqliteDatabaseConfig):
        if db.path != ":memory:":
            Path(db.path).parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{db.path}" if db.path != ":memory:" else "sqlite://"
        return create_engine(url, connect_args={"check_same_thread": False})
    if isinstance(db, PostgresqlDatabaseConfig):
        url = URL.create(
            "postgresql+psycopg",
            username=db.user or None,
            password=db.password or None,
            host=db.host,
            port=db.port,
            database=db.database,
        )
        return create_engine(url)
    if isinstance(db, MysqlDatabaseConfig):
        url = URL.create(
            "mysql+pymysql",
            username=db.user or None,
            password=db.password or None,
            host=db.host,
            port=db.port,
            database=db.database,
        )
        return create_engine(url)
    raise ValueError(f"Unsupported database type: {db.type}")


engine = build_engine()


def get_conn() -> Iterator[Connection]:
    with engine.begin() as conn:
        yield conn
