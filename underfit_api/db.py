from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine

from underfit_api.config import config
from underfit_api.schema import metadata

engine: Engine | None = None


def get_engine() -> Engine:
    global engine  # noqa: PLW0603
    if engine is None:
        if config.database.type == "sqlite":
            if config.database.path != ":memory:":
                Path(config.database.path).parent.mkdir(parents=True, exist_ok=True)
            url = f"sqlite:///{config.database.path}" if config.database.path != ":memory:" else "sqlite://"
            engine = create_engine(url, connect_args={"check_same_thread": False})
        else:
            raise ValueError(f"Unsupported database type: {config.database.type}")
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
