from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]


class DatabaseConfig(BaseModel):
    type: Literal["sqlite"] = "sqlite"
    path: str = ".underfit/db.sqlite"


class FileStorageConfig(BaseModel):
    type: Literal["file"] = "file"
    base: str = ".underfit/storage"


class S3StorageConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str = ""
    prefix: str = ""
    region: str = ""
    endpoint_url: str = ""


StorageConfig = Annotated[Union[FileStorageConfig, S3StorageConfig], Field(discriminator="type")]


class BackfillConfig(BaseModel):
    enabled: bool = False
    scan_interval_ms: int = 15_000
    debounce_ms: int = 500
    realtime: bool = True


class BufferConfig(BaseModel):
    max_segment_bytes: int = 256 * 1024
    max_segment_age_ms: int = 30_000
    flush_interval_ms: int = 1000


class Config(BaseModel):
    auth_enabled: bool = True
    static_dir: str = 'static'
    frontend_url: str | None = None
    database: DatabaseConfig = DatabaseConfig()
    storage: StorageConfig = FileStorageConfig()
    backfill: BackfillConfig = BackfillConfig()
    buffer: BufferConfig = BufferConfig()


def load_config(path: Path | None = None) -> Config:
    if path is None:
        path = Path(os.environ.get("UNDERFIT_CONFIG", "underfit.toml"))
    if not path.exists():
        return Config()
    with path.open("rb") as f:
        data = tomllib.load(f)
    return Config(**data)


config = load_config()
