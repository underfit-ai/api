from __future__ import annotations

import json
import logging
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.alias_generators import to_camel

from underfit_api.config import config
from underfit_api.models import Project, Run
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)
_T = TypeVar("_T", bound=BaseModel)


class RunUISidecar(BaseModel):
    model_config = ConfigDict(extra="ignore", alias_generator=to_camel, populate_by_name=True)
    ui_state: dict[str, object] = Field(default_factory=dict)
    is_pinned: bool = False
    is_baseline: bool = False


class ProjectUISidecar(BaseModel):
    model_config = ConfigDict(extra="ignore", alias_generator=to_camel, populate_by_name=True)
    ui_state: dict[str, object] = Field(default_factory=dict)


def sync_project_ui_sidecar(storage: Storage, project: Project) -> None:
    if config.backfill.enabled:
        sidecar = ProjectUISidecar(ui_state=project.ui_state)
        storage_key = f".projects/{project.owner}/{project.name}/ui.json"
        storage.write(storage_key, sidecar.model_dump_json(by_alias=True).encode())


def sync_run_ui_sidecar(storage: Storage, run: Run) -> None:
    if config.backfill.enabled:
        sidecar = RunUISidecar(ui_state=run.ui_state, is_pinned=run.is_pinned, is_baseline=run.is_baseline)
        storage.write(f"{run.storage_key}/ui.json", sidecar.model_dump_json(by_alias=True).encode())


def load_sidecar(storage: Storage, key: str, cls: type[_T]) -> _T:
    if not storage.exists(key):
        return cls()
    try:
        return cls.model_validate_json(storage.read(key))
    except (ValidationError, json.JSONDecodeError, FileNotFoundError):
        logger.warning("Ignoring invalid sidecar at %s", key)
        return cls()
