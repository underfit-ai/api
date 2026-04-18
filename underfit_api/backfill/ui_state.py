from __future__ import annotations

import json
import logging
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.alias_generators import to_camel

from underfit_api.models import Project, Run
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

UI_STATE_KEY = ".ui-state.json"


class _Entry(BaseModel):
    model_config = ConfigDict(extra="ignore", alias_generator=to_camel, populate_by_name=True)


class RunEntry(_Entry):
    ui_state: dict[str, object] = Field(default_factory=dict)
    is_pinned: bool = False
    is_baseline: bool = False


class ProjectEntry(_Entry):
    ui_state: dict[str, object] = Field(default_factory=dict)


class UIState(_Entry):
    runs: dict[str, RunEntry] = Field(default_factory=dict)
    projects: dict[str, ProjectEntry] = Field(default_factory=dict)


def load(storage: Storage) -> UIState:
    try:
        return UIState.model_validate_json(storage.read(UI_STATE_KEY))
    except (FileNotFoundError, ValidationError, json.JSONDecodeError):
        return UIState()


def _save(storage: Storage, state: UIState) -> None:
    storage.write(UI_STATE_KEY, state.model_dump_json(by_alias=True).encode())


def write_run(storage: Storage, run: Run) -> None:
    state = load(storage)
    state.runs[str(run.id)] = RunEntry(
        ui_state=run.ui_state, is_pinned=run.is_pinned, is_baseline=run.is_baseline,
    )
    _save(storage, state)


def write_project(storage: Storage, project: Project) -> None:
    state = load(storage)
    state.projects[f"{project.owner}/{project.name}"] = ProjectEntry(ui_state=project.ui_state)
    _save(storage, state)


def lookup_run(state: UIState, run_id: UUID) -> RunEntry:
    return state.runs.get(str(run_id), RunEntry())


def lookup_project(state: UIState, owner: str, name: str) -> ProjectEntry:
    return state.projects.get(f"{owner}/{name}", ProjectEntry())
