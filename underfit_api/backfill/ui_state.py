from __future__ import annotations

import json
import logging
from uuid import UUID

from pydantic import Field, ValidationError

from underfit_api.models import Body, Project, Run
from underfit_api.repositories.runs import RunSettings
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)

UI_STATE_KEY = ".ui-state.json"


class ProjectEntry(Body):
    ui_state: dict[str, object] = Field(default_factory=dict)
    baseline_run_id: UUID | None = None


class UIState(Body):
    runs: dict[str, RunSettings] = Field(default_factory=dict)
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
    state.runs[str(run.id)] = RunSettings(ui_state=run.ui_state, is_pinned=run.is_pinned)
    _save(storage, state)


def write_project(storage: Storage, project: Project) -> None:
    state = load(storage)
    state.projects[f"{project.owner}/{project.name}"] = ProjectEntry(
        ui_state=project.ui_state, baseline_run_id=project.baseline_run_id,
    )
    _save(storage, state)


def lookup_run(state: UIState, run_id: UUID) -> RunSettings:
    return state.runs.get(str(run_id), RunSettings())


def lookup_project(state: UIState, owner: str, name: str) -> ProjectEntry:
    return state.projects.get(f"{owner}/{name}", ProjectEntry())
