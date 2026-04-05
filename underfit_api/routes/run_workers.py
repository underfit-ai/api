from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from underfit_api.auth import create_worker_token
from underfit_api.dependencies import Conn, CurrentUser, CurrentWorker, MaybeUser
from underfit_api.models import OkResponse, Worker
from underfit_api.permissions import require_project_contributor
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()

class AddWorkerBody(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    worker_label: str


@router.post("/accounts/{handle}/projects/{project_name}/runs/{run_name}/workers")
def add_worker(
    handle: str, project_name: str, run_name: str, body: AddWorkerBody, conn: Conn, user: CurrentUser,
) -> Worker:
    run = resolve_run(conn, handle, project_name, run_name, user)
    require_project_contributor(conn, run.project_id, user.id)
    if workers_repo.get(conn, run.id, body.worker_label):
        raise HTTPException(409, "Worker already exists")
    worker = workers_repo.create(conn, run.id, body.worker_label, is_primary=False)
    token = create_worker_token(worker.id)
    return worker.model_copy(update={"worker_token": token})


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/workers")
def list_workers(handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser) -> list[Worker]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    return workers_repo.list_by_run(conn, run.id)


@router.post("/workers/heartbeat")
def heartbeat(conn: Conn, worker_id: CurrentWorker) -> OkResponse:
    if not workers_repo.touch(conn, worker_id):
        raise HTTPException(401, "Unauthorized")
    return OkResponse()
