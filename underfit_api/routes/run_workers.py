from __future__ import annotations

from fastapi import APIRouter, HTTPException

from underfit_api.dependencies import Conn, CurrentWorker, MaybeUser
from underfit_api.models import OkResponse, Worker
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.routes.resolvers import resolve_run

router = APIRouter()


@router.get("/accounts/{handle}/projects/{project_name}/runs/{run_name}/workers")
def list_workers(handle: str, project_name: str, run_name: str, conn: Conn, user: MaybeUser) -> list[Worker]:
    run = resolve_run(conn, handle, project_name, run_name, user)
    return workers_repo.list_by_run(conn, run.id)


@router.post("/workers/heartbeat")
def heartbeat(conn: Conn, worker_id: CurrentWorker) -> OkResponse:
    if not workers_repo.touch(conn, worker_id):
        raise HTTPException(401, "Unauthorized")
    return OkResponse()
