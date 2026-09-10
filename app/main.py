"""FastAPI application factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

JobStatus = Literal[
    "queued", "running", "canceling", "succeeded", "failed", "canceled", "timeout"
]

from app.queue.logging import configure_logging
from app.queue.service import QueueError, TaskQueue


class SubmitJobRequest(BaseModel):
    task_name: str
    params: dict[str, object] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=300, ge=1, le=3600)


def create_app(queue: TaskQueue, *, log_dir: Path | None = None) -> FastAPI:
    """Create the task queue HTTP API."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        configure_logging(log_dir)
        await queue.startup()
        try:
            yield
        finally:
            await queue.shutdown()

    app = FastAPI(title="WMMaker Task Queue", lifespan=lifespan)

    def error_response(status_code: int, code: str, message: str) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content={"error": {"code": code, "message": message}},
        )

    @app.exception_handler(QueueError)
    async def queue_error_handler(_: Request, exc: QueueError) -> JSONResponse:
        return error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(HTTPException)
    async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
        return error_response(exc.status_code, "http_error", str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return error_response(422, "validation_error", str(exc.errors()))

    @app.get("/tasks")
    async def list_tasks() -> list[dict[str, object]]:
        return [
            {
                "name": task.name,
                "description": task.description,
                "params_schema": task.params_model.model_json_schema(),
            }
            for task in queue.tasks
        ]

    @app.post("/jobs", status_code=202)
    async def submit_job(request: SubmitJobRequest) -> dict[str, object]:
        record = await queue.submit(
            task_name=request.task_name,
            params=dict(request.params),
            timeout_seconds=request.timeout_seconds,
        )
        return queue.record_view(record, detailed=True)

    @app.get("/jobs")
    async def list_jobs(
        status: JobStatus | None = None,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, object]]:
        return [
            queue.record_view(record)
            for record in queue.jobs(status=status, offset=offset, limit=limit)
        ]

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict[str, object]:
        return queue.record_view(queue.job(job_id), detailed=True)

    @app.get("/jobs/{job_id}/events")
    async def get_job_events(job_id: str) -> list[dict[str, object]]:
        return queue.event_views(queue.job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str) -> dict[str, object]:
        return queue.record_view(await queue.cancel(job_id), detailed=True)

    return app
