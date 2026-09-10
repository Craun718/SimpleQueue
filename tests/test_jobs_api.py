import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from app.main import create_app
from app.queue.service import JobContext, TaskQueue


class EchoParams(BaseModel):
    text: str


class TraceParams(BaseModel):
    name: str


async def make_client() -> AsyncIterator[tuple[AsyncClient, FastAPI, TaskQueue]]:
    queue = TaskQueue()

    @queue.task("echo", params_model=EchoParams, description="Echo input.")
    async def echo(params: EchoParams, job: JobContext) -> dict:
        return {"text": params.text}

    @queue.task("trace", params_model=TraceParams, description="Trace execution.")
    async def trace(params: TraceParams, job: JobContext) -> dict:
        if params.name == "second":
            await asyncio.sleep(0.05)
        return {"name": params.name}

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        yield client, app, queue


async def wait_for_status(
    client: AsyncClient,
    job_id: str,
    *statuses: str,
) -> dict:
    for _ in range(100):
        response = await client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] in statuses:
            return body
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not reach {statuses}")


async def test_client_can_submit_job_and_read_successful_result() -> None:
    async for client, _, _ in make_client():
        response = await client.post(
            "/jobs",
            json={"task_name": "echo", "params": {"text": "hello"}},
        )

        assert response.status_code == 202
        submitted = response.json()
        assert submitted["task_name"] == "echo"
        assert submitted["status"] in {"queued", "running"}

        completed = await wait_for_status(client, submitted["job_id"], "succeeded")

    assert completed["result"] == {"text": "hello"}
    assert completed["error"] is None
    assert completed["started_at"] is not None
    assert completed["finished_at"] is not None


async def test_jobs_can_be_filtered_and_paginated() -> None:
    async for client, _, _ in make_client():
        succeeded = (await client.post(
            "/jobs", json={"task_name": "echo", "params": {"text": "done"}}
        )).json()
        await wait_for_status(client, succeeded["job_id"], "succeeded")

        running = (await client.post(
            "/jobs", json={"task_name": "trace", "params": {"name": "second"}}
        )).json()
        await wait_for_status(client, running["job_id"], "running")
        queued = (await client.post(
            "/jobs", json={"task_name": "trace", "params": {"name": "third"}}
        )).json()

        all_jobs = (await client.get("/jobs")).json()
        second_page = (await client.get("/jobs?offset=1&limit=1")).json()
        succeeded_jobs = (await client.get("/jobs?status=succeeded")).json()
        queued_jobs = (await client.get("/jobs?status=queued&offset=0&limit=1")).json()
        running_jobs = (await client.get("/jobs?status=running&offset=1&limit=1")).json()
        invalid_status = await client.get("/jobs?status=unknown")

        await wait_for_status(client, running["job_id"], "succeeded")
        await wait_for_status(client, queued["job_id"], "succeeded")

    assert [job["job_id"] for job in all_jobs] == [
        queued["job_id"],
        running["job_id"],
        succeeded["job_id"],
    ]
    assert [job["job_id"] for job in second_page] == [running["job_id"]]
    assert [job["job_id"] for job in succeeded_jobs] == [succeeded["job_id"]]
    assert [job["job_id"] for job in queued_jobs] == [queued["job_id"]]
    assert running_jobs == []
    assert invalid_status.status_code == 422
    assert invalid_status.json()["error"]["code"] == "validation_error"
