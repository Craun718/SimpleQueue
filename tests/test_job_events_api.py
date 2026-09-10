import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from app.main import create_app
from app.queue.service import JobContext, TaskQueue


class SleepParams(BaseModel):
    seconds: float


async def make_client() -> AsyncIterator[AsyncClient]:
    queue = TaskQueue()

    @queue.task("sleep", params_model=SleepParams, description="Sleep.")
    async def sleep(params: SleepParams, job: JobContext) -> dict:
        await asyncio.sleep(params.seconds)
        return {"slept": params.seconds}

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        yield client


async def wait_until_running(client: AsyncClient, job_id: str) -> None:
    for _ in range(100):
        body = (await client.get(f"/jobs/{job_id}")).json()
        if body["status"] == "running":
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Job did not start")


async def test_job_events_can_be_read_separately() -> None:
    async for client in make_client():
        submitted = (await client.post(
            "/jobs", json={"task_name": "sleep", "params": {"seconds": 0.03}}
        )).json()
        await wait_until_running(client, submitted["job_id"])
        for _ in range(100):
            job = (await client.get(f"/jobs/{submitted['job_id']}")).json()
            if job["status"] == "succeeded":
                break
            await asyncio.sleep(0.01)

        response = await client.get(f"/jobs/{submitted['job_id']}/events")

    assert job["status"] == "succeeded"
    assert response.status_code == 200
    assert [event["event"] for event in response.json()] == [
        "submitted",
        "started",
        "succeeded",
    ]
    assert all(event["at"].endswith("+00:00") for event in response.json())


async def test_events_for_missing_job_uses_error_envelope() -> None:
    async for client in make_client():
        response = await client.get("/jobs/missing/events")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "job_not_found"
