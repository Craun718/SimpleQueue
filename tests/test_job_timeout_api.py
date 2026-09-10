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


async def wait_for_timeout(client: AsyncClient, job_id: str) -> dict:
    for _ in range(150):
        response = await client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] == "timeout":
            return body
        await asyncio.sleep(0.02)
    raise AssertionError("Job did not time out")


async def test_job_exceeding_deadline_is_reported_as_timeout() -> None:
    async for client in make_client():
        submitted = (await client.post(
            "/jobs",
            json={
                "task_name": "sleep",
                "params": {"seconds": 1.1},
                "timeout_seconds": 1,
            },
        )).json()

        job = await wait_for_timeout(client, submitted["job_id"])

    assert job["error"]["type"] == "TimeoutError"
    assert "child process termination not guaranteed" in job["error"]["message"]
    assert job["events"][-1]["event"] == "timeout"
