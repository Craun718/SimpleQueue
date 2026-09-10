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
        response = await client.get(f"/jobs/{job_id}")
        if response.json()["status"] == "running":
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Job did not start")


async def wait_for_terminal(client: AsyncClient, job_id: str) -> dict:
    for _ in range(150):
        response = await client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] in {"succeeded", "failed", "canceled", "timeout"}:
            return body
        await asyncio.sleep(0.01)
    raise AssertionError("Job did not finish")


async def test_queued_job_can_be_canceled_before_it_starts() -> None:
    async for client in make_client():
        first = (await client.post(
            "/jobs", json={"task_name": "sleep", "params": {"seconds": 0.1}}
        )).json()
        await wait_until_running(client, first["job_id"])
        second = (await client.post(
            "/jobs", json={"task_name": "sleep", "params": {"seconds": 1}}
        )).json()

        canceled = (await client.post(f"/jobs/{second['job_id']}/cancel")).json()

        assert canceled["status"] == "canceled"
        assert canceled["finished_at"] is not None
        assert canceled["events"][-1]["event"] == "canceled"
        assert canceled["events"][-1]["detail"] == {"phase": "queued"}
        assert (await client.get(f"/jobs/{second['job_id']}")).json()["status"] == "canceled"


async def test_running_job_can_be_canceled() -> None:
    async for client in make_client():
        submitted = (await client.post(
            "/jobs", json={"task_name": "sleep", "params": {"seconds": 10}}
        )).json()
        await wait_until_running(client, submitted["job_id"])

        cancel_response = (await client.post(f"/jobs/{submitted['job_id']}/cancel")).json()
        job = await wait_for_terminal(client, submitted["job_id"])

    assert cancel_response["status"] in {"canceling", "canceled"}
    assert job["status"] == "canceled"
