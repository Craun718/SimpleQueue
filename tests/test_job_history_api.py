import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from loguru import logger
from pydantic import BaseModel

from app.main import create_app
from app.queue.service import JobContext, TaskQueue


class EchoParams(BaseModel):
    value: int


async def make_client() -> AsyncIterator[AsyncClient]:
    queue = TaskQueue()

    @queue.task("echo", params_model=EchoParams, description="Echo a value.")
    async def echo(params: EchoParams, job: JobContext) -> dict:
        return {"value": params.value}

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        logger.remove()
        yield client


async def test_finished_history_keeps_only_most_recent_1000_jobs() -> None:
    async for client in make_client():
        submissions = []
        for value in range(1002):
            response = await client.post(
                "/jobs", json={"task_name": "echo", "params": {"value": value}}
            )
            assert response.status_code == 202
            submissions.append(response.json()["job_id"])

        queue_drained = False
        for _ in range(1000):
            response = await client.get(f"/jobs/{submissions[-1]}")
            if response.json()["status"] == "succeeded":
                queue_drained = True
                break
            await asyncio.sleep(0.01)

        assert queue_drained

        first = await client.get(f"/jobs/{submissions[0]}")
        second = await client.get(f"/jobs/{submissions[1]}")
        third = await client.get(f"/jobs/{submissions[2]}")

    assert first.status_code == 404, first.json()
    assert second.status_code == 404, second.json()
    assert third.status_code == 200
    assert first.json()["error"]["code"] == "job_not_found"
    assert third.json()["result"] == {"value": 2}
