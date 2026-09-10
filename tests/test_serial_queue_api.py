import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from app.main import create_app
from app.queue.service import JobContext, TaskQueue


class TraceParams(BaseModel):
    name: str


async def make_client() -> AsyncIterator[tuple[AsyncClient, list[str]]]:
    order: list[str] = []
    queue = TaskQueue()

    @queue.task("trace", params_model=TraceParams, description="Trace execution.")
    async def trace(params: TraceParams, job: JobContext) -> dict[str, str]:
        order.append(params.name)
        await asyncio.sleep(0.03)
        return {"name": params.name}

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        yield client, order


async def wait_for_terminal(client: AsyncClient, job_id: str) -> dict:
    for _ in range(100):
        response = await client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] in {"succeeded", "failed", "canceled", "timeout"}:
            return body
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not finish")


async def test_jobs_run_one_at_a_time_in_submission_order() -> None:
    async for client, order in make_client():
        first = (await client.post(
            "/jobs", json={"task_name": "trace", "params": {"name": "first"}}
        )).json()
        second = (await client.post(
            "/jobs", json={"task_name": "trace", "params": {"name": "second"}}
        )).json()

        first_job = await wait_for_terminal(client, first["job_id"])
        second_job = await wait_for_terminal(client, second["job_id"])

    assert first_job["status"] == "succeeded"
    assert second_job["status"] == "succeeded"
    assert [item["name"] for item in (first_job["result"], second_job["result"])] == [
        "first",
        "second",
    ]
    assert order == ["first", "second"]
