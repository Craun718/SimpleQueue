import asyncio
from collections.abc import AsyncIterator

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from app.main import create_app
from app.queue.service import JobContext, TaskQueue


class TextParams(BaseModel):
    text: str


async def make_client() -> AsyncIterator[AsyncClient]:
    queue = TaskQueue()

    @queue.task("fail", params_model=TextParams, description="Always fails.")
    async def fail(params: TextParams, job: JobContext) -> dict:
        raise RuntimeError(params.text)

    @queue.task("bad_result", params_model=TextParams, description="Invalid result.")
    async def bad_result(params: TextParams, job: JobContext) -> dict:
        return {"value": {params.text}}  # type: ignore[dict-item]

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        yield client


async def wait_for_failed(client: AsyncClient, job_id: str) -> dict:
    for _ in range(100):
        response = await client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] == "failed":
            return body
        assert body["status"] in {"queued", "running"}
        await asyncio.sleep(0.01)
    raise AssertionError("Job did not fail")


async def test_unknown_task_uses_stable_error_envelope() -> None:
    async for client in make_client():
        response = await client.post(
            "/jobs", json={"task_name": "missing", "params": {}}
        )

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "task_not_found", "message": "Task 'missing' was not found"}
    }


async def test_invalid_task_params_uses_stable_error_envelope() -> None:
    async for client in make_client():
        response = await client.post(
            "/jobs", json={"task_name": "fail", "params": {}}
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "task_params_invalid"


async def test_request_validation_uses_stable_error_envelope() -> None:
    async for client in make_client():
        response = await client.post(
            "/jobs",
            json={"task_name": "fail", "params": {}, "timeout_seconds": 0},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_handler_exception_is_recorded_as_failure() -> None:
    async for client in make_client():
        submitted = (await client.post(
            "/jobs", json={"task_name": "fail", "params": {"text": "boom"}}
        )).json()
        job = await wait_for_failed(client, submitted["job_id"])

    assert job["error"] == {"type": "RuntimeError", "message": "boom"}
    assert job["events"][-1]["event"] == "failed"


async def test_non_serializable_result_is_a_failure() -> None:
    async for client in make_client():
        submitted = (await client.post(
            "/jobs", json={"task_name": "bad_result", "params": {"text": "value"}}
        )).json()
        job = await wait_for_failed(client, submitted["job_id"])

    assert job["error"]["type"] == "TypeError"
