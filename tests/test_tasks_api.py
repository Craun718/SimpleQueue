from collections.abc import AsyncIterator

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from asgi_lifespan import LifespanManager
from pydantic import BaseModel
import pytest

from app.main import create_app
from app.queue.service import TaskQueue


class DemoParams(BaseModel):
    text: str


async def make_client() -> AsyncIterator[tuple[AsyncClient, FastAPI, TaskQueue]]:
    queue = TaskQueue()

    @queue.task("echo", params_model=DemoParams, description="Echo input.")
    async def echo(params: DemoParams, job: object) -> dict:
        return {"text": params.text}

    app = create_app(queue)
    async with LifespanManager(app) as manager, AsyncClient(
        transport=ASGITransport(app=manager.app), base_url="http://test"
    ) as client:
        yield client, app, queue


async def test_client_can_discover_registered_task_schema() -> None:
    async for client, _, _ in make_client():
        response = await client.get("/tasks")

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "echo",
            "description": "Echo input.",
            "params_schema": {
                "properties": {"text": {"title": "Text", "type": "string"}},
                "required": ["text"],
                "title": "DemoParams",
                "type": "object",
            },
        }
    ]


def test_registered_handler_must_be_async() -> None:
    queue = TaskQueue()

    with pytest.raises(TypeError, match="must be an async function"):

        @queue.task("echo", params_model=DemoParams)
        def echo(params: DemoParams, job: object) -> dict:
            return {}
