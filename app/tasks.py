"""Application task registrations."""

import asyncio

from pydantic import BaseModel, Field

from app.queue.service import JobContext, TaskQueue


class SleepParams(BaseModel):
    seconds: float = Field(gt=0, le=3600)


def register_tasks(queue: TaskQueue) -> None:
    """Register the tasks bundled with the application."""

    @queue.task("sleep", params_model=SleepParams, description="Sleep for a bounded time.")
    async def sleep(params: SleepParams, job: JobContext) -> dict[str, float]:
        await asyncio.sleep(params.seconds)
        return {"slept_seconds": params.seconds}
