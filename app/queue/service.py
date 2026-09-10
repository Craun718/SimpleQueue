"""Public in-process task queue service."""

import asyncio
import inspect
import itertools
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel, ValidationError
from taskiq import InMemoryBroker


JobHandler = Callable[[BaseModel, "JobContext"], Awaitable[dict[str, Any]]]

QUEUED = "queued"
RUNNING = "running"
CANCELING = "canceling"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELED = "canceled"
TIMEOUT = "timeout"
TERMINAL_STATUSES = frozenset({SUCCEEDED, FAILED, CANCELED, TIMEOUT})
MAX_EVENTS_PER_JOB = 100
MAX_FINISHED_JOBS = 1000


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    """A queue task selected by name and validated before execution."""

    name: str
    description: str
    params_model: type[BaseModel]
    handler: JobHandler


class JobContext:
    """Values supplied to a task while the queue executes it."""

    def __init__(
        self,
        *,
        job_id: str,
        task_name: str,
        submitted_at: datetime,
        timeout_seconds: float,
        cancel_event: asyncio.Event,
    ) -> None:
        self.job_id = job_id
        self.task_name = task_name
        self.submitted_at = submitted_at
        self.timeout_seconds = timeout_seconds
        self._cancel_event = cancel_event

    @property
    def canceled(self) -> bool:
        """Whether the client asked the queue to cancel this job."""

        return self._cancel_event.is_set()


@dataclass(frozen=True, slots=True)
class JobEvent:
    """A state transition observed by the queue."""

    event: str
    at: datetime
    detail: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "at": self.at.isoformat(),
            "detail": self.detail,
        }


@dataclass(slots=True)
class JobRecord:
    """The queue's authoritative in-memory view of one job."""

    job_id: str = field(default_factory=lambda: uuid4().hex)
    task_name: str = ""
    status: str = QUEUED
    submitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    timeout_seconds: float = 300.0
    taskiq_id: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    events: list[JobEvent] = field(default_factory=list)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


class QueueError(Exception):
    """Public API error with a stable code."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class TaskQueue:
    """Registry and in-memory executor for application tasks."""

    def __init__(self) -> None:
        self.broker = InMemoryBroker(max_async_tasks=30)
        self._tasks: dict[str, TaskDefinition] = {}
        self._jobs: OrderedDict[str, JobRecord] = OrderedDict()
        self._finished_ids: OrderedDict[str, None] = OrderedDict()
        self._semaphore = asyncio.Semaphore(1)
        async def execute_job(payload: dict[str, Any]) -> None:
            await self._execute_job(payload)

        self._runner = self.broker.register_task(
            execute_job,
            "app.queue.execute_job",
        )

    def task(
        self,
        name: str,
        *,
        params_model: type[BaseModel],
        description: str = "",
    ) -> Callable[[JobHandler], JobHandler]:
        """Register a coroutine as a named queue task."""

        def decorator(handler: JobHandler) -> JobHandler:
            if name in self._tasks:
                raise ValueError(f"Task {name!r} is already registered")
            if not inspect.iscoroutinefunction(handler):
                raise TypeError(f"Task {name!r} handler must be an async function")
            if handler.__code__.co_argcount != 2:
                raise TypeError(f"Task {name!r} must accept params and job")
            self._tasks[name] = TaskDefinition(
                name=name,
                description=description,
                params_model=params_model,
                handler=handler,
            )
            return handler

        return decorator

    @property
    def tasks(self) -> tuple[TaskDefinition, ...]:
        """Registered tasks in registration order."""

        return tuple(self._tasks.values())

    async def startup(self) -> None:
        """Start the in-memory Taskiq broker."""

        await self.broker.startup()

    async def shutdown(self) -> None:
        """Cancel active jobs, then stop the in-memory broker."""

        active_tasks: list[asyncio.Task[None]] = []
        for record in list(self._jobs.values()):
            if record.status == QUEUED:
                record.cancel_event.set()
                self._finish(record, status=CANCELED, detail={"phase": "shutdown"})
            elif record.status in {RUNNING, CANCELING}:
                record.cancel_event.set()
                self._transition(record, CANCELING)
                self._add_event(record, "cancel_requested", {"phase": "shutdown"})
                if record.task is not None and not record.task.done():
                    record.task.cancel()
                    active_tasks.append(record.task)

        if active_tasks:
            await asyncio.wait(active_tasks, timeout=5)
        await self.broker.shutdown()

    async def submit(
        self,
        *,
        task_name: str,
        params: dict[str, Any],
        timeout_seconds: float,
    ) -> JobRecord:
        """Validate, enqueue, and return the current view of a job."""

        definition = self._tasks.get(task_name)
        if definition is None:
            raise QueueError(404, "task_not_found", f"Task {task_name!r} was not found")
        try:
            validated = definition.params_model.model_validate(params)
        except ValidationError as exc:
            raise QueueError(
                422,
                "task_params_invalid",
                "Task parameters failed validation",
            ) from exc

        record = JobRecord(task_name=task_name, timeout_seconds=timeout_seconds)
        self._add_event(record, "submitted")
        self._jobs[record.job_id] = record
        logger.bind(job_id=record.job_id, task_name=task_name, status=record.status).info(
            "Job submitted"
        )
        try:
            kicked = await self._runner.kiq(
                {
                    "job_id": record.job_id,
                    "task_name": task_name,
                    "params": validated.model_dump(mode="json"),
                    "timeout_seconds": timeout_seconds,
                }
            )
            record.taskiq_id = kicked.task_id
        except Exception as exc:
            self._finish(record, status=FAILED, error=self._serialize_error(exc))
            logger.bind(job_id=record.job_id, task_name=task_name, status=FAILED).exception(
                "Job failed to enqueue"
            )
        return record

    def job(self, job_id: str) -> JobRecord:
        record = self._jobs.get(job_id)
        if record is None:
            raise QueueError(404, "job_not_found", "Job not found")
        return record

    def jobs(
        self,
        *,
        status: str | None = None,
        offset: int,
        limit: int,
    ) -> list[JobRecord]:
        records = reversed(list(self._jobs.values()))
        if status is not None:
            records = (record for record in records if record.status == status)
        return list(itertools.islice(records, offset, offset + limit))

    async def cancel(self, job_id: str) -> JobRecord:
        record = self.job(job_id)
        if record.status == QUEUED:
            record.cancel_event.set()
            self._finish(record, status=CANCELED, detail={"phase": "queued"})
            logger.bind(job_id=record.job_id, task_name=record.task_name, status=CANCELED).info(
                "Queued job canceled"
            )
        elif record.status == RUNNING:
            record.cancel_event.set()
            self._transition(record, CANCELING)
            self._add_event(record, "cancel_requested")
            logger.bind(
                job_id=record.job_id,
                task_name=record.task_name,
                status=CANCELING,
            ).warning("Running job cancellation requested; child termination not guaranteed")
            if record.task is not None and not record.task.done():
                record.task.cancel()
        return record

    def record_view(self, record: JobRecord, *, detailed: bool = False) -> dict[str, Any]:
        view: dict[str, Any] = {
            "job_id": record.job_id,
            "task_name": record.task_name,
            "status": record.status,
            "submitted_at": record.submitted_at.isoformat(),
            "started_at": record.started_at.isoformat() if record.started_at else None,
            "finished_at": record.finished_at.isoformat() if record.finished_at else None,
            "duration_ms": self._duration_ms(record),
        }
        if detailed:
            view.update(
                {
                    "timeout_seconds": record.timeout_seconds,
                    "result": record.result,
                    "error": record.error,
                    "events": [event.asdict() for event in record.events],
                }
            )
        return view

    def event_views(self, record: JobRecord) -> list[dict[str, Any]]:
        return [event.asdict() for event in record.events]

    def _add_event(
        self,
        record: JobRecord,
        event: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        record.events.append(JobEvent(event=event, at=datetime.now(UTC), detail=detail or {}))
        del record.events[:-MAX_EVENTS_PER_JOB]

    def _transition(self, record: JobRecord, status: str) -> bool:
        if record.status in TERMINAL_STATUSES:
            return False
        if status == RUNNING and record.status != QUEUED:
            return False
        if status in {SUCCEEDED, FAILED} and record.status not in {RUNNING, CANCELING}:
            return False
        if status == CANCELING and record.status != RUNNING:
            return False
        record.status = status
        return True

    def _finish(
        self,
        record: JobRecord,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        if record.status in TERMINAL_STATUSES:
            return False
        record.status = status
        record.result = result
        record.error = error
        record.finished_at = datetime.now(UTC)
        self._add_event(record, status, detail)
        self._finished_ids[record.job_id] = None
        while len(self._finished_ids) > MAX_FINISHED_JOBS:
            oldest_id, _ = self._finished_ids.popitem(last=False)
            self._jobs.pop(oldest_id, None)
        logger.bind(
            job_id=record.job_id,
            task_name=record.task_name,
            status=status,
            duration_ms=self._duration_ms(record),
            error=error,
        ).info("Job finished")
        return True

    def _duration_ms(self, record: JobRecord) -> int | None:
        if record.started_at is None:
            return None
        ended = record.finished_at or datetime.now(UTC)
        return int((ended - record.started_at).total_seconds() * 1000)

    @staticmethod
    def _serialize_error(exc: BaseException) -> dict[str, Any]:
        return {"type": type(exc).__name__, "message": str(exc)}

    async def _execute_job(self, payload: dict[str, Any]) -> None:
        record = self._jobs.get(payload["job_id"])
        if record is None:
            return
        definition = self._tasks.get(record.task_name)
        if definition is None:
            self._finish(
                record,
                status=FAILED,
                error={"type": "UnknownTaskError", "message": "Task is no longer registered"},
            )
            return

        await self._semaphore.acquire()
        try:
            if record.cancel_event.is_set():
                self._finish(record, status=CANCELED)
                return
            if not self._transition(record, RUNNING):
                return
            record.started_at = datetime.now(UTC)
            record.task = asyncio.current_task()
            self._add_event(record, "started")
            logger.bind(job_id=record.job_id, task_name=record.task_name, status=RUNNING).info(
                "Job started"
            )

            context = JobContext(
                job_id=record.job_id,
                task_name=record.task_name,
                submitted_at=record.submitted_at,
                timeout_seconds=record.timeout_seconds,
                cancel_event=record.cancel_event,
            )
            try:
                async with asyncio.timeout(record.timeout_seconds):
                    raw_result = await definition.handler(
                        definition.params_model.model_validate(payload["params"]),
                        context,
                    )
                result = json.loads(json.dumps(raw_result))
                self._finish(record, status=SUCCEEDED, result=result)
            except TimeoutError:
                self._add_event(record, "timeout", {"child_termination_guaranteed": False})
                self._finish(
                    record,
                    status=TIMEOUT,
                    error={
                        "type": "TimeoutError",
                        "message": "Queue-level timeout happened; child process termination not guaranteed",
                    },
                )
            except asyncio.CancelledError:
                record.cancel_event.set()
                self._finish(record, status=CANCELED)
                raise
            except Exception as exc:
                self._finish(record, status=FAILED, error=self._serialize_error(exc))
        finally:
            record.task = None
            self._semaphore.release()
