# WMMaker Task Queue

A small in-process FastAPI service for running registered async jobs serially through Taskiq.

## Run

```bash
uv sync
uv run python main.py
```

The API defaults to `http://127.0.0.1:8000`. Override it with `WMMAKER_HOST`, `WMMAKER_PORT`, or `WMMAKER_LOG_DIR`. Structured loguru JSON logs go to stdout and `logs/queue.jsonl`.

Jobs run one at a time in submission order, with a single uvicorn worker. State is in memory and disappears on restart. The latest 1000 finished jobs are retained; each job keeps up to 100 lifecycle events. Default timeout is 300 seconds; submissions accept 1 to 3600 seconds. Cancel and timeout cancel the outer Python task only, so termination of an uncontrolled child process is not guaranteed.

## API

List registered task schemas:

```bash
curl -sS http://127.0.0.1:8000/tasks
```

Submit the bundled `sleep` task:

```bash
curl -sS -X POST http://127.0.0.1:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"task_name":"sleep","params":{"seconds":2}}'
```

Read, list, inspect, or cancel a job:

```bash
curl -sS "http://127.0.0.1:8000/jobs?status=running&offset=0&limit=50"
curl -sS http://127.0.0.1:8000/jobs/JOB_ID
curl -sS http://127.0.0.1:8000/jobs/JOB_ID/events
curl -sS -X POST http://127.0.0.1:8000/jobs/JOB_ID/cancel
```

Errors use the uniform envelope:

```json
{"error": {"code": "job_not_found", "message": "Job not found"}}
```

Add application tasks in `app/tasks.py` with `queue.task`; handlers receive validated parameters and a `JobContext`, and return a JSON-serializable dictionary.

## Tests

```bash
uv run pytest -q
```
