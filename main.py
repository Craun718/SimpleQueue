"""ASGI entry point."""

import uvicorn

from app.config import Settings
from app.main import create_app
from app.queue.service import TaskQueue
from app.tasks import register_tasks


queue = TaskQueue()
register_tasks(queue)

settings = Settings()
app = create_app(queue, log_dir=settings.log_dir)


if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
