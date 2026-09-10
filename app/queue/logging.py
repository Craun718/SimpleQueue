"""Structured logging setup for the queue API."""

import sys
from pathlib import Path

from loguru import logger


def configure_logging(log_dir: Path | None = None) -> None:
    """Configure JSON logging for stdout and the queue log file."""

    directory = log_dir or Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stdout, serialize=True, enqueue=False)
    logger.add(
        directory / "queue.jsonl",
        serialize=True,
        enqueue=True,
        rotation="20 MB",
        retention="14 days",
    )
