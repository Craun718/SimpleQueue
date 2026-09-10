"""Runtime settings loaded from the environment."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Settings for running the queue API."""

    model_config = SettingsConfigDict(env_prefix="WMMAKER_", env_nested_delimiter="__")

    host: str = "127.0.0.1"
    port: int = 8000
    log_dir: Path = PROJECT_ROOT / "logs"
