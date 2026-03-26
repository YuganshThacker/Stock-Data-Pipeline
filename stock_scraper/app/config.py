import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    database_url: str = os.environ.get("DATABASE_URL", "")
    max_concurrent_requests: int = 2
    rate_limit_per_second: float = 1.0
    request_timeout: int = 30
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    batch_size: int = 50
    scraper_mode: str = "full"
    api_host: str = "0.0.0.0"
    api_port: int = 8001
    log_level: str = "INFO"
    log_format: str = "json"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
