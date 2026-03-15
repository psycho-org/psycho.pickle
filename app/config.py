from functools import lru_cache
from typing import ClassVar

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str | None = None
    db_host: str
    db_port: str
    db_name: str
    db_username: str
    db_password: str

    openai_api_key: str | None = None
    openai_default_model: str | None = None
    openai_webhook_secret: str

    sqs_queue_url: str | None = None
    sqs_listener_queue_url: str | None = None
    sqs_wait_time_seconds: int = 20
    sqs_visibility_timeout_seconds: int = 120
    sqs_max_messages: int = 5
    worker_idle_sleep_seconds: float = 1.0
    enable_inprocess_worker: bool = False
    worker_submit_concurrency: int = 4
    worker_postprocess_concurrency: int = 4
    worker_shutdown_grace_seconds: float = 30.0
    aws_region: str | None = None

    business_notify_url: str | None = None
    business_notify_sqs_queue_url: str | None = None
    business_notify_sqs_message_group_id: str | None = None
    business_notify_timeout_seconds: float = 10.0
    business_notify_auth_header_name: str | None = None
    business_notify_auth_header_value: str | None = None

    result_fetch_timeout_seconds: float = 10.0
    result_fetch_max_attempts: int = 5
    notify_max_attempts: int = 5

    recovery_stale_after_seconds: int = 60
    recovery_poll_interval_seconds: int = 30
    recovery_batch_size: int = 10
    postprocess_poll_interval_seconds: int = 5
    postprocess_batch_size: int = 10
    lease_seconds: int = 300

    db_auto_create_tables: bool = True

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=(".env", ".env.local"),
        extra="ignore",
    )

    @computed_field  # pyright: ignore[reportUntypedFunctionDecorator]
    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+asyncpg://{self.db_username}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @computed_field  # pyright: ignore[reportUntypedFunctionDecorator]
    @property
    def resolved_sqs_listener_queue_url(self) -> str | None:
        return self.sqs_listener_queue_url or self.sqs_queue_url


@lru_cache
def get_settings() -> Settings:
    return Settings()  # pyright: ignore[reportCallIssue]
