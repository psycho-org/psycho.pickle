from datetime import datetime
from typing import Any, ClassVar

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field


class SQSJobPayload(BaseModel):
    external_request_id: str = Field(min_length=1, max_length=255)
    result_fetch_url: AnyHttpUrl
    openai_request: dict[str, Any]
    tenant: str | None = Field(default=None, max_length=255)
    context: dict[str, Any] | None = None

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")


class BusinessNotifyPayload(BaseModel):
    job_id: int
    external_request_id: str
    openai_response_id: str | None
    openai_state: str
    postprocess_state: str
    result_fetch_status: str
    result: Any | None = None
    error: dict[str, Any] | None = None
    occurred_at: datetime


class JobResponse(BaseModel):
    id: int
    external_request_id: str
    openai_response_id: str | None
    openai_state: str
    postprocess_state: str
    created_at: datetime
    updated_at: datetime

    model_config: ClassVar[ConfigDict] = ConfigDict(from_attributes=True)
