from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, ConfigDict


class JobCreate(BaseModel):
    input_text: str


class JobResponse(BaseModel):
    id: int
    status: str
    input_text: str
    output_text: str | None
    created_at: datetime
    updated_at: datetime

    model_config: ClassVar[ConfigDict] = ConfigDict(from_attributes=True)
