from datetime import datetime

from pydantic import BaseModel


class JobCreate(BaseModel):
    input_text: str


class JobResponse(BaseModel):
    id: int
    status: str
    input_text: str
    output_text: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
