from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.database import Base

JSON_PAYLOAD = JSON().with_variant(JSONB, "postgresql")


class Job(Base):
    __tablename__: str = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    external_request_id: Mapped[str] = mapped_column(
        String(255), unique=True, index=True
    )
    sqs_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tenant: Mapped[str | None] = mapped_column(String(255), nullable=True)

    result_fetch_url: Mapped[str] = mapped_column(Text)
    openai_request_payload: Mapped[dict[str, object]] = mapped_column(JSON_PAYLOAD)
    request_context: Mapped[dict[str, object] | None] = mapped_column(
        JSON_PAYLOAD, nullable=True
    )

    openai_response_id: Mapped[str | None] = mapped_column(
        String(255), unique=True, nullable=True, index=True
    )
    openai_response_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSON_PAYLOAD, nullable=True
    )
    result_payload: Mapped[object | None] = mapped_column(JSON_PAYLOAD, nullable=True)

    openai_state: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    postprocess_state: Mapped[str] = mapped_column(
        String(32), default="not_started", index=True
    )

    submission_attempts: Mapped[int] = mapped_column(Integer, default=0)
    fetch_attempts: Mapped[int] = mapped_column(Integer, default=0)
    notify_attempts: Mapped[int] = mapped_column(Integer, default=0)
    recovery_attempts: Mapped[int] = mapped_column(Integer, default=0)

    openai_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    openai_error_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSON_PAYLOAD, nullable=True
    )
    postprocess_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    postprocess_error_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSON_PAYLOAD, nullable=True
    )

    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    openai_terminal_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    result_fetched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    notified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_recovery_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    events: Mapped[list["JobEvent"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
    )


class JobEvent(Base):
    __tablename__ = "job_events"
    __table_args__ = (
        Index("ix_job_events_job_id_created_at", "job_id", "created_at"),
        Index("ix_job_events_event_type_created_at", "event_type", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True)
    source: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(64))
    external_event_id: Mapped[str | None] = mapped_column(
        String(255), unique=True, nullable=True
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSON_PAYLOAD)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    job: Mapped[Job | None] = relationship(back_populates="events")
