import asyncio
import json
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from langchain_openai import ChatOpenAI
from openai import AsyncOpenAI
from openai.types.webhooks import UnwrapWebhookEvent
from pydantic import ValidationError
from sqlalchemy import Select, or_, select
from sqlalchemy.exc import IntegrityError

from app.config import get_settings
from app.constants import (
    OPENAI_STATE_CANCELLED,
    OPENAI_STATE_COMPLETED,
    OPENAI_STATE_FAILED,
    OPENAI_STATE_INCOMPLETE,
    OPENAI_STATE_PENDING,
    OPENAI_STATE_SUBMITTED,
    # POSTPROCESS_STATE_FETCH_FAILED,
    # POSTPROCESS_STATE_FETCH_IN_PROGRESS,
    # POSTPROCESS_STATE_FETCH_PENDING,
    POSTPROCESS_STATE_NOT_REQUIRED,
    POSTPROCESS_STATE_NOT_STARTED,
    POSTPROCESS_STATE_NOTIFY_FAILED,
    POSTPROCESS_STATE_NOTIFY_IN_PROGRESS,
    POSTPROCESS_STATE_NOTIFY_PENDING,
    POSTPROCESS_STATE_NOTIFY_SUCCEEDED,
    TERMINAL_OPENAI_STATES,
)
from app.database import AsyncSessionLocal, engine
from app.models import Job, JobEvent
from app.schemas import BusinessNotifyPayload, SQSJobPayload
from langchain_core.messages import HumanMessage, SystemMessage

from app.services.llm import dump_openai_model, get_async_client, get_langchain_client
from app.services.sqs import build_sqs_client, is_fifo_queue

logger = logging.getLogger(__name__)

JSONValue = dict[str, Any] | list[Any] | str | int | float | bool | None

SQS_RECEIVED_EVENT = "sqs_received"
OPENAI_SUBMITTED_EVENT = "openai_submitted"
OPENAI_SUBMIT_FAILED_EVENT = "openai_submit_failed"
WEBHOOK_RECEIVED_EVENT = "webhook_received"
RESULT_FETCH_ATTEMPT_EVENT = "result_fetch_attempt"
RESULT_FETCH_SUCCEEDED_EVENT = "result_fetch_succeeded"
RESULT_FETCH_FAILED_EVENT = "result_fetch_failed"
NOTIFY_ATTEMPT_EVENT = "notify_attempt"
NOTIFY_SUCCEEDED_EVENT = "notify_succeeded"
NOTIFY_FAILED_EVENT = "notify_failed"
RECOVERY_CHECKED_EVENT = "recovery_checked"
MANUAL_RECOVERY_MARKED_EVENT = "manual_recovery_marked"


def utcnow() -> datetime:
    return datetime.now(UTC)


def map_openai_status(status: str | None) -> str:
    if status == OPENAI_STATE_COMPLETED:
        return OPENAI_STATE_COMPLETED
    if status == OPENAI_STATE_FAILED:
        return OPENAI_STATE_FAILED
    if status == OPENAI_STATE_INCOMPLETE:
        return OPENAI_STATE_INCOMPLETE
    if status == OPENAI_STATE_CANCELLED:
        return OPENAI_STATE_CANCELLED
    return OPENAI_STATE_SUBMITTED


def map_webhook_type_to_state(event_type: str) -> str:
    mapping = {
        "response.completed": OPENAI_STATE_COMPLETED,
        "response.failed": OPENAI_STATE_FAILED,
        "response.incomplete": OPENAI_STATE_INCOMPLETE,
        "response.cancelled": OPENAI_STATE_CANCELLED,
    }
    return mapping[event_type]


def build_openai_request(
    *, job_id: int, external_request_id: str, payload: dict[str, Any]
) -> dict[str, Any]:
    request_payload = dict(payload)
    metadata = dict(request_payload.get("metadata") or {})
    metadata["job_id"] = str(job_id)
    metadata["external_request_id"] = external_request_id
    request_payload["metadata"] = metadata
    request_payload["background"] = True

    settings = get_settings()
    if "model" not in request_payload and settings.openai_default_model:
        request_payload["model"] = settings.openai_default_model
    return request_payload


# NOTE: 프롬프트 빌더
def build_langchain_request(
    *, job_id: int, external_request_id: str, payload: dict[str, Any]
) -> str:
    # payload는 메인 서버에서 보내는 snake_case JSON 형태
    context = payload.get("context", {})
    summary = payload.get("summary", {})
    metrics = payload.get("metrics", {})

    prompt = f"""
Analyze the following sprint data and provide insights on sprint performance, potential issues, and recommendations.

Schema Version: {payload.get("schema_version", "unknown")}

Context:
- Workspace ID: {context.get("workspace_id", "unknown")}
- Sprint: {context.get("sprint", {}).get("name", "unknown")} (ID: {context.get("sprint", {}).get("id", "unknown")})
- Period: {context.get("sprint", {}).get("period_days", 0)} days
- Total Tasks: {context.get("sprint", {}).get("total_tasks_count", 0)}

Summary (Status Snapshot):
- TODO: {summary.get("status_snapshot", {}).get("todo_count", 0)}
- In Progress: {summary.get("status_snapshot", {}).get("in_progress_count", 0)}
- Done: {summary.get("status_snapshot", {}).get("done_count", 0)}
- Canceled: {summary.get("status_snapshot", {}).get("canceled_count", 0)}

Metrics:
- Completion: Unassigned Tasks: {metrics.get("completion", {}).get("unassigned_tasks_count", 0)}
- Stability: Goal Changes: {metrics.get("stability", {}).get("sprint_goal_change_count", 0)}, Period Changes: {metrics.get("stability", {}).get("sprint_period_change_count", 0)}
- Flow: Rework Events: {metrics.get("flow", {}).get("rework_events_count", 0)}, Direct TODO to Done: {metrics.get("flow", {}).get("todo_to_done_direct_count", 0)}, Scope Churn: {metrics.get("flow", {}).get("scope_churn_events_count", 0)}, Canceled Tasks: {metrics.get("flow", {}).get("canceled_tasks_count", 0)}

Provide a detailed analysis including:
1. Sprint health assessment
2. Key performance indicators
3. Potential risks or issues
4. Recommendations for improvement
"""
    return prompt


def compute_next_retry(attempt: int) -> datetime:
    delay_seconds = min(300, 2 ** min(attempt, 8))
    return utcnow() + timedelta(seconds=delay_seconds)


# def result_fetch_status_for_job(job: Job) -> str:
#     if job.result_fetched_at is not None or job.result_payload is not None:
#         return "succeeded"
#     if job.openai_state != OPENAI_STATE_COMPLETED:
#         return "skipped"
#     if job.postprocess_state in {
#         POSTPROCESS_STATE_FETCH_PENDING,
#         POSTPROCESS_STATE_FETCH_IN_PROGRESS,
#         POSTPROCESS_STATE_FETCH_FAILED,
#     }:
#         return "pending"
#     return "not_started"


def build_notify_error(job: Job) -> dict[str, Any] | None:
    if job.postprocess_error:
        return {
            "scope": "postprocess",
            "message": job.postprocess_error,
            "details": job.postprocess_error_payload,
        }
    if job.openai_error:
        return {
            "scope": "openai",
            "message": job.openai_error,
            "details": job.openai_error_payload,
        }
    return None


def build_notify_payload(job: Job) -> BusinessNotifyPayload:
    return BusinessNotifyPayload(
        job_id=job.id,
        external_request_id=job.external_request_id,
        openai_response_id=job.openai_response_id,
        openai_state=job.openai_state,
        postprocess_state=job.postprocess_state,
        # result_fetch_status=result_fetch_status_for_job(job),
        result=job.result_payload,
        error=build_notify_error(job),
        occurred_at=utcnow(),
    )


def resolve_notify_target(settings: Any) -> tuple[str, str]:
    if settings.business_notify_sqs_queue_url:
        return "sqs", settings.business_notify_sqs_queue_url
    if settings.business_notify_url:
        return "http", settings.business_notify_url
    raise RuntimeError(
        "BUSINESS_NOTIFY_URL or BUSINESS_NOTIFY_SQS_QUEUE_URL must be configured for notifications."
    )


def build_notify_sqs_request(
    *,
    job: Job,
    queue_url: str,
    payload: dict[str, Any],
    settings: Any | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or get_settings()
    message_attributes: dict[str, dict[str, str]] = {
        "external_request_id": {
            "DataType": "String",
            "StringValue": job.external_request_id,
        },
        "openai_state": {
            "DataType": "String",
            "StringValue": job.openai_state,
        },
        "postprocess_state": {
            "DataType": "String",
            "StringValue": job.postprocess_state,
        },
    }
    if job.tenant:
        message_attributes["tenant"] = {
            "DataType": "String",
            "StringValue": job.tenant,
        }

    request: dict[str, Any] = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(payload),
        "MessageAttributes": message_attributes,
    }
    if is_fifo_queue(queue_url):
        request["MessageGroupId"] = (
            resolved_settings.business_notify_sqs_message_group_id
            or job.external_request_id
        )
        request["MessageDeduplicationId"] = (
            f"job-notify-{job.id}-{job.openai_state}-{job.postprocess_state}"
        )
    return request


def _event_payload(
    *, payload: dict[str, Any] | None = None, raw_body: str | None = None
) -> dict[str, Any]:
    event_payload: dict[str, Any] = payload or {}
    if raw_body is not None:
        event_payload = {**event_payload, "raw_body": raw_body}
    return event_payload


def _apply_openai_terminal_state(
    *,
    job: Job,
    openai_state: str,
    occurred_at: datetime,
    error_message: str | None,
    error_payload: dict[str, Any] | None,
) -> None:
    job.openai_state = openai_state
    job.openai_terminal_at = occurred_at
    job.openai_error = error_message
    job.openai_error_payload = error_payload

    if openai_state == OPENAI_STATE_COMPLETED:
        # job.postprocess_state = POSTPROCESS_STATE_FETCH_PENDING
        job.postprocess_state = POSTPROCESS_STATE_NOTIFY_PENDING
        job.next_retry_at = utcnow()
        return

    job.postprocess_state = POSTPROCESS_STATE_NOTIFY_PENDING
    job.next_retry_at = utcnow()


def _with_row_lock(stmt: Select[tuple[Job]]) -> Select[tuple[Job]]:
    if engine.dialect.name == "postgresql":
        return stmt.with_for_update(skip_locked=True)
    return stmt.with_for_update()


def _with_claim_row_lock(stmt: Select[tuple[Job]]) -> Select[tuple[Job]]:
    if engine.dialect.name == "postgresql":
        return stmt.with_for_update(skip_locked=True)
    return stmt


def _append_event(
    *,
    job: Job | None,
    source: str,
    event_type: str,
    payload: dict[str, Any],
    external_event_id: str | None = None,
) -> JobEvent:
    return JobEvent(
        job=job,
        source=source,
        event_type=event_type,
        external_event_id=external_event_id,
        payload=payload,
    )


def _build_notify_destination_payload(*, transport: str, target: str) -> dict[str, str]:
    key = "queue_url" if transport == "sqs" else "notify_url"
    return {"transport": transport, key: target}


async def _load_or_create_job(
    payload: SQSJobPayload,
    message: dict[str, Any],
) -> tuple[Job, bool]:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = _with_row_lock(
                select(Job).where(
                    Job.external_request_id == payload.external_request_id
                )
            )
            job = (await session.execute(stmt)).scalar_one_or_none()

            if job is None:
                job = Job(
                    external_request_id=payload.external_request_id,
                    sqs_message_id=message.get("MessageId"),
                    tenant=payload.tenant,
                    result_fetch_url=str(payload.result_fetch_url),
                    openai_request_payload=payload.openai_request,
                    request_context=payload.context,
                    openai_state=OPENAI_STATE_PENDING,
                    postprocess_state=POSTPROCESS_STATE_NOT_STARTED,
                )
                session.add(job)
                await session.flush()
            else:
                job.sqs_message_id = message.get("MessageId")
                if not job.openai_response_id:
                    job.result_fetch_url = str(payload.result_fetch_url)
                    job.openai_request_payload = payload.openai_request
                    job.request_context = payload.context

            session.add(
                _append_event(
                    job=job,
                    source="sqs",
                    event_type=SQS_RECEIVED_EVENT,
                    payload=message,
                )
            )
            await session.flush()
            return job, job.openai_response_id is not None


async def process_sqs_message(
    message: dict[str, Any],
    *,
    langchain_client: ChatOpenAI | None = None,
) -> bool:
    raw_body = message.get("Body", "")
    try:
        payload = SQSJobPayload.model_validate_json(raw_body)
    except ValidationError:
        logger.exception("Failed to validate SQS payload: %s", raw_body)
        return False

    try:
        job, already_submitted = await _load_or_create_job(payload, message)
    except IntegrityError:
        logger.warning(
            "Duplicate insert raced for external_request_id=%s. Treating as retry.",
            payload.external_request_id,
        )
        return True

    if already_submitted:
        return True

    request_payload = build_langchain_request(
        job_id=job.id,
        external_request_id=job.external_request_id,
        payload=job.openai_request_payload,
    )

    # owns_client = langchain_client is None
    client = langchain_client or get_langchain_client()
    try:
        response = await client.ainvoke([
            SystemMessage(content="You are a helpful assistant. Always respond in the same language as the user's input."),
            HumanMessage(content=request_payload),
        ])
    except Exception as exc:
        logger.exception(
            "LangChain invocation failed for external_request_id=%s",
            job.external_request_id,
        )
        async with AsyncSessionLocal() as session:
            async with session.begin():
                persisted_job = await session.get(Job, job.id)
                if persisted_job is None:
                    return False
                persisted_job.submission_attempts += 1
                persisted_job.openai_error = str(exc)
                persisted_job.openai_error_payload = {"request": request_payload}
                session.add(
                    _append_event(
                        job=persisted_job,
                        source="langchain",
                        event_type="langchain_submit_failed",
                        payload={
                            "stage": "invoke",
                            "request": request_payload,
                            "error": str(exc),
                        },
                    )
                )
        return False

    # 성공 처리
    result_content = response.content if hasattr(response, "content") else str(response)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job.id)
            if persisted_job is None:
                return False

            persisted_job.submission_attempts += 1
            persisted_job.submitted_at = utcnow()
            persisted_job.openai_response_id = (
                f"langchain-{job.id}-{utcnow().isoformat()}"  # 임의 ID
            )
            persisted_job.openai_response_payload = {"result": result_content}
            persisted_job.openai_state = OPENAI_STATE_COMPLETED
            persisted_job.result_payload = {"analysis": result_content}
            persisted_job.openai_error = None
            persisted_job.openai_error_payload = None

            _apply_openai_terminal_state(
                job=persisted_job,
                openai_state=OPENAI_STATE_COMPLETED,
                occurred_at=utcnow(),
                error_message=None,
                error_payload=None,
            )

            session.add(
                _append_event(
                    job=persisted_job,
                    source="langchain",
                    event_type="langchain_completed",
                    payload={"result": result_content},
                )
            )
    return True


async def process_openai_webhook(*, event: UnwrapWebhookEvent, raw_body: str) -> None:
    event_payload = _event_payload(
        payload=event.model_dump(mode="json"),
        raw_body=raw_body,
    )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = select(JobEvent).where(JobEvent.external_event_id == event.id)
            if (await session.execute(stmt)).scalar_one_or_none():
                return

            job_stmt = select(Job).where(Job.openai_response_id == event.data.id)
            job = (await session.execute(job_stmt)).scalar_one_or_none()
            session.add(
                _append_event(
                    job=job,
                    source="openai",
                    event_type=WEBHOOK_RECEIVED_EVENT,
                    payload=event_payload,
                    external_event_id=event.id,
                )
            )

            if job is None:
                logger.warning(
                    "Received webhook for unknown response_id=%s", event.data.id
                )
                return

            openai_state = map_webhook_type_to_state(event.type)
            if (
                job.openai_state in TERMINAL_OPENAI_STATES
                and job.openai_state != openai_state
            ):
                logger.warning(
                    "Ignoring conflicting terminal webhook. job_id=%s existing=%s incoming=%s",
                    job.id,
                    job.openai_state,
                    openai_state,
                )
                return

            if job.openai_state == openai_state and job.postprocess_state not in {
                POSTPROCESS_STATE_NOT_STARTED,
                POSTPROCESS_STATE_NOT_REQUIRED,
            }:
                return

            occurred_at = datetime.fromtimestamp(event.created_at, tz=UTC)
            error_message = (
                None if openai_state == OPENAI_STATE_COMPLETED else openai_state
            )
            error_payload = (
                None if openai_state == OPENAI_STATE_COMPLETED else event_payload
            )

            _apply_openai_terminal_state(
                job=job,
                openai_state=openai_state,
                occurred_at=occurred_at,
                error_message=error_message,
                error_payload=error_payload,
            )


async def _claim_jobs_for_stage(
    *,
    eligible_states: tuple[str, ...],
    in_progress_state: str,
    limit: int,
) -> list[int]:
    lease_deadline = utcnow()
    lease_until = lease_deadline + timedelta(seconds=get_settings().lease_seconds)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = (
                select(Job)
                .where(
                    Job.postprocess_state.in_(eligible_states),
                    or_(
                        Job.next_retry_at.is_(None), Job.next_retry_at <= lease_deadline
                    ),
                )
                .order_by(Job.updated_at.asc())
                .limit(limit)
            )
            stmt = _with_claim_row_lock(stmt)
            jobs = list((await session.execute(stmt)).scalars().all())
            for job in jobs:
                job.postprocess_state = in_progress_state
                job.next_retry_at = lease_until
            return [job.id for job in jobs]


async def _claim_recovery_jobs(limit: int) -> list[int]:
    now = utcnow()
    stale_before = now - timedelta(seconds=get_settings().recovery_stale_after_seconds)
    recovery_retry_before = now - timedelta(
        seconds=get_settings().recovery_poll_interval_seconds
    )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = (
                select(Job)
                .where(
                    Job.openai_state == OPENAI_STATE_SUBMITTED,
                    Job.submitted_at.is_not(None),
                    Job.submitted_at <= stale_before,
                    or_(
                        Job.last_recovery_at.is_(None),
                        Job.last_recovery_at <= recovery_retry_before,
                    ),
                )
                .order_by(Job.submitted_at.asc())
                .limit(limit)
            )
            stmt = _with_claim_row_lock(stmt)
            jobs = list((await session.execute(stmt)).scalars().all())
            for job in jobs:
                job.last_recovery_at = now
                job.recovery_attempts += 1
            return [job.id for job in jobs]


"""
async def _fetch_result_payload(
    job_id: int,
    *,
    downstream_client: httpx.AsyncClient | None = None,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(Job, job_id)
    if job is None:
        return

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job_id)
            if persisted_job is None:
                return
            session.add(
                _append_event(
                    job=persisted_job,
                    source="downstream",
                    event_type=RESULT_FETCH_ATTEMPT_EVENT,
                    payload={"result_fetch_url": persisted_job.result_fetch_url},
                )
            )

    owns_client = downstream_client is None
    client = downstream_client or httpx.AsyncClient()
    try:
        try:
            response = await client.get(
                job.result_fetch_url,
                timeout=get_settings().result_fetch_timeout_seconds,
            )
            response.raise_for_status()
            result_payload: JSONValue = response.json()
        except Exception as exc:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    persisted_job = await session.get(Job, job_id)
                    if persisted_job is None:
                        return
                    persisted_job.fetch_attempts += 1
                    persisted_job.postprocess_state = POSTPROCESS_STATE_FETCH_FAILED
                    persisted_job.postprocess_error = str(exc)
                    persisted_job.postprocess_error_payload = {
                        "result_fetch_url": persisted_job.result_fetch_url,
                    }
                    persisted_job.next_retry_at = compute_next_retry(
                        persisted_job.fetch_attempts
                    )
                    session.add(
                        _append_event(
                            job=persisted_job,
                            source="downstream",
                            event_type=RESULT_FETCH_FAILED_EVENT,
                            payload={
                                "result_fetch_url": persisted_job.result_fetch_url,
                                "error": str(exc),
                            },
                        )
                    )
            return
    finally:
        if owns_client:
            await client.aclose()

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job_id)
            if persisted_job is None:
                return
            persisted_job.fetch_attempts += 1
            persisted_job.result_payload = result_payload
            persisted_job.result_fetched_at = utcnow()
            persisted_job.postprocess_state = POSTPROCESS_STATE_NOTIFY_PENDING
            persisted_job.postprocess_error = None
            persisted_job.postprocess_error_payload = None
            persisted_job.next_retry_at = utcnow()
            session.add(
                _append_event(
                    job=persisted_job,
                    source="downstream",
                    event_type=RESULT_FETCH_SUCCEEDED_EVENT,
                    payload={
                        "result_fetch_url": persisted_job.result_fetch_url,
                        "result": result_payload,
                    },
                )
            )
"""


async def _notify_job(
    job_id: int,
    *,
    downstream_client: httpx.AsyncClient | None = None,
    sqs_client: Any | None = None,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(Job, job_id)
        print(f"🔔 Notifying downstream for job_id={job_id}")
    if job is None:
        print(f"⚠️ Job not found for job_id={job_id}")
        return
    print(
        f"📋 Job details: openai_state={job.openai_state} postprocess_state={job.postprocess_state} notify_attempts={job.notify_attempts}"
    )

    settings = get_settings()
    transport, target = resolve_notify_target(settings)
    notify_payload = build_notify_payload(job).model_dump(mode="json")
    destination_payload = _build_notify_destination_payload(
        transport=transport,
        target=target,
    )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job_id)
            if persisted_job is None:
                return
            session.add(
                _append_event(
                    job=persisted_job,
                    source="downstream",
                    event_type=NOTIFY_ATTEMPT_EVENT,
                    payload={**destination_payload, "payload": notify_payload},
                )
            )

    try:
        if transport == "sqs":
            print(f"📤 Sending completion notification to SQS for job_id={job_id}")
            request = build_notify_sqs_request(
                job=job,
                queue_url=target,
                payload=notify_payload,
                settings=settings,
            )
            owns_client = sqs_client is None
            client = sqs_client or build_sqs_client(settings)
            try:
                response = await asyncio.to_thread(client.send_message, **request)
            finally:
                if owns_client:
                    with suppress(Exception):
                        close = getattr(client, "close", None)
                        if callable(close):
                            close()
            success_payload = {
                **destination_payload,
                "payload": notify_payload,
                "message_id": response.get("MessageId"),
            }
            logger.info(
                "✅ Successfully sent completion notification to SQS for job_id={}",
                job_id,
            )
        else:
            headers: dict[str, str] = {}
            if (
                settings.business_notify_auth_header_name
                and settings.business_notify_auth_header_value
            ):
                headers[settings.business_notify_auth_header_name] = (
                    settings.business_notify_auth_header_value
                )

            owns_client = downstream_client is None
            client = downstream_client or httpx.AsyncClient()
            try:
                response = await client.post(
                    target,
                    json=notify_payload,
                    headers=headers,
                    timeout=settings.business_notify_timeout_seconds,
                )
                response.raise_for_status()
            finally:
                if owns_client:
                    await client.aclose()
            success_payload = {**destination_payload, "payload": notify_payload}
    except Exception as exc:
        logger.error(
            "❌ Failed to send completion notification to SQS for job_id={}",
            job_id,
            exc_info=exc,
        )
        async with AsyncSessionLocal() as session:
            async with session.begin():
                persisted_job = await session.get(Job, job_id)
                if persisted_job is None:
                    return
                persisted_job.notify_attempts += 1
                persisted_job.postprocess_state = POSTPROCESS_STATE_NOTIFY_FAILED
                persisted_job.postprocess_error = str(exc)
                persisted_job.postprocess_error_payload = {
                    **destination_payload,
                    "payload": notify_payload,
                }
                persisted_job.next_retry_at = compute_next_retry(
                    persisted_job.notify_attempts
                )
                session.add(
                    _append_event(
                        job=persisted_job,
                        source="downstream",
                        event_type=NOTIFY_FAILED_EVENT,
                        payload={
                            **destination_payload,
                            "payload": notify_payload,
                            "error": str(exc),
                        },
                    )
                )
        return

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job_id)
            if persisted_job is None:
                return
            persisted_job.notify_attempts += 1
            persisted_job.notified_at = utcnow()
            persisted_job.postprocess_state = POSTPROCESS_STATE_NOTIFY_SUCCEEDED
            persisted_job.postprocess_error = None
            persisted_job.postprocess_error_payload = None
            persisted_job.next_retry_at = None
            session.add(
                _append_event(
                    job=persisted_job,
                    source="downstream",
                    event_type=NOTIFY_SUCCEEDED_EVENT,
                    payload=success_payload,
                )
            )


async def claim_due_postprocess_work(limit: int) -> list[tuple[str, int]]:
    if limit <= 0:
        return []

    # fetch_job_ids = await _claim_jobs_for_stage(
    #     eligible_states=(
    #         POSTPROCESS_STATE_FETCH_PENDING,
    #         POSTPROCESS_STATE_FETCH_FAILED,
    #         POSTPROCESS_STATE_FETCH_IN_PROGRESS,
    #     ),
    #     in_progress_state=POSTPROCESS_STATE_FETCH_IN_PROGRESS,
    #     limit=limit,
    # )
    # remaining = limit - len(fetch_job_ids)
    # notify_job_ids: Sequence[int] = []
    # if remaining > 0:
    notify_job_ids = await _claim_jobs_for_stage(
        eligible_states=(
            POSTPROCESS_STATE_NOTIFY_PENDING,
            POSTPROCESS_STATE_NOTIFY_FAILED,
            POSTPROCESS_STATE_NOTIFY_IN_PROGRESS,
        ),
        in_progress_state=POSTPROCESS_STATE_NOTIFY_IN_PROGRESS,
        # limit=remaining,
        limit=limit,
    )

    # return [("fetch", job_id) for job_id in fetch_job_ids] + [
    #     ("notify", job_id) for job_id in notify_job_ids
    # ]

    return [("notify", job_id) for job_id in notify_job_ids]


async def process_postprocess_work(
    work_kind: str,
    job_id: int,
    *,
    downstream_client: httpx.AsyncClient | None = None,
    sqs_client: Any | None = None,
) -> None:
    # if work_kind == "fetch":
    #     print(f"📥 Processing result fetch work for job_id={job_id}")
    #     await _fetch_result_payload(job_id, downstream_client=downstream_client)
    #     return
    if work_kind == "notify":
        print(f"🚀 Processing notify work for job_id={job_id}")
        await _notify_job(
            job_id,
            downstream_client=downstream_client,
            sqs_client=sqs_client,
        )
        return
    raise ValueError(f"Unsupported postprocess work kind: {work_kind}")


async def claim_recovery_job_ids(limit: int) -> list[int]:
    return await _claim_recovery_jobs(limit)


async def recover_job(
    job_id: int,
    *,
    openai_client: AsyncOpenAI | None = None,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(Job, job_id)
    if job is None or not job.openai_response_id:
        return

    owns_client = openai_client is None
    client = openai_client or get_async_client()
    try:
        try:
            response = await client.responses.retrieve(job.openai_response_id)
        except Exception as exc:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    persisted_job = await session.get(Job, job_id)
                    if persisted_job is None:
                        return
                    session.add(
                        _append_event(
                            job=persisted_job,
                            source="system",
                            event_type=RECOVERY_CHECKED_EVENT,
                            payload={"error": str(exc)},
                        )
                    )
            return
    finally:
        if owns_client:
            with suppress(Exception):
                await client.close()

    response_payload = dump_openai_model(response)
    openai_state = map_openai_status(response_payload.get("status"))

    async with AsyncSessionLocal() as session:
        async with session.begin():
            persisted_job = await session.get(Job, job_id)
            if persisted_job is None:
                return

            persisted_job.openai_response_payload = response_payload
            event_type = RECOVERY_CHECKED_EVENT
            if (
                openai_state in TERMINAL_OPENAI_STATES
                and persisted_job.openai_state not in TERMINAL_OPENAI_STATES
            ):
                event_type = MANUAL_RECOVERY_MARKED_EVENT
                _apply_openai_terminal_state(
                    job=persisted_job,
                    openai_state=openai_state,
                    occurred_at=utcnow(),
                    error_message=(
                        None if openai_state == OPENAI_STATE_COMPLETED else openai_state
                    ),
                    error_payload=(
                        None
                        if openai_state == OPENAI_STATE_COMPLETED
                        else response_payload
                    ),
                )

            session.add(
                _append_event(
                    job=persisted_job,
                    source="system",
                    event_type=event_type,
                    payload=response_payload,
                )
            )


async def process_due_postprocess_jobs(limit: int | None = None) -> None:
    batch_size = limit or get_settings().postprocess_batch_size
    work_items = await claim_due_postprocess_work(batch_size)
    for work_kind, job_id in work_items:
        await process_postprocess_work(work_kind, job_id)


async def recover_stale_jobs(limit: int | None = None) -> None:
    batch_size = limit or get_settings().recovery_batch_size
    job_ids = await claim_recovery_job_ids(batch_size)
    if not job_ids:
        return
    for job_id in job_ids:
        await recover_job(job_id)


def parse_sqs_payload(raw_body: str) -> SQSJobPayload:
    return SQSJobPayload.model_validate_json(raw_body)


def parse_json_body(raw_body: str) -> JSONValue:
    return json.loads(raw_body)
