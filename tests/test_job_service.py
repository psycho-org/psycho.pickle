import json
from datetime import UTC, datetime
from types import SimpleNamespace

from app.constants import OPENAI_STATE_COMPLETED, POSTPROCESS_STATE_NOTIFY_SUCCEEDED
from app.models import Job
from app.services.job import (
    build_notify_sqs_request,
    build_notify_payload,
    build_openai_request,
    map_openai_status,
    map_webhook_type_to_state,
    parse_sqs_payload,
    resolve_notify_target,
)


def test_parse_sqs_payload():
    payload = parse_sqs_payload(
        """
        {
          "external_request_id": "ext-123",
          "result_fetch_url": "https://example.com/results/123",
          "openai_request": {
            "model": "gpt-5.4",
            "input": "hello"
          }
        }
        """
    )

    assert payload.external_request_id == "ext-123"
    assert str(payload.result_fetch_url) == "https://example.com/results/123"
    assert payload.openai_request["model"] == "gpt-5.4"


def test_build_openai_request_injects_metadata_and_background():
    request = build_openai_request(
        job_id=42,
        external_request_id="ext-123",
        payload={
            "model": "gpt-5.4",
            "input": "hello",
            "metadata": {"tenant": "team-a"},
        },
    )

    assert request["background"] is True
    assert request["metadata"]["tenant"] == "team-a"
    assert request["metadata"]["job_id"] == "42"
    assert request["metadata"]["external_request_id"] == "ext-123"


def test_state_mappings():
    assert map_openai_status("completed") == OPENAI_STATE_COMPLETED
    assert map_openai_status("in_progress") == "submitted"
    assert map_webhook_type_to_state("response.cancelled") == "cancelled"


def test_build_notify_payload():
    job = Job(
        id=7,
        external_request_id="ext-123",
        result_fetch_url="https://example.com/results/123",
        openai_request_payload={"model": "gpt-5.4", "input": "hello"},
        openai_response_id="resp_123",
        openai_state="completed",
        postprocess_state=POSTPROCESS_STATE_NOTIFY_SUCCEEDED,
        result_payload={"answer": "ok"},
        notified_at=datetime(2026, 3, 15, tzinfo=UTC),
    )

    payload = build_notify_payload(job)

    assert payload.job_id == 7
    assert payload.openai_state == "completed"
    assert payload.result == {"answer": "ok"}
    # assert payload.result_fetch_status == "succeeded"


def test_resolve_notify_target_prefers_sqs():
    settings = SimpleNamespace(
        business_notify_sqs_queue_url="https://sqs.ap-northeast-2.amazonaws.com/123/notify",
        business_notify_url="https://example.com/callback",
    )

    assert resolve_notify_target(settings) == (
        "sqs",
        "https://sqs.ap-northeast-2.amazonaws.com/123/notify",
    )


def test_resolve_notify_target_falls_back_to_http():
    settings = SimpleNamespace(
        business_notify_sqs_queue_url=None,
        business_notify_url="https://example.com/callback",
    )

    assert resolve_notify_target(settings) == ("http", "https://example.com/callback")


def test_build_notify_sqs_request_for_fifo_queue():
    job = Job(
        id=7,
        external_request_id="ext-123",
        tenant="team-a",
        result_fetch_url="https://example.com/results/123",
        openai_request_payload={"model": "gpt-5.4", "input": "hello"},
        openai_response_id="resp_123",
        openai_state="completed",
        postprocess_state="notify_in_progress",
    )

    request = build_notify_sqs_request(
        job=job,
        queue_url="https://sqs.ap-northeast-2.amazonaws.com/123/notify.fifo",
        payload={"job_id": 7, "status": "completed"},
        settings=SimpleNamespace(
            business_notify_sqs_message_group_id="pickle-notify",
        ),
    )

    assert request["QueueUrl"].endswith("/notify.fifo")
    assert json.loads(request["MessageBody"]) == {"job_id": 7, "status": "completed"}
    assert request["MessageGroupId"] == "pickle-notify"
    assert (
        request["MessageDeduplicationId"] == "job-notify-7-completed-notify_in_progress"
    )
    assert (
        request["MessageAttributes"]["external_request_id"]["StringValue"] == "ext-123"
    )
    assert request["MessageAttributes"]["tenant"]["StringValue"] == "team-a"
