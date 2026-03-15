from types import SimpleNamespace

from fastapi.testclient import TestClient
from openai import InvalidWebhookSignatureError

from app.main import app


class _FakeClient:
    def __init__(self, event):
        self.webhooks = SimpleNamespace(unwrap=lambda _body, _headers: event)


def test_webhook_completed(monkeypatch):
    captured: dict[str, str] = {}

    async def fake_process_openai_webhook(*, event, raw_body):
        captured["event_type"] = event.type
        captured["response_id"] = event.data.id
        captured["raw_body"] = raw_body

    event = SimpleNamespace(
        id="evt_123",
        type="response.completed",
        data=SimpleNamespace(id="resp_123"),
        created_at=1_763_000_000,
    )

    monkeypatch.setattr(
        "app.routers.job.get_webhook_client", lambda: _FakeClient(event)
    )
    monkeypatch.setattr(
        "app.routers.job.process_openai_webhook", fake_process_openai_webhook
    )

    client = TestClient(app)
    response = client.post(
        "/openai/webhooks",
        headers={"webhook-id": "evt_123"},
        content=b'{"id":"evt_123"}',
    )

    assert response.status_code == 200
    assert captured["event_type"] == "response.completed"
    assert captured["response_id"] == "resp_123"
    assert captured["raw_body"] == '{"id":"evt_123"}'


def test_webhook_invalid_signature(monkeypatch):
    fake_client = SimpleNamespace(
        webhooks=SimpleNamespace(
            unwrap=lambda _body, _headers: (_ for _ in ()).throw(
                InvalidWebhookSignatureError("bad signature")
            )
        )
    )

    monkeypatch.setattr("app.routers.job.get_webhook_client", lambda: fake_client)

    client = TestClient(app)
    response = client.post("/openai/webhooks", content=b"{}")

    assert response.status_code == 400
