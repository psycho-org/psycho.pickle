from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import create_app


def test_lifespan_starts_background_manager_when_enabled(monkeypatch):
    calls: list[str] = []

    class FakeManager:
        def __init__(self, settings):
            assert settings.enable_inprocess_worker is True
            calls.append("manager_init")

        async def start(self):
            calls.append("manager_start")

        async def stop(self):
            calls.append("manager_stop")

    async def fake_close_database():
        calls.append("close_database")

    async def fake_init_database():
        calls.append("init_database")

    settings = SimpleNamespace(
        db_auto_create_tables=False,
        enable_inprocess_worker=True,
    )

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.BackgroundWorkerManager", FakeManager)
    monkeypatch.setattr("app.main.close_database", fake_close_database)
    monkeypatch.setattr("app.main.init_database", fake_init_database)

    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert calls == [
        "manager_init",
        "manager_start",
        "manager_stop",
        "close_database",
    ]


def test_lifespan_skips_background_manager_when_disabled(monkeypatch):
    calls: list[str] = []

    class FakeManager:
        def __init__(self, _settings):
            calls.append("manager_init")

        async def start(self):
            calls.append("manager_start")

        async def stop(self):
            calls.append("manager_stop")

    async def fake_close_database():
        calls.append("close_database")

    async def fake_init_database():
        calls.append("init_database")

    settings = SimpleNamespace(
        db_auto_create_tables=False,
        enable_inprocess_worker=False,
    )

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.BackgroundWorkerManager", FakeManager)
    monkeypatch.setattr("app.main.close_database", fake_close_database)
    monkeypatch.setattr("app.main.init_database", fake_init_database)

    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert calls == ["close_database"]
