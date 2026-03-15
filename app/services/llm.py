from collections.abc import Mapping
from typing import Any

from openai import AsyncOpenAI, OpenAI

from app.config import get_settings


def _build_client_kwargs() -> dict[str, str]:
    settings = get_settings()
    api_key = settings.openai_api_key or "webhook-verification-only"
    return {
        "api_key": api_key,
        "webhook_secret": settings.openai_webhook_secret,
    }


def get_webhook_client() -> OpenAI:
    return OpenAI(**_build_client_kwargs())


def get_async_client() -> AsyncOpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY must be configured for the worker.")
    return AsyncOpenAI(**_build_client_kwargs())


def dump_openai_model(payload: Any) -> dict[str, Any]:
    if isinstance(payload, Mapping):
        return dict(payload)
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json")
    raise TypeError(f"Unsupported OpenAI payload type: {type(payload)!r}")
