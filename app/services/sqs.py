from typing import Any

from app.config import Settings, get_settings


def build_sqs_client(settings: Settings | None = None) -> Any:
    resolved_settings = settings or get_settings()
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("boto3 must be installed to use SQS features.") from exc

    kwargs: dict[str, str] = {}
    if resolved_settings.aws_region:
        kwargs["region_name"] = resolved_settings.aws_region
    return boto3.client("sqs", **kwargs)


def is_fifo_queue(queue_url: str) -> bool:
    return queue_url.endswith(".fifo")
