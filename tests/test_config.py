from app.config import Settings


def test_resolved_sqs_listener_queue_url_uses_override():
    settings = Settings(
        sqs_queue_url="https://sqs.ap-northeast-2.amazonaws.com/123/legacy",
        sqs_listener_queue_url="https://sqs.ap-northeast-2.amazonaws.com/123/listener",
    )

    assert (
        settings.resolved_sqs_listener_queue_url
        == "https://sqs.ap-northeast-2.amazonaws.com/123/listener"
    )


def test_resolved_sqs_listener_queue_url_falls_back_to_legacy_queue():
    settings = Settings(
        sqs_queue_url="https://sqs.ap-northeast-2.amazonaws.com/123/legacy",
        sqs_listener_queue_url=None,
    )

    assert (
        settings.resolved_sqs_listener_queue_url
        == "https://sqs.ap-northeast-2.amazonaws.com/123/legacy"
    )
