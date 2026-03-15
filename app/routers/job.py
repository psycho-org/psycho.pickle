import logging

from fastapi import APIRouter, Request, Response
from openai import InvalidWebhookSignatureError

from app.services.job import process_openai_webhook
from app.services.llm import get_webhook_client

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/openai/webhooks")
async def webhook(request: Request):
    client = get_webhook_client()
    try:
        body = await request.body()
        event = client.webhooks.unwrap(body, request.headers)
        if event.type.startswith("response."):
            await process_openai_webhook(
                event=event,
                raw_body=body.decode("utf-8"),
            )
            manager = getattr(request.app.state, "background_manager", None)
            if manager is not None:
                manager.notify_work_available()
        return Response(status_code=200)
    except InvalidWebhookSignatureError as exc:
        logger.warning("Invalid webhook signature: %s", exc)
        return Response("Invalid signature", status_code=400)
    except Exception:
        logger.exception("Failed to process webhook")
        return Response("Webhook processing failed", status_code=500)
