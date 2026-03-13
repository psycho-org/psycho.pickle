from fastapi import APIRouter, Request, Response
from openai import InvalidWebhookSignatureError

from app.services.llm import get_client

router = APIRouter()


@router.post("/webhook")
async def webhook(request: Request):
    client = get_client()
    try:
        body = await request.body()
        event = client.webhooks.unwrap(body, request.headers)

        if event.type == "response.completed":
            response_id = event.data.id
            response = client.responses.retrieve(response_id)
            print("Response output:", response.output_text)

        return Response(status_code=200)
    except InvalidWebhookSignatureError as e:
        print("Invalid signature", e)
        return Response("Invalid signature", status_code=400)
