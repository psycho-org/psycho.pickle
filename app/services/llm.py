from openai import OpenAI

from app.config import get_settings


def get_client() -> OpenAI:
    return OpenAI(webhook_secret=get_settings().openai_webhook_secret)
