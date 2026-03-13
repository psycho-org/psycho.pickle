import os

from openai import OpenAI

client = OpenAI(webhook_secret=os.environ["OPENAI_WEBHOOK_SECRET"])
