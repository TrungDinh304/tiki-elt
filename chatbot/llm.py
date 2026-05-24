"""ds2api (OpenAI-compatible) client for chat completions.

Embedding is handled locally — see `embeddings.py`.
"""
from __future__ import annotations

import os

from openai import OpenAI

DS2API_BASE_URL = os.getenv("DS2API_BASE_URL", "http://host.docker.internal:8000/v1")
DS2API_KEY = os.getenv("DS2API_KEY", "sk-dummy")
CHAT_MODEL = os.getenv("CHAT_MODEL", "deepseek-v4-flash")

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(base_url=DS2API_BASE_URL, api_key=DS2API_KEY)
    return _client


def chat_stream(system: str, history: list[dict], user_msg: str):
    """Yield content chunks from a streaming chat completion."""
    messages = [{"role": "system", "content": system}] + history + [
        {"role": "user", "content": user_msg}
    ]
    stream = _get_client().chat.completions.create(
        model=CHAT_MODEL,
        messages=messages,
        stream=True,
        temperature=0.3,
    )
    for chunk in stream:
        delta = chunk.choices[0].delta.content if chunk.choices else None
        if delta:
            yield delta
