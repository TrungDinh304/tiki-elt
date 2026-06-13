"""OpenAI-compatible LLM client for chat completions.

Targets any OpenAI-compatible gateway — default is 9router running on the
docker host. Embedding is handled locally — see `embeddings.py`.
"""
from __future__ import annotations

import os

from openai import OpenAI

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://host.docker.internal:20128/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "sk-dummy")
CHAT_MODEL = os.getenv("CHAT_MODEL", "gc/gemini-3-flash-preview")

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
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
