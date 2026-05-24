"""Local embedding via sentence-transformers (BGE-M3 by default).

Runs entirely inside the chatbot container — no external API call for the
embedding step. BGE-M3 is multilingual and handles Vietnamese well; its
output dimension is 1024, which matches `EMBEDDING_DIM` in pgvector schema.

The model is loaded lazily on first call. First load downloads ~2.3GB from
Hugging Face into `$HF_HOME` (default `/root/.cache/huggingface`); mount a
named volume there in docker-compose so the download is reused across
container restarts.
"""
from __future__ import annotations

import os
import threading

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")

_model = None
_lock = threading.Lock()


def _get_model():
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                # Imported lazily so the chatbot container can still import this
                # module before sentence-transformers is installed (useful in CI).
                from sentence_transformers import SentenceTransformer

                _model = SentenceTransformer(MODEL_NAME)
    return _model


def embed(text: str) -> list[float]:
    vec = _get_model().encode(text, normalize_embeddings=True)
    return vec.tolist()
