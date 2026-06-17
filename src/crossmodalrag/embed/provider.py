from __future__ import annotations

import os
from typing import Protocol, runtime_checkable


DEFAULT_EMBED_MODEL = "BAAI/bge-small-en-v1.5"


class MissingEmbeddingBackend(RuntimeError):
    """Raised when the optional embedding backend is requested but not installed."""


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Minimal interface every embedding backend must satisfy.

    ``name`` is the model identifier; stored alongside each vector so retrieval
    only compares vectors produced by the same model.
    """

    name: str
    dim: int

    def embed(self, texts: list[str]) -> list[list[float]]:
        ...


class FastEmbedProvider:
    """Local, offline-capable embeddings via fastembed (ONNX). No torch."""

    def __init__(self, model_name: str = DEFAULT_EMBED_MODEL) -> None:
        try:
            from fastembed import TextEmbedding
        except ImportError as exc:  # pragma: no cover - exercised via get_default_provider
            raise MissingEmbeddingBackend(
                "Embedding backend not installed. Run: pip install -e \".[embeddings]\""
            ) from exc

        self.name = model_name
        self._model = TextEmbedding(model_name=model_name)
        # Probe a single embed to discover dimensionality.
        probe = next(iter(self._model.embed(["dimension probe"])))
        self.dim = int(len(probe))

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return [list(map(float, vec)) for vec in self._model.embed(texts)]


def get_default_provider(model_name: str | None = None) -> EmbeddingProvider | None:
    """Return the configured provider, or ``None`` if the backend isn't installed.

    Returning ``None`` lets callers (ask/eval) degrade gracefully to lexical
    retrieval. Commands that strictly require embeddings (``reindex-embeddings``)
    should call :func:`require_default_provider` instead.
    """
    model = model_name or os.getenv("CMRAG_EMBED_MODEL", DEFAULT_EMBED_MODEL)
    try:
        return FastEmbedProvider(model)
    except MissingEmbeddingBackend:
        return None


def require_default_provider(model_name: str | None = None) -> EmbeddingProvider:
    model = model_name or os.getenv("CMRAG_EMBED_MODEL", DEFAULT_EMBED_MODEL)
    return FastEmbedProvider(model)
