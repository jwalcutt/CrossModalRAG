"""Optional cross-modal backend probes.

Cross-modal ingestion depends on optional extras (``[pdf]``, ``[ocr]``) so the
core install stays dependency-free. This module mirrors the graceful-degradation
pattern in ``embed/provider.py`` (``MissingEmbeddingBackend`` /
``get_default_provider() -> None``):

- ``has_pdf()`` / ``has_ocr()`` return a bool — callers that can skip a modality
  use these to degrade gracefully.
- ``require_pdf()`` / ``require_ocr()`` raise :class:`MissingModalityBackend` with
  an actionable install hint, used by the ingestion pipelines that strictly need
  the backend.

Only the probe lives here; the actual imports and skip-with-message wiring live in
the ingestion pipelines.
"""

from __future__ import annotations

import importlib


class MissingModalityBackend(RuntimeError):
    """Raised when an optional cross-modal backend is required but not installed."""


def _module_available(module_name: str) -> bool:
    """Return True if ``module_name`` can be imported. Monkeypatch-friendly."""
    try:
        importlib.import_module(module_name)
    except ImportError:
        return False
    return True


def has_pdf() -> bool:
    """True when the PDF extraction backend (``[pdf]`` extra) is installed."""
    return _module_available("pypdf")


def has_ocr() -> bool:
    """True when the OCR backend (``[ocr]`` extra) is installed."""
    return _module_available("pytesseract") and _module_available("PIL")


def require_pdf() -> None:
    if not has_pdf():
        raise MissingModalityBackend(
            'PDF backend not installed. Run: pip install -e ".[pdf]"'
        )


def require_ocr() -> None:
    if not has_ocr():
        raise MissingModalityBackend(
            'OCR backend not installed. Run: pip install -e ".[ocr]"'
        )
