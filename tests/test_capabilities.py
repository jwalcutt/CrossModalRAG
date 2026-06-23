from __future__ import annotations

import pytest

from crossmodalrag import capabilities
from crossmodalrag.capabilities import (
    MissingModalityBackend,
    has_ocr,
    has_pdf,
    require_ocr,
    require_pdf,
)


def test_has_pdf_false_when_backend_absent(monkeypatch):
    monkeypatch.setattr(capabilities, "_module_available", lambda name: False)
    assert has_pdf() is False


def test_has_pdf_true_when_backend_present(monkeypatch):
    monkeypatch.setattr(capabilities, "_module_available", lambda name: name == "pypdf")
    assert has_pdf() is True


def test_has_ocr_requires_both_pytesseract_and_pillow(monkeypatch):
    # Only pytesseract present (no Pillow) → OCR is not usable.
    monkeypatch.setattr(
        capabilities, "_module_available", lambda name: name == "pytesseract"
    )
    assert has_ocr() is False

    monkeypatch.setattr(
        capabilities,
        "_module_available",
        lambda name: name in {"pytesseract", "PIL"},
    )
    assert has_ocr() is True


def test_require_pdf_raises_actionable_error_when_absent(monkeypatch):
    monkeypatch.setattr(capabilities, "_module_available", lambda name: False)
    with pytest.raises(MissingModalityBackend, match=r'\[pdf\]'):
        require_pdf()


def test_require_ocr_raises_actionable_error_when_absent(monkeypatch):
    monkeypatch.setattr(capabilities, "_module_available", lambda name: False)
    with pytest.raises(MissingModalityBackend, match=r'\[ocr\]'):
        require_ocr()


def test_require_pdf_is_noop_when_present(monkeypatch):
    monkeypatch.setattr(capabilities, "_module_available", lambda name: True)
    require_pdf()  # should not raise
