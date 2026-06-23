"""Empirically validate the cross-modal eval fixtures against real OCR.

These assertions are what make the native-embedding gate *meaningful*: if the
visual diagram's OCR text could answer its query, OCR-text-first would retrieve
it and the gate would never fire (false HOLD); if the text screenshot's OCR text
did NOT contain its answer, the text slice would be understated. The fixtures are
designed for both invariants — this test freezes that so fixture drift (or a
tesseract behavior change) can't silently break the gate.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytesseract = pytest.importorskip("pytesseract")
PIL_Image = pytest.importorskip("PIL.Image")

try:
    pytesseract.get_tesseract_version()
except Exception:  # pragma: no cover - environment without the tesseract binary
    pytest.skip("tesseract binary not installed", allow_module_level=True)

DOCUMENTS = (
    Path(__file__).resolve().parent / "fixtures" / "sample_seed" / "vault" / "documents"
)

# Terms that would let OCR-text-first answer the VISUAL query — none may appear in
# the diagram's OCR text, or the visual slice would spuriously pass.
VISUAL_LEAK_TERMS = ["bottleneck", "red", "colour", "color", "stage", "middle", "highlight"]


def _ocr(name: str) -> str:
    with PIL_Image.open(DOCUMENTS / name) as img:
        return pytesseract.image_to_string(img.convert("RGB")).lower()


def test_diagram_is_ocr_resistant_for_the_visual_query():
    text = _ocr("architecture-diagram.png")
    leaked = [term for term in VISUAL_LEAK_TERMS if term in text]
    assert not leaked, (
        f"architecture-diagram.png OCR leaked visual answer term(s) {leaked}: {text!r}. "
        "Regenerate a more OCR-resistant fixture (scripts/generate_xmodal_fixtures.py) or the "
        "native-embedding gate can never fire."
    )


def test_screenshot_is_ocr_readable_for_the_text_query():
    text = _ocr("notes-screenshot.png")
    assert "reindex" in text
    assert "backfill" in text or "embeddings" in text
