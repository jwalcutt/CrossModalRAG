from __future__ import annotations

import json

import pytest

from crossmodalrag.modality import (
    MODALITIES,
    MODALITY_OCR,
    MODALITY_PDF_PAGE,
    MODALITY_TEXT,
    Locator,
    build_chunk_metadata,
    format_locator,
    parse_locator,
)


def test_build_chunk_metadata_round_trips_through_parse_locator():
    meta = build_chunk_metadata(
        modality=MODALITY_PDF_PAGE,
        source_type="pdf",
        page=4,
        figure_index=1,
        ocr_confidence=0.92,
    )

    locator = parse_locator(json.dumps(meta))

    assert locator == Locator(
        modality=MODALITY_PDF_PAGE,
        page=4,
        region=None,
        figure_index=1,
        ocr_confidence=0.92,
    )


def test_build_chunk_metadata_omits_none_locator_fields():
    # Determinism: unchanged content must yield a stable, minimal dict.
    meta = build_chunk_metadata(modality=MODALITY_TEXT, source_type="note")

    assert meta == {"modality": MODALITY_TEXT, "source_type": "note"}


def test_build_chunk_metadata_preserves_connector_extra_fields():
    meta = build_chunk_metadata(
        modality="code",
        source_type="git_commit",
        sha="abc123",
        dropped=None,
    )

    assert meta["sha"] == "abc123"
    assert "dropped" not in meta  # None extras are omitted like locator fields


def test_build_chunk_metadata_rejects_unknown_modality():
    with pytest.raises(ValueError, match="Unknown modality"):
        build_chunk_metadata(modality="hologram", source_type="image")


def test_parse_locator_returns_none_for_missing_or_malformed():
    assert parse_locator(None) is None
    assert parse_locator("") is None
    assert parse_locator("{not valid json") is None
    assert parse_locator(json.dumps(["not", "a", "dict"])) is None
    assert parse_locator(json.dumps({"source_type": "note"})) is None  # no modality


def test_parse_locator_tolerates_garbage_locator_values():
    # A legacy/garbled blob must not raise in a retrieval/citation path.
    blob = json.dumps(
        {"modality": MODALITY_OCR, "page": "not-a-number", "ocr_confidence": "high"}
    )

    locator = parse_locator(blob)

    assert locator is not None
    assert locator.modality == MODALITY_OCR
    assert locator.page is None
    assert locator.ocr_confidence is None


def test_format_locator_renders_page_and_region():
    locator = Locator(modality=MODALITY_PDF_PAGE, page=4, region="top-left")

    rendered = format_locator("/abs/spec.pdf", locator)

    assert rendered == "/abs/spec.pdf p.4 [region: top-left]"


def test_format_locator_falls_back_to_bare_uri_without_finer_locator():
    assert format_locator("/abs/note.md", None) == "/abs/note.md"
    bare = Locator(modality=MODALITY_TEXT)
    assert format_locator("/abs/note.md", bare) == "/abs/note.md"


def test_modality_taxonomy_is_the_documented_set():
    assert MODALITIES == {"text", "code", "ocr", "pdf-page", "caption"}
