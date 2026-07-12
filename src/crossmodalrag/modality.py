"""Modality taxonomy + locator contract for cross-modal evidence.

This module is the single source of truth for how an ``evidence_chunks`` row
declares *what kind of evidence it is* and *where in its source it came from*.
It is deliberately storage-light: the contract lives inside the existing
``evidence_chunks.metadata_json`` free-form JSON column, so no schema change is
needed. Today's text/code ingestion already stamps ``{"modality": ...}`` there
(see ``ingest/notes.py`` and ``ingest/git.py``); cross-modal ingestion (Phase 3
steps 2-3) and citation rendering (step 4) reuse the helpers here.

Contract (stable; additive only):

- ``modality``     one of :data:`MODALITIES`.
- ``source_type``  the ingest connector that produced the chunk (note/git/pdf/image).
- ``page``         1-based page number for paged sources (PDF). Optional.
- ``region``       a human-checkable region within the source (e.g. an OCR bbox
                   rendered as a string, or a figure label). Optional.
- ``figure_index`` 0-based index of an embedded figure on a page. Optional.
- ``ocr_confidence`` mean OCR confidence in [0, 1] for derived text. Optional.

A *locator* is the subset that answers "where in the source": page / region /
figure. Provenance requires every cross-modal chunk to carry at least a citable
source URI; a finer locator is added when the extractor can supply one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# --- Modality taxonomy --------------------------------------------------------

MODALITY_TEXT = "text"          # plain prose (markdown notes)
MODALITY_CODE = "code"          # source code / diffs
MODALITY_OCR = "ocr"            # text recognized from an image
MODALITY_PDF_PAGE = "pdf-page"  # text extracted from a PDF page
MODALITY_CAPTION = "caption"    # generated/derived description of an image

MODALITIES: frozenset[str] = frozenset(
    {
        MODALITY_TEXT,
        MODALITY_CODE,
        MODALITY_OCR,
        MODALITY_PDF_PAGE,
        MODALITY_CAPTION,
    }
)


@dataclass(frozen=True)
class Locator:
    """Where a chunk came from inside its source (the citable "where")."""

    modality: str
    page: int | None = None
    region: str | None = None
    figure_index: int | None = None
    ocr_confidence: float | None = None


def build_chunk_metadata(
    *,
    modality: str,
    source_type: str,
    page: int | None = None,
    region: str | None = None,
    figure_index: int | None = None,
    ocr_confidence: float | None = None,
    **extra: object,
) -> dict[str, object]:
    """Canonical writer for ``evidence_chunks.metadata_json`` payloads.

    ``None`` locator fields are omitted so unchanged content yields a stable,
    minimal dict (supports the deterministic-ingestion non-negotiable). Unknown
    ``extra`` keys are preserved for connector-specific fields (e.g. a git sha).
    """
    if modality not in MODALITIES:
        raise ValueError(
            f"Unknown modality {modality!r}. Choose from: {', '.join(sorted(MODALITIES))}."
        )
    meta: dict[str, object] = {"modality": modality, "source_type": source_type}
    if page is not None:
        meta["page"] = int(page)
    if region is not None:
        meta["region"] = str(region)
    if figure_index is not None:
        meta["figure_index"] = int(figure_index)
    if ocr_confidence is not None:
        meta["ocr_confidence"] = float(ocr_confidence)
    for key, value in extra.items():
        if value is not None:
            meta[key] = value
    return meta


def parse_locator(metadata_json: str | None) -> Locator | None:
    """Tolerant reader: return a :class:`Locator` or ``None`` if absent/invalid.

    Mirrors the defensive ``json.loads``/``except JSONDecodeError`` pattern used
    in ``memory/episodes.py`` and ``memory/extract.py`` so a malformed or legacy
    metadata blob never raises in a retrieval/citation path.
    """
    if not metadata_json:
        return None
    try:
        meta = json.loads(metadata_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(meta, dict):
        return None
    modality = meta.get("modality")
    if not isinstance(modality, str) or not modality:
        return None
    return Locator(
        modality=modality,
        page=_as_int(meta.get("page")),
        region=_as_str(meta.get("region")),
        figure_index=_as_int(meta.get("figure_index")),
        ocr_confidence=_as_float(meta.get("ocr_confidence")),
    )


def format_locator(source_uri: str, locator: Locator | None) -> str:
    """Render a human-checkable citation target.

    Falls back to the bare ``source_uri`` when no finer locator is available, so
    a citation never points to less than "which source". Examples:
    ``spec.pdf p.4`` / ``diagram.png [region: top-left]``.
    """
    if locator is None:
        return source_uri
    parts: list[str] = []
    if locator.page is not None:
        parts.append(f"p.{locator.page}")
    if locator.figure_index is not None:
        parts.append(f"fig.{locator.figure_index}")
    if locator.region:
        parts.append(f"[region: {locator.region}]")
    if not parts:
        return source_uri
    return f"{source_uri} " + " ".join(parts)


def _as_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None
