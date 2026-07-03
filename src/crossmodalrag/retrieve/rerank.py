"""Cross-modal retrieval post-processing: modality→source_type filtering + dedupe.

Step 4 deliverables that are concrete and testable (per the eval-before-complexity
non-negotiable). Per-modality *weight tuning* is deliberately deferred until a real
cross-modal corpus + eval can justify it.

The user-facing ``--modality`` filter keys off ``source_type`` (the consistently
populated field), not the heterogeneous chunk ``modality`` string (notes write
``text``, git ``code+text``, pdf ``pdf-page``, image ``ocr``).
"""

from __future__ import annotations

import os

from crossmodalrag.retrieve.lexical import (
    RetrievalHit,
    lexical_overlap_score,
    tokenize,
)

# User-facing modality term -> the source_type(s) it selects.
MODALITY_SOURCE_TYPES: dict[str, set[str]] = {
    "text": {"note"},
    "code": {"git_commit"},
    "pdf": {"pdf"},
    "image": {"image"},
}

DEFAULT_DEDUPE_THRESHOLD = 0.95


def resolve_source_types(modalities: list[str] | None) -> set[str] | None:
    """Map ``--modality`` terms to a set of source_types (None = no restriction)."""
    if not modalities:
        return None
    resolved: set[str] = set()
    for modality in modalities:
        types = MODALITY_SOURCE_TYPES.get(modality)
        if types is None:
            raise ValueError(
                f"Unknown modality '{modality}'. Choose from: {', '.join(sorted(MODALITY_SOURCE_TYPES))}."
            )
        resolved |= types
    return resolved


def get_dedupe_threshold() -> float:
    raw = os.getenv("CMRAG_DEDUPE_THRESHOLD", str(DEFAULT_DEDUPE_THRESHOLD)).strip()
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_DEDUPE_THRESHOLD


def dedupe_hits(
    hits: list[RetrievalHit], threshold: float | None = None, max_kept: int | None = None
) -> list[RetrievalHit]:
    """Drop near-identical duplicate evidence, keeping the higher-scored hit.

    Collapses the same content re-ingested across modalities (e.g. a screenshot of
    a note alongside the note). ``hits`` must already be score-sorted (descending);
    a candidate is dropped when its token overlap with an already-kept hit is
    ``>= threshold`` (conservative default 0.95).

    ``max_kept`` stops the scan once that many hits survive. Because hits are
    processed in score order, the result is identical to deduping everything and
    slicing — but bounds the work to O(max_kept * n) instead of O(n^2), which
    matters when the caller passes the full scored candidate pool.
    """
    if threshold is None:
        threshold = get_dedupe_threshold()
    kept: list[RetrievalHit] = []
    kept_tokens: list[list[str]] = []
    for hit in hits:
        if max_kept is not None and len(kept) >= max_kept:
            break
        tokens = tokenize(hit.chunk_text)
        if any(lexical_overlap_score(tokens, kt) >= threshold for kt in kept_tokens):
            continue
        kept.append(hit)
        kept_tokens.append(tokens)
    return kept
