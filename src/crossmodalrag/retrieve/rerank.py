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
DEFAULT_MAX_CHUNKS_PER_SOURCE = 2


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


def get_max_chunks_per_source() -> int:
    """Max chunks one source may occupy in a retrieval result. Default 2; 0 disables."""
    raw = os.getenv("CMRAG_MAX_CHUNKS_PER_SOURCE", str(DEFAULT_MAX_CHUNKS_PER_SOURCE)).strip()
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_MAX_CHUNKS_PER_SOURCE


def get_dedupe_threshold() -> float:
    raw = os.getenv("CMRAG_DEDUPE_THRESHOLD", str(DEFAULT_DEDUPE_THRESHOLD)).strip()
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_DEDUPE_THRESHOLD


def cap_hits_per_source(hits: list[RetrievalHit], cap: int | None = None) -> list[RetrievalHit]:
    """Keep at most ``cap`` chunks per source, preserving score order.

    Chunks of one source now share a title/heading context line, so a source whose
    title matches the query can flood the whole top-k with its own chunks and crowd
    every other source out of the evidence list. Capping keeps the best chunks of
    the winning source while leaving room for the rest of the corpus — it never
    demotes a source's *best* chunk, so source-level recall/MRR can only improve.
    ``cap`` defaults to ``CMRAG_MAX_CHUNKS_PER_SOURCE``; 0 disables capping.
    """
    if cap is None:
        cap = get_max_chunks_per_source()
    if cap <= 0:
        return hits
    counts: dict[int, int] = {}
    kept: list[RetrievalHit] = []
    for hit in hits:
        seen = counts.get(hit.source_id, 0)
        if seen >= cap:
            continue
        counts[hit.source_id] = seen + 1
        kept.append(hit)
    return kept


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
