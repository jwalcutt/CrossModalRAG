from __future__ import annotations

from crossmodalrag.generate.synthesize import GeneratedAnswer
from crossmodalrag.modality import Locator, format_locator, parse_locator
from crossmodalrag.retrieve.lexical import RetrievalHit


def _locator(hit: RetrievalHit) -> Locator | None:
    return parse_locator(hit.chunk_metadata_json)


def _provenance_str(hit: RetrievalHit) -> str:
    """Human-readable provenance: modality + a citable locator (+ OCR confidence)."""
    loc = _locator(hit)
    rendered = format_locator(hit.source_uri, loc)
    parts: list[str] = []
    if loc is not None and loc.modality:
        parts.append(loc.modality)
    parts.append(f"uri={rendered}")
    if loc is not None and loc.ocr_confidence is not None:
        parts.append(f"ocr_conf={loc.ocr_confidence:.2f}")
    return " ".join(parts)


def format_grounded_answer(query: str, hits: list[RetrievalHit], explain: bool = False) -> str:
    if not hits:
        return (
            f'Query: "{query}"\n'
            "No supporting evidence found in the current memory store.\n"
            "Try broadening terms or ingesting more sources."
        )

    lines: list[str] = []
    lines.append(f'Query: "{query}"')
    lines.append("Evidence-grounded findings:")
    for i, hit in enumerate(hits, start=1):
        preview = _preview(hit.chunk_text)
        lines.append(
            (
                f"{i}. Claim: Potentially relevant context in {hit.source_type} "
                f"({hit.title or 'untitled'})."
            )
        )
        lines.append(
            f"   Evidence: source_id={hit.source_id}, chunk_id={hit.chunk_id}, {_provenance_str(hit)}"
        )
        if explain:
            lines.append(
                f"   Scores: combined={hit.score:.3f} "
                f"vector={hit.vector_score:.3f} "
                f"lexical={hit.lexical_score:.3f} "
                f"recency={hit.recency_score:.3f} "
                f"usage={hit.usage_score:.3f} "
                f"title={hit.title_score:.3f}"
            )
            if hit.subquery:
                lines.append(f"   Sub-query: {hit.subquery}")
        lines.append(f"   Excerpt: {preview}")
    return "\n".join(lines)


def format_generated_answer(gen: GeneratedAnswer, explain: bool = False, debug: bool = False) -> str:
    lines: list[str] = []
    lines.append(f'Query: "{gen.query}"')
    lines.append(f"Model: {gen.model}")
    if gen.abstained:
        # Distinguish gate refusals from model refusals, and show the top retrieval
        # score so "abstained despite strong evidence" is visible, not hidden.
        top_score = gen.evidence[0].score if gen.evidence else 0.0
        reason = gen.abstention_reason or "insufficient evidence"
        lines.append(f"Status: abstained ({reason}; top retrieval score {top_score:.3f})")
    lines.append("")
    lines.append("Answer:")
    lines.append(gen.answer_text)

    if gen.invalid_citations:
        lines.append("")
        lines.append(
            "Warning: answer cited unknown evidence ids "
            f"(ignored): {', '.join(gen.invalid_citations)}"
        )

    if gen.evidence:
        lines.append("")
        lines.append("Evidence:")
        for eid, hit in _ordered_evidence(gen):
            marker = "*" if eid in gen.cited_evidence_ids else " "
            lines.append(
                f" [{eid}]{marker} {hit.source_type} ({hit.title or 'untitled'}) "
                f"chunk_id={hit.chunk_id} {_provenance_str(hit)}"
            )
            if explain or debug:
                lines.append(
                    f"      scores: combined={hit.score:.3f} vector={hit.vector_score:.3f} "
                    f"lexical={hit.lexical_score:.3f} recency={hit.recency_score:.3f} "
                    f"usage={hit.usage_score:.3f} title={hit.title_score:.3f}"
                )
                if hit.subquery:
                    lines.append(f"      sub-query: {hit.subquery}")
            lines.append(f"      excerpt: {_preview(hit.chunk_text)}")

    if debug:
        lines.append("")
        lines.append("--- debug: prompt ---")
        lines.append(gen.raw_prompt)
        lines.append("--- debug: raw model output ---")
        lines.append(gen.raw_output)

    return "\n".join(lines)


def generated_answer_to_dict(gen: GeneratedAnswer, *, total_seconds: float | None = None) -> dict:
    """Stable JSON contract for `mem ask --json`. Keep field names backward-compatible.

    ``total_seconds`` is the caller-measured wall clock for the whole ask
    (retrieval + synthesis); ``generation_seconds`` is the LLM call alone.
    Additive `timing` block — existing keys are unchanged.
    """
    return {
        "query": gen.query,
        "model": gen.model,
        "abstained": gen.abstained,
        "abstention_reason": gen.abstention_reason,
        "answer": gen.answer_text,
        "timing": {
            "total_seconds": _round_seconds(total_seconds),
            "generation_seconds": _round_seconds(gen.generation_seconds),
        },
        "cited_evidence_ids": list(gen.cited_evidence_ids),
        "invalid_citations": list(gen.invalid_citations),
        "evidence": [
            {
                "evidence_id": eid,
                "cited": eid in gen.cited_evidence_ids,
                "source_id": hit.source_id,
                "chunk_id": hit.chunk_id,
                "source_type": hit.source_type,
                "source_uri": hit.source_uri,
                "title": hit.title,
                # Additive cross-modal provenance (step 4). Existing keys above are unchanged.
                "modality": _modality_of(hit),
                "locator": format_locator(hit.source_uri, _locator(hit)),
                "page": _page_of(hit),
                "ocr_confidence": _ocr_conf_of(hit),
                "subquery": hit.subquery,
                "scores": {
                    "combined": hit.score,
                    "vector": hit.vector_score,
                    "lexical": hit.lexical_score,
                    "recency": hit.recency_score,
                    "usage": hit.usage_score,
                    "title": hit.title_score,
                },
                "excerpt": _preview(hit.chunk_text),
            }
            for eid, hit in _ordered_evidence(gen)
        ],
    }


def template_answer_to_dict(
    query: str, hits: list[RetrievalHit], *, total_seconds: float | None = None
) -> dict:
    """No-LLM evidence-template payload (same shape `mem ask --no-llm --json` emits).

    The deterministic counterpart to ``generated_answer_to_dict``: lists the retrieved evidence with
    provenance (modality + locator) but no synthesized answer or citations. Stable/additive contract.
    """
    return {
        "query": query,
        "model": None,
        "abstained": not hits,
        # No LLM in this path: the only abstention cause is empty retrieval.
        "abstention_reason": None if hits else "weak_retrieval",
        "answer": None,
        "timing": {
            "total_seconds": _round_seconds(total_seconds),
            "generation_seconds": None,
        },
        "evidence": [
            {
                "evidence_id": f"E{i}",
                "source_id": hit.source_id,
                "chunk_id": hit.chunk_id,
                "source_type": hit.source_type,
                "source_uri": hit.source_uri,
                "title": hit.title,
                "modality": _modality_of(hit),
                "locator": format_locator(hit.source_uri, _locator(hit)),
                "page": _page_of(hit),
                "ocr_confidence": _ocr_conf_of(hit),
                "scores": {
                    "combined": hit.score,
                    "vector": hit.vector_score,
                    "lexical": hit.lexical_score,
                    "recency": hit.recency_score,
                    "usage": hit.usage_score,
                },
            }
            for i, hit in enumerate(hits, start=1)
        ],
    }


def _round_seconds(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


def _modality_of(hit: RetrievalHit) -> str | None:
    loc = _locator(hit)
    return loc.modality if loc is not None else None


def _page_of(hit: RetrievalHit) -> int | None:
    loc = _locator(hit)
    return loc.page if loc is not None else None


def _ocr_conf_of(hit: RetrievalHit) -> float | None:
    loc = _locator(hit)
    return loc.ocr_confidence if loc is not None else None


def _ordered_evidence(gen: GeneratedAnswer) -> list[tuple[str, RetrievalHit]]:
    if gen.id_map:
        return [(eid, gen.id_map[eid]) for eid in sorted(gen.id_map, key=lambda e: int(e[1:]))]
    return [(f"E{i}", hit) for i, hit in enumerate(gen.evidence, start=1)]


def _preview(text: str, max_chars: int = 220) -> str:
    flat = " ".join(text.split())
    if len(flat) <= max_chars:
        return flat
    return f"{flat[:max_chars].rstrip()}..."

