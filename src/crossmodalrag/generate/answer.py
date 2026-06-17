from __future__ import annotations

from crossmodalrag.generate.synthesize import GeneratedAnswer
from crossmodalrag.retrieve.lexical import RetrievalHit


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
            f"   Evidence: source_id={hit.source_id}, chunk_id={hit.chunk_id}, uri={hit.source_uri}"
        )
        if explain:
            lines.append(
                f"   Scores: combined={hit.score:.3f} "
                f"vector={hit.vector_score:.3f} "
                f"lexical={hit.lexical_score:.3f} "
                f"recency={hit.recency_score:.3f}"
            )
        lines.append(f"   Excerpt: {preview}")
    return "\n".join(lines)


def format_generated_answer(gen: GeneratedAnswer, explain: bool = False, debug: bool = False) -> str:
    lines: list[str] = []
    lines.append(f'Query: "{gen.query}"')
    lines.append(f"Model: {gen.model}")
    if gen.abstained:
        lines.append("Status: abstained (insufficient evidence)")
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
                f"chunk_id={hit.chunk_id} uri={hit.source_uri}"
            )
            if explain or debug:
                lines.append(
                    f"      scores: combined={hit.score:.3f} vector={hit.vector_score:.3f} "
                    f"lexical={hit.lexical_score:.3f} recency={hit.recency_score:.3f}"
                )
            lines.append(f"      excerpt: {_preview(hit.chunk_text)}")

    if debug:
        lines.append("")
        lines.append("--- debug: prompt ---")
        lines.append(gen.raw_prompt)
        lines.append("--- debug: raw model output ---")
        lines.append(gen.raw_output)

    return "\n".join(lines)


def generated_answer_to_dict(gen: GeneratedAnswer) -> dict:
    """Stable JSON contract for `mem ask --json`. Keep field names backward-compatible."""
    return {
        "query": gen.query,
        "model": gen.model,
        "abstained": gen.abstained,
        "answer": gen.answer_text,
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
                "scores": {
                    "combined": hit.score,
                    "vector": hit.vector_score,
                    "lexical": hit.lexical_score,
                    "recency": hit.recency_score,
                },
                "excerpt": _preview(hit.chunk_text),
            }
            for eid, hit in _ordered_evidence(gen)
        ],
    }


def _ordered_evidence(gen: GeneratedAnswer) -> list[tuple[str, RetrievalHit]]:
    if gen.id_map:
        return [(eid, gen.id_map[eid]) for eid in sorted(gen.id_map, key=lambda e: int(e[1:]))]
    return [(f"E{i}", hit) for i, hit in enumerate(gen.evidence, start=1)]


def _preview(text: str, max_chars: int = 220) -> str:
    flat = " ".join(text.split())
    if len(flat) <= max_chars:
        return flat
    return f"{flat[:max_chars].rstrip()}..."

