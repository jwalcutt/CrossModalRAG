from __future__ import annotations

from crossmodalrag.retrieve.lexical import RetrievalHit


def format_grounded_answer(query: str, hits: list[RetrievalHit]) -> str:
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
        lines.append(f"   Excerpt: {preview}")
    return "\n".join(lines)


def _preview(text: str, max_chars: int = 220) -> str:
    flat = " ".join(text.split())
    if len(flat) <= max_chars:
        return flat
    return f"{flat[:max_chars].rstrip()}..."

