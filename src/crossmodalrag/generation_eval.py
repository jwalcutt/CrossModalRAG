from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from crossmodalrag.evaluation import list_eval_queries
from crossmodalrag.generate.provider import LLMProvider
from crossmodalrag.generate.synthesize import GeneratedAnswer, synthesize_answer
from crossmodalrag.retrieve.hybrid import DEFAULT_PROFILE, retrieve


@dataclass(frozen=True)
class GenerationEvalResult:
    query_text: str
    answerable: bool
    abstained: bool
    citation_valid: bool
    source_grounded: bool
    abstention_correct: bool
    cited_evidence_ids: list[str]
    invalid_citations: list[str]


@dataclass(frozen=True)
class GenerationEvalSummary:
    query_count: int
    top_k: int
    profile: str
    model: str
    citation_validity: float
    source_grounding_hit: float
    abstention_correct: float
    results: list[GenerationEvalResult]


def run_generation_eval(
    conn: sqlite3.Connection,
    provider: LLMProvider,
    *,
    top_k: int = 5,
    query_prefix: str | None = None,
    profile: str = DEFAULT_PROFILE,
) -> GenerationEvalSummary:
    queries = list_eval_queries(conn, query_prefix=query_prefix)
    results: list[GenerationEvalResult] = []

    for query in queries:
        answerable = bool(query.expected_source_uris)
        hits = retrieve(conn, query=query.query_text, top_k=top_k, profile=profile)
        gen = synthesize_answer(query.query_text, hits, provider)

        results.append(
            GenerationEvalResult(
                query_text=query.query_text,
                answerable=answerable,
                abstained=gen.abstained,
                citation_valid=_citation_valid(gen),
                source_grounded=_source_grounded(gen, set(query.expected_source_uris)),
                abstention_correct=(gen.abstained != answerable),
                cited_evidence_ids=gen.cited_evidence_ids,
                invalid_citations=gen.invalid_citations,
            )
        )

    if not results:
        return GenerationEvalSummary(
            query_count=0,
            top_k=top_k,
            profile=profile,
            model=provider.name,
            citation_validity=0.0,
            source_grounding_hit=0.0,
            abstention_correct=0.0,
            results=[],
        )

    n = len(results)
    answerable_results = [r for r in results if r.answerable]
    grounding_denom = len(answerable_results) or 1
    return GenerationEvalSummary(
        query_count=n,
        top_k=top_k,
        profile=profile,
        model=provider.name,
        citation_validity=sum(1 for r in results if r.citation_valid) / n,
        # Grounding only makes sense for answerable queries (others should abstain).
        source_grounding_hit=sum(1 for r in answerable_results if r.source_grounded) / grounding_denom,
        abstention_correct=sum(1 for r in results if r.abstention_correct) / n,
        results=results,
    )


def _citation_valid(gen: GeneratedAnswer) -> bool:
    """An abstention is trivially valid; otherwise no hallucinated citation ids."""
    if gen.abstained:
        return True
    return not gen.invalid_citations


def _source_grounded(gen: GeneratedAnswer, expected: set[str]) -> bool:
    if not expected or gen.abstained:
        return False
    for eid in gen.cited_evidence_ids:
        hit = gen.id_map.get(eid)
        if hit is not None and hit.source_uri in expected:
            return True
    return False
