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
    source_coverage: float
    abstention_correct: bool
    cited_evidence_ids: list[str]
    invalid_citations: list[str]


@dataclass(frozen=True)
class GenerationEvalSummary:
    query_count: int
    top_k: int
    profile: str
    level: str
    model: str
    citation_validity: float
    source_grounding_hit: float
    source_coverage: float
    abstention_correct: float
    results: list[GenerationEvalResult]


def run_generation_eval(
    conn: sqlite3.Connection,
    provider: LLMProvider,
    *,
    top_k: int = 5,
    query_prefix: str | None = None,
    profile: str = DEFAULT_PROFILE,
    level: str = "evidence",
) -> GenerationEvalSummary:
    queries = list_eval_queries(conn, query_prefix=query_prefix)
    results: list[GenerationEvalResult] = []

    for query in queries:
        answerable = bool(query.expected_source_uris)
        hits = _retrieve_for_level(conn, query.query_text, top_k=top_k, profile=profile, level=level)
        gen = synthesize_answer(query.query_text, hits, provider)

        expected = set(query.expected_source_uris)
        results.append(
            GenerationEvalResult(
                query_text=query.query_text,
                answerable=answerable,
                abstained=gen.abstained,
                citation_valid=_citation_valid(gen),
                source_grounded=_source_grounded(gen, expected),
                source_coverage=_source_coverage(gen, expected),
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
            level=level,
            model=provider.name,
            citation_validity=0.0,
            source_grounding_hit=0.0,
            source_coverage=0.0,
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
        level=level,
        model=provider.name,
        citation_validity=sum(1 for r in results if r.citation_valid) / n,
        # Grounding/coverage only make sense for answerable queries (others should abstain).
        source_grounding_hit=sum(1 for r in answerable_results if r.source_grounded) / grounding_denom,
        source_coverage=sum(r.source_coverage for r in answerable_results) / grounding_denom,
        abstention_correct=sum(1 for r in results if r.abstention_correct) / n,
        results=results,
    )


def _retrieve_for_level(
    conn: sqlite3.Connection,
    query_text: str,
    *,
    top_k: int,
    profile: str,
    level: str,
):
    """Retrieve L0 evidence hits, optionally scoped via a memory level's drill-down.

    Mirrors ``cli.ask_cmd``: at a memory level, retrieve nodes, drill them down to
    their L0 candidate chunks, then rank those chunks. Synthesis stays grounded in
    (and cites) L0 regardless of entry level. Provider resolution happens inside
    ``retrieve_nodes``/``retrieve`` (lexical fallback when no embeddings).
    """
    if level == "evidence":
        return retrieve(conn, query=query_text, top_k=top_k, profile=profile)

    from crossmodalrag.retrieve.nodes import candidate_chunk_ids, retrieve_nodes

    node_hits = retrieve_nodes(conn, query_text, level=level, top_k=top_k, profile=profile)
    chunk_ids = candidate_chunk_ids(conn, node_hits)
    if not chunk_ids:
        return []
    return retrieve(
        conn, query=query_text, top_k=top_k, profile=profile, restrict_chunk_ids=chunk_ids
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


def _source_coverage(gen: GeneratedAnswer, expected: set[str]) -> float:
    """Fraction of an answerable query's expected gold sources the answer cites.

    Unlike the binary ``_source_grounded``, this rewards aggregating multiple
    sources — the benefit synthesis (memory-level) queries are meant to capture.
    """
    if not expected or gen.abstained:
        return 0.0
    cited_uris = {
        hit.source_uri
        for eid in gen.cited_evidence_ids
        if (hit := gen.id_map.get(eid)) is not None
    }
    return len(cited_uris & expected) / len(expected)
