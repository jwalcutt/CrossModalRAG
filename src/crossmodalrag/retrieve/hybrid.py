from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from crossmodalrag.embed.provider import EmbeddingProvider, get_default_provider
from crossmodalrag.retrieve import lexical
from crossmodalrag.retrieve.lexical import RetrievalHit
from crossmodalrag.retrieve.vector import has_vectors_for_model, vector_retrieve


# Profile weights: (vector, lexical, recency, usage). Each sums to 1.0. The `usage` term
# (rehearsal strength) is 0 for every profile except `usage`, so existing profiles are
# byte-identical to before — usage-aware ranking is strictly opt-in via `--profile usage`.
PROFILE_WEIGHTS: dict[str, tuple[float, float, float, float]] = {
    "balanced": (0.55, 0.30, 0.15, 0.0),
    "relevant": (0.70, 0.25, 0.05, 0.0),
    "recent": (0.35, 0.20, 0.45, 0.0),
    "usage": (0.55, 0.25, 0.05, 0.15),
}

DEFAULT_PROFILE = "balanced"


def retrieve(
    conn: sqlite3.Connection,
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    provider: EmbeddingProvider | None = None,
    restrict_chunk_ids: set[int] | None = None,
    restrict_source_types: set[str] | None = None,
    now: datetime | None = None,
    decompose: bool = True,
) -> list[RetrievalHit]:
    """Hybrid retrieval blending semantic, lexical, and recency signals.

    Falls back to pure lexical+recency retrieval when no embedding provider is
    available or no stored vectors match the active model. ``restrict_chunk_ids``,
    when given, limits scoring to that chunk set (used for level-targeted
    drill-down retrieval); ``restrict_source_types`` limits to a modality's
    source_type(s) (the ``--modality`` filter).

    A comparative query ("difference between X and Y", "X vs Y") is retrieved as
    two sub-queries with reserved slots per side, so the better-represented
    subject cannot crowd the other out of the evidence entirely. Applies only to
    top-level retrieval (not drill-down); non-matching queries are unaffected.
    """
    if profile not in PROFILE_WEIGHTS:
        raise ValueError(
            f"Unknown profile '{profile}'. Choose from: {', '.join(sorted(PROFILE_WEIGHTS))}."
        )

    if decompose and restrict_chunk_ids is None:
        from crossmodalrag.retrieve.decompose import merge_side_hits, split_comparative_query

        comp = split_comparative_query(query)
        if comp is not None:
            def _sub_retrieve(subquery: str, stamp: bool = True) -> list[RetrievalHit]:
                hits = retrieve(
                    conn,
                    query=subquery,
                    top_k=top_k,
                    profile=profile,
                    provider=provider,
                    restrict_source_types=restrict_source_types,
                    now=now,
                    decompose=False,
                )
                if stamp:
                    for hit in hits:
                        hit.subquery = subquery
                return hits

            # Full-query pass keeps joint-context ranking (a source covering both
            # subjects); per-side passes guarantee each subject representation.
            return merge_side_hits(
                _sub_retrieve(comp.left),
                _sub_retrieve(comp.right),
                top_k,
                full_hits=_sub_retrieve(query, stamp=False),
            )

    provider = provider or get_default_provider()
    if provider is None or not has_vectors_for_model(conn, provider.name):
        # No semantic signal available — preserve the established lexical behavior.
        return lexical.retrieve(
            conn,
            query=query,
            top_k=top_k,
            restrict_chunk_ids=restrict_chunk_ids,
            restrict_source_types=restrict_source_types,
        )

    query_tokens = lexical.tokenize(query)
    query_vector = provider.embed([query])[0]
    # Pull a generous vector candidate pool so good semantic matches survive blending.
    vector_sims = vector_retrieve(conn, query_vector, model=provider.name, top_k=max(top_k * 10, 50))

    w_vec, w_lex, w_rec, w_usage = PROFILE_WEIGHTS[profile]
    now = now or datetime.now(timezone.utc)

    # Title boost: a small additive bonus for query overlap with the source *title*,
    # profile-independent (a note named for the query's terms should win near-ties
    # against incidental mentions in diffs). 0 disables it.
    from crossmodalrag.config import get_title_boost_weight

    w_title = get_title_boost_weight()
    title_tokens_cache: dict[str, list[str]] = {}

    # Usage (rehearsal strength) is loaded only when the profile asks for it (opt-in).
    usage_strengths: dict[int, float] = {}
    if w_usage > 0:
        from crossmodalrag.config import get_usage_halflife_days
        from crossmodalrag.usage.store import usage_summaries

        summaries = usage_summaries(conn, now=now, halflife_days=get_usage_halflife_days())
        usage_strengths = {
            target_id: summary.strength
            for (kind, target_id), summary in summaries.items()
            if kind == "chunk"
        }

    rows = conn.execute(
        """
        SELECT
            c.id as chunk_id,
            c.source_id as source_id,
            c.chunk_index as chunk_index,
            c.chunk_text as chunk_text,
            c.metadata_json as chunk_metadata_json,
            s.source_type as source_type,
            s.source_uri as source_uri,
            s.timestamp as source_timestamp,
            s.title as title
        FROM evidence_chunks c
        JOIN sources s ON s.id = c.source_id
        """
    ).fetchall()

    hits: list[RetrievalHit] = []
    for row in rows:
        chunk_id = int(row["chunk_id"])
        if restrict_chunk_ids is not None and chunk_id not in restrict_chunk_ids:
            continue
        if restrict_source_types is not None and str(row["source_type"]) not in restrict_source_types:
            continue
        cosine = vector_sims.get(chunk_id)
        lex = (
            lexical.lexical_overlap_score(query_tokens, lexical.tokenize(str(row["chunk_text"])))
            if query_tokens
            else 0.0
        )
        if cosine is None and lex <= 0:
            continue

        vec_norm = ((cosine + 1.0) / 2.0) if cosine is not None else 0.0
        recency = lexical.recency_score(row["source_timestamp"], now=now)
        # Usage only re-ranks candidates that already have semantic/lexical signal (the
        # `continue` guard above) — it can never surface an irrelevant item, which also
        # bounds the retrieve->boost->retrieve feedback loop.
        usage_norm = 0.0
        if w_usage > 0:
            from crossmodalrag.config import get_usage_saturation
            from crossmodalrag.usage.strength import normalize_strength

            usage_norm = normalize_strength(
                usage_strengths.get(chunk_id, 0.0), saturation=get_usage_saturation()
            )
        title_lex = lexical.title_overlap(query_tokens, row["title"], title_tokens_cache)
        score = (
            (w_vec * vec_norm)
            + (w_lex * lex)
            + (w_rec * recency)
            + (w_usage * usage_norm)
            + (w_title * title_lex)
        )
        hits.append(
            RetrievalHit(
                chunk_id=chunk_id,
                source_id=int(row["source_id"]),
                source_type=str(row["source_type"]),
                source_uri=str(row["source_uri"]),
                source_timestamp=row["source_timestamp"],
                title=row["title"],
                chunk_index=int(row["chunk_index"]),
                chunk_text=str(row["chunk_text"]),
                score=score,
                lexical_score=lex,
                recency_score=recency,
                vector_score=vec_norm,
                chunk_metadata_json=row["chunk_metadata_json"],
                usage_score=usage_norm,
                title_score=title_lex,
            )
        )

    hits.sort(key=lambda hit: hit.score, reverse=True)
    from crossmodalrag.retrieve.rerank import cap_hits_per_source, dedupe_hits

    # Source-diversity cap applies only to open retrieval: drill-down
    # (restrict_chunk_ids) deliberately ranks within one node's evidence,
    # which is often a single source.
    if restrict_chunk_ids is None:
        hits = cap_hits_per_source(hits)
    # max_kept bounds dedupe to O(top_k * n): the candidate pool here is every chunk
    # with any signal, and unbounded pairwise dedupe over it dominated ask latency.
    return dedupe_hits(hits, max_kept=top_k)
