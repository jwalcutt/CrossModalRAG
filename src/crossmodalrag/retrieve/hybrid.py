from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from crossmodalrag.embed.provider import EmbeddingProvider, get_default_provider
from crossmodalrag.retrieve import lexical
from crossmodalrag.retrieve.lexical import RetrievalHit
from crossmodalrag.retrieve.vector import has_vectors_for_model, vector_retrieve


# Profile weights: (vector, lexical, recency). Must each sum to 1.0.
PROFILE_WEIGHTS: dict[str, tuple[float, float, float]] = {
    "balanced": (0.55, 0.30, 0.15),
    "relevant": (0.70, 0.25, 0.05),
    "recent": (0.35, 0.20, 0.45),
}

DEFAULT_PROFILE = "balanced"


def retrieve(
    conn: sqlite3.Connection,
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    provider: EmbeddingProvider | None = None,
    restrict_chunk_ids: set[int] | None = None,
) -> list[RetrievalHit]:
    """Hybrid retrieval blending semantic, lexical, and recency signals.

    Falls back to pure lexical+recency retrieval when no embedding provider is
    available or no stored vectors match the active model. ``restrict_chunk_ids``,
    when given, limits scoring to that chunk set (used for level-targeted
    drill-down retrieval).
    """
    if profile not in PROFILE_WEIGHTS:
        raise ValueError(
            f"Unknown profile '{profile}'. Choose from: {', '.join(sorted(PROFILE_WEIGHTS))}."
        )

    provider = provider or get_default_provider()
    if provider is None or not has_vectors_for_model(conn, provider.name):
        # No semantic signal available — preserve the established lexical behavior.
        return lexical.retrieve(conn, query=query, top_k=top_k, restrict_chunk_ids=restrict_chunk_ids)

    query_tokens = lexical.tokenize(query)
    query_vector = provider.embed([query])[0]
    # Pull a generous vector candidate pool so good semantic matches survive blending.
    vector_sims = vector_retrieve(conn, query_vector, model=provider.name, top_k=max(top_k * 10, 50))

    w_vec, w_lex, w_rec = PROFILE_WEIGHTS[profile]
    now = datetime.now(timezone.utc)

    rows = conn.execute(
        """
        SELECT
            c.id as chunk_id,
            c.source_id as source_id,
            c.chunk_index as chunk_index,
            c.chunk_text as chunk_text,
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
        score = (w_vec * vec_norm) + (w_lex * lex) + (w_rec * recency)
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
            )
        )

    hits.sort(key=lambda hit: hit.score, reverse=True)
    return hits[:top_k]
