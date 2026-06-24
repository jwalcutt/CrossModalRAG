from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from crossmodalrag.embed.provider import EmbeddingProvider, get_default_provider
from crossmodalrag.embed.store import load_node_vectors
from crossmodalrag.memory.store import resolve_to_evidence
from crossmodalrag.retrieve import lexical

# Node-level profile weights: (vector, lexical, recency, centrality). Each sums to 1.0.
# `usage` mirrors `relevant` here: node-level usage is deferred (seeded usage is chunk-level,
# and chunk usage still applies on drill-down), so centrality remains the node importance term.
NODE_PROFILE_WEIGHTS: dict[str, tuple[float, float, float, float]] = {
    "balanced": (0.55, 0.25, 0.10, 0.10),
    "relevant": (0.70, 0.20, 0.05, 0.05),
    "recent": (0.30, 0.20, 0.40, 0.10),
    "usage": (0.70, 0.20, 0.05, 0.05),
}
DEFAULT_PROFILE = "balanced"

# Maps a CLI level name to (level, node_type).
LEVEL_TO_NODE: dict[str, tuple[int, str]] = {
    "event": (1, "event"),
    "episode": (2, "episode"),
    "concept": (3, "concept"),
}


@dataclass
class NodeHit:
    node_id: int
    level: int
    node_type: str
    title: str | None
    content: str | None
    time_start: str | None
    centrality: float
    score: float
    vector_score: float
    lexical_score: float
    recency_score: float
    centrality_score: float


def retrieve_nodes(
    conn: sqlite3.Connection,
    query: str,
    *,
    level: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    provider: EmbeddingProvider | None = None,
) -> list[NodeHit]:
    """Hybrid retrieval over memory nodes of a target level.

    Blends semantic (node embeddings), lexical (title+content), recency, and
    centrality. Vector is optional: with no node vectors it degrades to
    lexical+recency+centrality (no embeddings extra required).
    """
    if profile not in NODE_PROFILE_WEIGHTS:
        raise ValueError(
            f"Unknown profile '{profile}'. Choose from: {', '.join(sorted(NODE_PROFILE_WEIGHTS))}."
        )
    if level not in LEVEL_TO_NODE:
        raise ValueError(f"Unknown node level '{level}'. Choose from: {', '.join(LEVEL_TO_NODE)}.")

    node_level, node_type = LEVEL_TO_NODE[level]
    rows = conn.execute(
        """
        SELECT id, title, content, time_start, centrality
        FROM memory_nodes
        WHERE level = ? AND node_type = ?
        """,
        (node_level, node_type),
    ).fetchall()
    if not rows:
        return []

    query_tokens = lexical.tokenize(query)
    cosines = _query_cosines(conn, query, node_level, provider)

    w_vec, w_lex, w_rec, w_cen = NODE_PROFILE_WEIGHTS[profile]
    now = datetime.now(timezone.utc)

    hits: list[NodeHit] = []
    for row in rows:
        node_id = int(row["id"])
        cosine = cosines.get(node_id)
        text = f"{row['title'] or ''} {row['content'] or ''}"
        lex = lexical.lexical_overlap_score(query_tokens, lexical.tokenize(text)) if query_tokens else 0.0
        if cosine is None and lex <= 0:
            continue

        vec_norm = ((cosine + 1.0) / 2.0) if cosine is not None else 0.0
        recency = lexical.recency_score(row["time_start"], now=now)
        centrality = float(row["centrality"]) if row["centrality"] is not None else 0.0
        score = (w_vec * vec_norm) + (w_lex * lex) + (w_rec * recency) + (w_cen * centrality)
        hits.append(
            NodeHit(
                node_id=node_id,
                level=node_level,
                node_type=node_type,
                title=row["title"],
                content=row["content"],
                time_start=row["time_start"],
                centrality=centrality,
                score=score,
                vector_score=vec_norm,
                lexical_score=lex,
                recency_score=recency,
                centrality_score=centrality,
            )
        )

    hits.sort(key=lambda hit: (hit.score, hit.node_id), reverse=True)
    return hits[:top_k]


def candidate_chunk_ids(conn: sqlite3.Connection, node_hits: list[NodeHit]) -> set[int]:
    chunk_ids: set[int] = set()
    for hit in node_hits:
        chunk_ids.update(resolve_to_evidence(conn, hit.level, hit.node_id))
    return chunk_ids


def drilldown_source_uris(conn: sqlite3.Connection, node_hits: list[NodeHit]) -> list[str]:
    """Ordered-unique L0 source URIs reached by drilling the node hits (rank order)."""
    seen: set[str] = set()
    ordered: list[str] = []
    for hit in node_hits:
        chunk_ids = resolve_to_evidence(conn, hit.level, hit.node_id)
        if not chunk_ids:
            continue
        placeholders = ",".join("?" for _ in chunk_ids)
        rows = conn.execute(
            f"""
            SELECT DISTINCT s.source_uri AS uri
            FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
            WHERE c.id IN ({placeholders})
            """,
            tuple(chunk_ids),
        ).fetchall()
        for row in rows:
            uri = str(row["uri"])
            if uri not in seen:
                seen.add(uri)
                ordered.append(uri)
    return ordered


def _query_cosines(
    conn: sqlite3.Connection,
    query: str,
    node_level: int,
    provider: EmbeddingProvider | None,
) -> dict[int, float]:
    provider = provider or get_default_provider()
    if provider is None:
        return {}
    node_vectors = load_node_vectors(conn, provider.name, level=node_level)
    if not node_vectors:
        return {}

    import numpy as np

    ids = [nid for nid, _ in node_vectors]
    matrix = np.array([vec for _, vec in node_vectors], dtype=np.float32)
    query_vec = np.array(provider.embed([query])[0], dtype=np.float32)
    matrix_norms = np.linalg.norm(matrix, axis=1)
    query_norm = float(np.linalg.norm(query_vec))
    if query_norm == 0.0:
        return {}
    denom = matrix_norms * query_norm
    denom[denom == 0.0] = np.inf
    sims = (matrix @ query_vec) / denom
    return {ids[i]: float(sims[i]) for i in range(len(ids))}
