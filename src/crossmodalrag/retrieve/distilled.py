from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from crossmodalrag.embed.provider import EmbeddingProvider, get_default_provider
from crossmodalrag.embed.store import unpack_vector
from crossmodalrag.retrieve import lexical
from crossmodalrag.retrieve.nodes import NODE_PROFILE_WEIGHTS, NodeHit

# Maps a CLI level name to (level, node_type) — the distillable levels (L2 episode, L3 concept).
LEVEL_TO_NODE: dict[str, tuple[int, str]] = {
    "episode": (2, "episode"),
    "concept": (3, "concept"),
}
DEFAULT_PROFILE = "balanced"


def retrieve_distilled(
    conn: sqlite3.Connection,
    query: str,
    *,
    level: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    provider: EmbeddingProvider | None = None,
) -> list[NodeHit]:
    """Hybrid retrieval over the *distilled* stand-ins of a target level.

    Ranks distilled nodes by their stored summary embedding + lexical overlap on the summary, reusing
    the node-profile weights. The distilled node stands in for the full node: drill-down (below) reaches
    only its core-evidence subset, which is what the distillation gate measures against the full path.
    """
    if profile not in NODE_PROFILE_WEIGHTS:
        raise ValueError(
            f"Unknown profile '{profile}'. Choose from: {', '.join(sorted(NODE_PROFILE_WEIGHTS))}."
        )
    if level not in LEVEL_TO_NODE:
        raise ValueError(f"Unknown distilled level '{level}'. Choose from: {', '.join(LEVEL_TO_NODE)}.")

    node_level, node_type = LEVEL_TO_NODE[level]
    rows = conn.execute(
        """
        SELECT d.node_id AS id, d.summary AS summary, n.time_start AS time_start,
               n.centrality AS centrality
        FROM distilled_nodes d JOIN memory_nodes n ON n.id = d.node_id
        WHERE d.level = ?
        """,
        (node_level,),
    ).fetchall()
    if not rows:
        return []

    query_tokens = lexical.tokenize(query)
    cosines = _distilled_cosines(conn, query, node_level, provider)

    w_vec, w_lex, w_rec, w_cen = NODE_PROFILE_WEIGHTS[profile]
    now = datetime.now(timezone.utc)

    hits: list[NodeHit] = []
    for row in rows:
        node_id = int(row["id"])
        cosine = cosines.get(node_id)
        summary = row["summary"] or ""
        lex = lexical.lexical_overlap_score(query_tokens, lexical.tokenize(summary)) if query_tokens else 0.0
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
                title=summary,
                content=summary,
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


def distilled_drilldown_source_uris(conn: sqlite3.Connection, hits: list[NodeHit]) -> list[str]:
    """Ordered-unique L0 source URIs reached via each hit's CORE-evidence subset (rank order).

    Unlike the full-node drill-down, this resolves only the distilled ``core_evidence_json`` — the
    compressed surface the distilled node stands in for.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for hit in hits:
        row = conn.execute(
            "SELECT core_evidence_json FROM distilled_nodes WHERE node_id = ?", (hit.node_id,)
        ).fetchone()
        if row is None or not row["core_evidence_json"]:
            continue
        chunk_ids = [int(c) for c in json.loads(row["core_evidence_json"])]
        if not chunk_ids:
            continue
        placeholders = ",".join("?" for _ in chunk_ids)
        uri_rows = conn.execute(
            f"""
            SELECT DISTINCT s.source_uri AS uri
            FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
            WHERE c.id IN ({placeholders})
            """,
            tuple(chunk_ids),
        ).fetchall()
        for uri_row in uri_rows:
            uri = str(uri_row["uri"])
            if uri not in seen:
                seen.add(uri)
                ordered.append(uri)
    return ordered


def _distilled_cosines(
    conn: sqlite3.Connection,
    query: str,
    node_level: int,
    provider: EmbeddingProvider | None,
) -> dict[int, float]:
    provider = provider or get_default_provider()
    if provider is None:
        return {}
    rows = conn.execute(
        "SELECT node_id, model, vector FROM distilled_nodes WHERE level = ? AND vector IS NOT NULL",
        (node_level,),
    ).fetchall()
    rows = [r for r in rows if str(r["model"]) == provider.name]
    if not rows:
        return {}

    import numpy as np

    ids = [int(r["node_id"]) for r in rows]
    matrix = np.array([unpack_vector(r["vector"]) for r in rows], dtype=np.float32)
    query_vec = np.array(provider.embed([query])[0], dtype=np.float32)
    matrix_norms = np.linalg.norm(matrix, axis=1)
    query_norm = float(np.linalg.norm(query_vec))
    if query_norm == 0.0:
        return {}
    denom = matrix_norms * query_norm
    denom[denom == 0.0] = np.inf
    sims = (matrix @ query_vec) / denom
    return {ids[i]: float(sims[i]) for i in range(len(ids))}
