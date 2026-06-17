from __future__ import annotations

import sqlite3

from crossmodalrag.embed.store import unpack_vector


def has_vectors_for_model(conn: sqlite3.Connection, model: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM chunk_embeddings WHERE model = ? LIMIT 1", (model,)
    ).fetchone()
    return row is not None


def vector_retrieve(
    conn: sqlite3.Connection,
    query_vector: list[float],
    model: str,
    top_k: int = 50,
) -> dict[int, float]:
    """Brute-force cosine similarity over stored vectors for ``model``.

    Returns a {chunk_id: cosine} map (cosine in [-1, 1]) for the top_k chunks.
    Uses numpy (guaranteed by the embeddings extra) for the matrix multiply.
    """
    import numpy as np

    rows = conn.execute(
        "SELECT chunk_id, vector FROM chunk_embeddings WHERE model = ?",
        (model,),
    ).fetchall()
    if not rows:
        return {}

    chunk_ids = [int(row["chunk_id"]) for row in rows]
    matrix = np.array([unpack_vector(row["vector"]) for row in rows], dtype=np.float32)
    query = np.array(query_vector, dtype=np.float32)

    matrix_norms = np.linalg.norm(matrix, axis=1)
    query_norm = float(np.linalg.norm(query))
    if query_norm == 0.0:
        return {}
    denom = matrix_norms * query_norm
    denom[denom == 0.0] = np.inf
    sims = (matrix @ query) / denom

    order = np.argsort(-sims)[:top_k]
    return {chunk_ids[i]: float(sims[i]) for i in order}
