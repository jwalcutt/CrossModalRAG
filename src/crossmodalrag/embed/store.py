from __future__ import annotations

import sqlite3
from array import array
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crossmodalrag.embed.provider import EmbeddingProvider


def pack_vector(vector: list[float]) -> bytes:
    """Serialize a float vector to little-endian float32 bytes (stdlib only)."""
    arr = array("f", vector)
    if array("f", [1.0]).tobytes() != _LE_ONE:
        arr.byteswap()  # pragma: no cover - only on big-endian hosts
    return arr.tobytes()


def unpack_vector(blob: bytes) -> list[float]:
    arr = array("f")
    arr.frombytes(blob)
    if array("f", [1.0]).tobytes() != _LE_ONE:
        arr.byteswap()  # pragma: no cover - only on big-endian hosts
    return list(arr)


_LE_ONE = b"\x00\x00\x80\x3f"  # float32 1.0, little-endian


def upsert_chunk_embedding(
    conn: sqlite3.Connection,
    chunk_id: int,
    model: str,
    dim: int,
    vector_bytes: bytes,
) -> None:
    conn.execute(
        """
        INSERT INTO chunk_embeddings (chunk_id, model, dim, vector)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chunk_id) DO UPDATE SET
            model = excluded.model,
            dim = excluded.dim,
            vector = excluded.vector,
            created_at = CURRENT_TIMESTAMP
        """,
        (chunk_id, model, dim, vector_bytes),
    )


def count_embeddings(conn: sqlite3.Connection, model: str | None = None) -> int:
    if model is None:
        row = conn.execute("SELECT COUNT(*) AS n FROM chunk_embeddings").fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM chunk_embeddings WHERE model = ?", (model,)
        ).fetchone()
    return int(row["n"])


def embed_pending_chunks(
    conn: sqlite3.Connection,
    provider: "EmbeddingProvider",
    batch_size: int = 64,
    progress=None,
) -> int:
    """Embed chunks that have no vector for ``provider.name`` (or a stale model).

    Idempotent and resumable: only chunks missing a current-model embedding are
    processed, so re-running after an interruption continues where it left off.
    Returns the number of chunks embedded. ``progress`` (optional ``(done, total) -> None``)
    is called after each batch.
    """
    rows = conn.execute(
        """
        SELECT c.id AS chunk_id, c.chunk_text AS chunk_text
        FROM evidence_chunks c
        LEFT JOIN chunk_embeddings e ON e.chunk_id = c.id
        WHERE e.chunk_id IS NULL OR e.model != ?
        ORDER BY c.id ASC
        """,
        (provider.name,),
    ).fetchall()

    total = len(rows)
    embedded = 0
    for start in range(0, total, batch_size):
        batch = rows[start : start + batch_size]
        texts = [str(row["chunk_text"]) for row in batch]
        vectors = provider.embed(texts)
        for row, vector in zip(batch, vectors):
            upsert_chunk_embedding(
                conn,
                chunk_id=int(row["chunk_id"]),
                model=provider.name,
                dim=len(vector),
                vector_bytes=pack_vector(vector),
            )
            embedded += 1
        conn.commit()
        if progress is not None:
            progress(min(start + batch_size, total), total)
    return embedded


def upsert_node_embedding(
    conn: sqlite3.Connection,
    node_id: int,
    model: str,
    dim: int,
    vector_bytes: bytes,
) -> None:
    conn.execute(
        """
        INSERT INTO node_embeddings (node_id, model, dim, vector)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            model = excluded.model,
            dim = excluded.dim,
            vector = excluded.vector,
            created_at = CURRENT_TIMESTAMP
        """,
        (node_id, model, dim, vector_bytes),
    )


def count_node_embeddings(conn: sqlite3.Connection, model: str | None = None) -> int:
    if model is None:
        row = conn.execute("SELECT COUNT(*) AS n FROM node_embeddings").fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM node_embeddings WHERE model = ?", (model,)
        ).fetchone()
    return int(row["n"])


def embed_pending_nodes(
    conn: sqlite3.Connection,
    provider: "EmbeddingProvider",
    *,
    level: int,
    node_type: str,
    batch_size: int = 64,
    progress=None,
) -> int:
    """Embed memory_nodes (title + content) lacking a current-model vector.

    Mirrors ``embed_pending_chunks``: resumable and model-aware. Returns the
    number of nodes embedded this run. ``progress`` (optional ``(done, total) -> None``)
    is called after each batch.
    """
    rows = conn.execute(
        """
        SELECT n.id AS node_id, n.title AS title, n.content AS content
        FROM memory_nodes n
        LEFT JOIN node_embeddings e ON e.node_id = n.id
        WHERE n.level = ? AND n.node_type = ? AND (e.node_id IS NULL OR e.model != ?)
        ORDER BY n.id ASC
        """,
        (level, node_type, provider.name),
    ).fetchall()

    total = len(rows)
    embedded = 0
    for start in range(0, total, batch_size):
        batch = rows[start : start + batch_size]
        texts = [_node_text(row) for row in batch]
        vectors = provider.embed(texts)
        for row, vector in zip(batch, vectors):
            upsert_node_embedding(
                conn,
                node_id=int(row["node_id"]),
                model=provider.name,
                dim=len(vector),
                vector_bytes=pack_vector(vector),
            )
            embedded += 1
        conn.commit()
        if progress is not None:
            progress(min(start + batch_size, total), total)
    return embedded


def load_node_vectors(
    conn: sqlite3.Connection,
    model: str,
    *,
    level: int,
) -> list[tuple[int, list[float]]]:
    rows = conn.execute(
        """
        SELECT e.node_id AS node_id, e.vector AS vector
        FROM node_embeddings e
        JOIN memory_nodes n ON n.id = e.node_id
        WHERE e.model = ? AND n.level = ?
        ORDER BY e.node_id ASC
        """,
        (model, level),
    ).fetchall()
    return [(int(row["node_id"]), unpack_vector(row["vector"])) for row in rows]


def _node_text(row: sqlite3.Row) -> str:
    title = str(row["title"] or "")
    content = str(row["content"] or "")
    return f"{title}\n{content}".strip()
