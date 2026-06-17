from __future__ import annotations

import sqlite3

from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.embed.store import pack_vector, upsert_chunk_embedding


def purge_source_embeddings(conn: sqlite3.Connection, source_id: int) -> None:
    """Drop embeddings for a source's chunks before those chunks are deleted."""
    conn.execute(
        """
        DELETE FROM chunk_embeddings
        WHERE chunk_id IN (SELECT id FROM evidence_chunks WHERE source_id = ?)
        """,
        (source_id,),
    )


def embed_source_chunks(
    conn: sqlite3.Connection,
    embedder: EmbeddingProvider | None,
    chunks: list[tuple[int, str]],
) -> None:
    """Embed freshly inserted chunks inline (no-op when embedder is None)."""
    if embedder is None or not chunks:
        return
    texts = [text for _, text in chunks]
    vectors = embedder.embed(texts)
    for (chunk_id, _), vector in zip(chunks, vectors):
        upsert_chunk_embedding(
            conn,
            chunk_id=chunk_id,
            model=embedder.name,
            dim=len(vector),
            vector_bytes=pack_vector(vector),
        )
