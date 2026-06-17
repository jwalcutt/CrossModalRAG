from __future__ import annotations

import hashlib
import re

from crossmodalrag.db import connect, init_db
from crossmodalrag.embed.store import (
    count_embeddings,
    embed_pending_chunks,
    pack_vector,
    unpack_vector,
)
from crossmodalrag.ingest.notes import ingest_notes
from crossmodalrag.retrieve import hybrid
from crossmodalrag.retrieve.vector import has_vectors_for_model, vector_retrieve

WORD_RE = re.compile(r"[a-z0-9]+")


class StubProvider:
    """Deterministic bag-of-hashed-tokens embeddings (no external deps).

    Similar text yields similar vectors, so cosine ranking is meaningful, while
    staying fully reproducible across processes (hashlib, not builtin hash()).
    """

    def __init__(self, dim: int = 64, name: str = "stub-model-v1") -> None:
        self.dim = dim
        self.name = name

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            vec = [0.0] * self.dim
            for tok in WORD_RE.findall(text.lower()):
                bucket = int(hashlib.md5(tok.encode()).hexdigest(), 16) % self.dim
                vec[bucket] += 1.0
            out.append(vec)
        return out


def _new_db(tmp_path):
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    return conn


def _add_chunk(conn, source_id, idx, text) -> int:
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (source_id, idx, text),
    )
    return int(cur.lastrowid)


def _add_source(conn, uri, timestamp="2026-06-01T00:00:00+00:00") -> int:
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", uri, timestamp, uri),
    )
    return int(cur.lastrowid)


def test_pack_unpack_round_trip() -> None:
    vec = [0.0, 1.5, -2.25, 3.0]
    restored = unpack_vector(pack_vector(vec))
    assert restored == vec


def test_embed_pending_chunks_is_idempotent_and_model_aware(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a")
    _add_chunk(conn, sid, 0, "alpha beta")
    _add_chunk(conn, sid, 1, "gamma delta")
    conn.commit()

    provider = StubProvider()
    assert embed_pending_chunks(conn, provider) == 2
    assert embed_pending_chunks(conn, provider) == 0  # nothing pending now
    assert count_embeddings(conn, model=provider.name) == 2

    # A new model re-embeds everything.
    other = StubProvider(name="stub-model-v2")
    assert embed_pending_chunks(conn, other) == 2
    assert count_embeddings(conn, model=other.name) == 2


def test_vector_retrieve_ranks_by_cosine(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a")
    c_match = _add_chunk(conn, sid, 0, "parser bounds check bug fix")
    _add_chunk(conn, sid, 1, "unrelated cooking recipe content")
    conn.commit()

    provider = StubProvider()
    embed_pending_chunks(conn, provider)
    query_vec = provider.embed(["parser bounds check"])[0]
    ranked = vector_retrieve(conn, query_vec, model=provider.name, top_k=2)

    assert list(ranked)[0] == c_match
    assert ranked[c_match] > 0.0


def test_hybrid_uses_vectors_when_available(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a")
    target = _add_chunk(conn, sid, 0, "semantic memory retrieval ranking")
    _add_chunk(conn, sid, 1, "completely different lunch menu")
    conn.commit()

    provider = StubProvider()
    embed_pending_chunks(conn, provider)
    hits = hybrid.retrieve(conn, "memory ranking", top_k=2, provider=provider)

    assert has_vectors_for_model(conn, provider.name)
    assert hits[0].chunk_id == target
    assert hits[0].vector_score > 0.0


def test_hybrid_falls_back_to_lexical_without_vectors(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a")
    target = _add_chunk(conn, sid, 0, "deterministic ingestion fingerprint")
    _add_chunk(conn, sid, 1, "another note about weather")
    conn.commit()

    # Provider given but no vectors stored -> lexical fallback (vector_score stays 0).
    provider = StubProvider()
    hits = hybrid.retrieve(conn, "fingerprint", top_k=2, provider=provider)
    assert hits[0].chunk_id == target
    assert all(hit.vector_score == 0.0 for hit in hits)


def test_hybrid_rejects_unknown_profile(tmp_path) -> None:
    conn = _new_db(tmp_path)
    import pytest

    with pytest.raises(ValueError):
        hybrid.retrieve(conn, "anything", profile="nonsense", provider=StubProvider())


def test_ingest_inline_embeds_and_reingest_leaves_no_orphans(tmp_path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    note = vault / "topic.md"
    note.write_text("# Topic\n\nfirst body about parsing\n\n## Detail\n\nsecond body", encoding="utf-8")

    conn = _new_db(tmp_path)
    provider = StubProvider()
    ingest_notes(conn, vault_path=note.parent, embedder=provider)
    assert count_embeddings(conn, model=provider.name) > 0

    # Change content and re-ingest: embeddings must match current chunks, no orphans.
    note.write_text("# Topic\n\nrewritten single body only", encoding="utf-8")
    ingest_notes(conn, vault_path=note.parent, embedder=provider)

    orphans = conn.execute(
        """
        SELECT COUNT(*) AS n FROM chunk_embeddings e
        LEFT JOIN evidence_chunks c ON c.id = e.chunk_id
        WHERE c.id IS NULL
        """
    ).fetchone()["n"]
    assert orphans == 0

    chunk_count = conn.execute("SELECT COUNT(*) AS n FROM evidence_chunks").fetchone()["n"]
    assert count_embeddings(conn) == chunk_count


def test_run_eval_through_hybrid_fallback(tmp_path, monkeypatch) -> None:
    from crossmodalrag.evaluation import run_eval

    conn = _new_db(tmp_path)
    sid = _add_source(conn, "/abs/note-a.md")
    _add_chunk(conn, sid, 0, "bounds check parser fix")
    conn.execute(
        "INSERT INTO queries_eval (query_text, expected_source_uris) VALUES (?, ?)",
        ("[test] parser fix", '["/abs/note-a.md"]'),
    )
    conn.commit()

    # No embeddings + no provider -> lexical fallback path inside hybrid.
    monkeypatch.setattr(hybrid, "get_default_provider", lambda: None)
    summary = run_eval(conn, top_k=5, query_prefix="[test]")
    assert summary.query_count == 1
    assert summary.recall_at_k == 1.0
