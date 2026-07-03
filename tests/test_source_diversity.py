"""Source-diversity cap: one source must not flood the whole retrieval top-k.

Chunks of a source share a title/heading context line, so a title-matching note
scores well on *all* its chunks; without a cap it crowds every other source out
of a small top-k (observed as a Recall@5 regression on the core eval).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.retrieve.lexical import RetrievalHit
from crossmodalrag.retrieve.lexical import retrieve as lexical_retrieve
from crossmodalrag.retrieve.rerank import cap_hits_per_source


def _hit(chunk_id: int, source_id: int, score: float, text: str = "") -> RetrievalHit:
    return RetrievalHit(
        chunk_id=chunk_id,
        source_id=source_id,
        source_type="note",
        source_uri=f"/vault/source-{source_id}.md",
        source_timestamp=None,
        title=f"source-{source_id}",
        chunk_index=chunk_id,
        chunk_text=text or f"chunk {chunk_id}",
        score=score,
        lexical_score=score,
        recency_score=0.0,
    )


def test_cap_keeps_best_chunks_and_score_order() -> None:
    hits = [
        _hit(1, source_id=10, score=0.9),
        _hit(2, source_id=10, score=0.8),
        _hit(3, source_id=10, score=0.7),
        _hit(4, source_id=20, score=0.6),
        _hit(5, source_id=10, score=0.5),
        _hit(6, source_id=30, score=0.4),
    ]
    capped = cap_hits_per_source(hits, cap=2)
    assert [h.chunk_id for h in capped] == [1, 2, 4, 6]


def test_cap_zero_disables(monkeypatch: pytest.MonkeyPatch) -> None:
    hits = [_hit(i, source_id=10, score=1.0 - i / 10) for i in range(5)]
    assert cap_hits_per_source(hits, cap=0) == hits
    monkeypatch.setenv("CMRAG_MAX_CHUNKS_PER_SOURCE", "0")
    assert cap_hits_per_source(hits) == hits


def test_cap_default_comes_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    hits = [_hit(i, source_id=10, score=1.0 - i / 10) for i in range(5)]
    monkeypatch.setenv("CMRAG_MAX_CHUNKS_PER_SOURCE", "3")
    assert len(cap_hits_per_source(hits)) == 3


def _seed_corpus(db_path: Path) -> "sqlite3.Connection":  # noqa: F821
    conn = connect(db_path)
    init_db(conn)
    # Source 1: a note whose every chunk carries a query-matching context line
    # (the flooding source). Source 2: the note the query is actually about.
    conn.execute(
        "INSERT INTO sources (id, source_type, source_uri, timestamp, title, metadata_json)"
        " VALUES (1, 'note', '/vault/spectral-faq.md', '2026-01-01T00:00:00+00:00', 'spectral faq', '{}')"
    )
    conn.execute(
        "INSERT INTO sources (id, source_type, source_uri, timestamp, title, metadata_json)"
        " VALUES (2, 'note', '/vault/spectroscopy.md', '2026-01-01T00:00:00+00:00', 'spectroscopy', '{}')"
    )
    for idx in range(6):
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)"
            " VALUES (1, ?, ?, '{}')",
            (idx, f"spectral signature faq\n\nbody {idx} discusses the spectral signature topic"),
        )
    conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)"
        " VALUES (2, 0, 'spectroscopy\n\na spectral signature is the wavelength fingerprint of a material', '{}')"
    )
    conn.commit()
    return conn


def test_lexical_retrieve_top_k_spans_multiple_sources(tmp_path: Path) -> None:
    conn = _seed_corpus(tmp_path / "mem.db")
    try:
        hits = lexical_retrieve(conn, query="spectral signature", top_k=5)
        source_ids = {h.source_id for h in hits}
        # Without the cap, source 1's six matching chunks fill the entire top-5.
        assert 2 in source_ids
    finally:
        conn.close()


def test_lexical_retrieve_restricted_drilldown_is_uncapped(tmp_path: Path) -> None:
    conn = _seed_corpus(tmp_path / "mem.db")
    try:
        source1_chunk_ids = {
            int(row["id"])
            for row in conn.execute("SELECT id FROM evidence_chunks WHERE source_id = 1").fetchall()
        }
        hits = lexical_retrieve(
            conn, query="spectral signature", top_k=5, restrict_chunk_ids=source1_chunk_ids
        )
        # Drill-down ranks within one node's evidence (often a single source):
        # the diversity cap must not starve it below top_k.
        assert len(hits) == 5
        assert all(h.source_id == 1 for h in hits)
    finally:
        conn.close()
