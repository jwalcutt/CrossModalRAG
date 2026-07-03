from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.retrieve.decompose import (
    ComparativeQuery,
    merge_side_hits,
    split_comparative_query,
)
from crossmodalrag.retrieve.hybrid import retrieve
from crossmodalrag.retrieve.lexical import RetrievalHit


# --- pattern detection ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "query,left,right",
    [
        pytest.param(
            "What is the difference between breadth-first search and depth-first search?",
            "breadth-first search",
            "depth-first search",
            id="difference-between",
        ),
        pytest.param(
            "Explain the differences between temporal and spacial locality.",
            "temporal",
            "spacial locality",
            id="differences-plural",
        ),
        pytest.param(
            "Compare a convolutional neural network with a recurrent neural network",
            "convolutional neural network",
            "recurrent neural network",
            id="compare-with-articles-stripped",
        ),
        pytest.param("compare BFS to DFS", "BFS", "DFS", id="compare-to"),
        pytest.param("CNN vs RNN?", "CNN", "RNN", id="vs"),
        pytest.param("gradient descent versus newton's method", "gradient descent", "newton's method", id="versus"),
    ],
)
def test_split_matches_comparative_queries(query: str, left: str, right: str) -> None:
    assert split_comparative_query(query) == ComparativeQuery(left=left, right=right)


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("What is the difference in performance?", id="difference-in"),
        pytest.param("What is a convolutional neural network?", id="plain-definitional"),
        pytest.param("compare notes", id="compare-without-conjunction"),
        pytest.param("What makes an agent autonomous?", id="no-comparative-marker"),
        pytest.param("difference between apples and apples", id="identical-sides"),
    ],
)
def test_split_ignores_non_comparative_queries(query: str) -> None:
    assert split_comparative_query(query) is None


# --- slot merge ------------------------------------------------------------------------------


def _hit(chunk_id: int, score: float, text: str = "text") -> RetrievalHit:
    return RetrievalHit(
        chunk_id=chunk_id,
        source_id=chunk_id,
        source_type="note",
        source_uri=f"/abs/{chunk_id}.md",
        source_timestamp=None,
        title=None,
        chunk_index=0,
        chunk_text=text,
        score=score,
        lexical_score=score,
        recency_score=0.0,
    )


def test_merge_reserves_slots_for_both_sides() -> None:
    # Left side scores dominate; without reservation the right side would vanish.
    left = [_hit(1, 0.9), _hit(2, 0.8), _hit(3, 0.7), _hit(4, 0.6)]
    right = [_hit(11, 0.5), _hit(12, 0.4)]
    merged = merge_side_hits(left, right, top_k=4)
    ids = {h.chunk_id for h in merged}
    assert len(merged) == 4
    assert 11 in ids  # right side kept its reserved share
    assert {1, 2} <= ids  # backfill favors the stronger side


def test_merge_full_query_ranking_stays_the_base() -> None:
    # A source covering BOTH subjects wins on the full query; the merge must keep
    # it even when the per-side pools rank other chunks higher.
    full = [_hit(50, 0.95), _hit(51, 0.85), _hit(52, 0.75)]
    left = [_hit(1, 0.9), _hit(2, 0.8)]
    right = [_hit(11, 0.7)]
    merged = merge_side_hits(left, right, top_k=5, full_hits=full)
    ids = {h.chunk_id for h in merged}
    assert {50, 51, 52} <= ids  # full-query base (k - 2 side slots = 3)
    assert 1 in ids and 11 in ids  # one reserved slot per side


def test_merge_backfills_when_one_side_is_short() -> None:
    left = [_hit(1, 0.9), _hit(2, 0.8), _hit(3, 0.7), _hit(4, 0.6), _hit(5, 0.5)]
    merged = merge_side_hits(left, [], top_k=4)
    assert [h.chunk_id for h in merged] == [1, 2, 3, 4]


def test_merge_dedupes_shared_chunks_and_sorts_by_score() -> None:
    shared = _hit(1, 0.9)
    merged = merge_side_hits([shared, _hit(2, 0.5)], [_hit(1, 0.9), _hit(3, 0.7)], top_k=3)
    assert [h.chunk_id for h in merged] == [1, 3, 2]  # deduped, score-desc


# --- end-to-end through hybrid retrieve (lexical fallback path) ------------------------------


def _seed(conn: sqlite3.Connection, docs: list[tuple[str, str]]) -> None:
    for uri, text in docs:
        cur = conn.execute(
            "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
            ("note", uri, "2026-02-01T00:00:00+00:00", Path(uri).stem),
        )
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
            (int(cur.lastrowid), 0, text),
        )
    conn.commit()


def test_comparative_query_retrieves_both_sides(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        # Alpha heavily represented; beta has a single note. A plain top-4 pass
        # over the full query would be dominated by alpha chunks.
        _seed(
            conn,
            [
                (
                    f"/vault/alpha-{i}.md",
                    # Distinct bodies (near-identical text would be collapsed by dedupe).
                    f"alpha protocol design details and tradeoffs part {i} "
                    + "covering " + " ".join(f"topic{i}{j}" for j in range(3)),
                )
                for i in range(4)
            ]
            + [("/vault/beta.md", "beta protocol design overview")],
        )
        hits = retrieve(conn, query="difference between alpha protocol and beta protocol", top_k=4)
        uris = {h.source_uri for h in hits}
        assert "/vault/beta.md" in uris  # the thin side is represented
        assert any("alpha" in u for u in uris)
        # Side-pool hits carry sub-query provenance; full-query hits carry None.
        assert {h.subquery for h in hits} <= {"alpha protocol", "beta protocol", None}
        assert any(h.subquery is not None for h in hits)
    finally:
        conn.close()


def test_comparative_query_with_one_absent_side_returns_present_side(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _seed(conn, [("/vault/alpha.md", "alpha protocol design details")])
        hits = retrieve(conn, query="difference between alpha protocol and zeta protocol", top_k=4)
        assert hits  # no crash, present side returned
        assert {h.subquery for h in hits} <= {"alpha protocol", None}
    finally:
        conn.close()


def test_non_comparative_query_bypasses_decomposition(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _seed(conn, [("/vault/alpha.md", "alpha protocol design details")])
        hits = retrieve(conn, query="alpha protocol design", top_k=4)
        assert hits
        assert all(h.subquery is None for h in hits)
    finally:
        conn.close()


def test_drilldown_restriction_skips_decomposition(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _seed(conn, [("/vault/alpha.md", "difference between alpha and beta noted here")])
        chunk_id = int(conn.execute("SELECT id FROM evidence_chunks").fetchone()["id"])
        hits = retrieve(
            conn,
            query="difference between alpha and beta",
            top_k=4,
            restrict_chunk_ids={chunk_id},
        )
        assert all(h.subquery is None for h in hits)
    finally:
        conn.close()
