from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from crossmodalrag.config import get_title_boost_weight
from crossmodalrag.db import connect, init_db
from crossmodalrag.retrieve import lexical
from crossmodalrag.retrieve.lexical import lexical_overlap_score, title_overlap, tokenize


# --- tokenizer: underscore-squashed variants ------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        pytest.param("F_1 score", ["f_1", "f1", "score"], id="math-notation"),
        pytest.param("top_k", ["top_k", "topk"], id="code-identifier"),
        pytest.param("plain words", ["plain", "words"], id="no-underscores-unchanged"),
        pytest.param("_", ["_"], id="bare-underscore-no-empty-variant"),
    ],
)
def test_tokenize_emits_squashed_underscore_variants(text: str, expected: list[str]) -> None:
    assert tokenize(text) == expected


def test_query_f1_matches_doc_f_underscore_1_symmetrically() -> None:
    # The Q9-class fix: a note's LaTeX "F_1" must be matchable by the query "f1".
    query = tokenize("What does F1 score measure?")
    doc = tokenize(r"$F_1 = 2 \times \frac{Precision \times Recall}{Precision + Recall}$")
    assert lexical_overlap_score(query, doc) > 0.0


# --- title_overlap ---------------------------------------------------------------------------


def test_title_overlap_scores_and_caches() -> None:
    cache: dict[str, list[str]] = {}
    q = tokenize("what is the fourier transform?")
    assert title_overlap(q, "Fourier Transform", cache) > 0.5
    assert title_overlap(q, "Unrelated Topic", cache) == 0.0
    assert title_overlap(q, None, cache) == 0.0
    assert set(cache) == {"Fourier Transform", "Unrelated Topic"}


def test_title_boost_weight_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMRAG_TITLE_BOOST_WEIGHT", raising=False)
    assert get_title_boost_weight() == 0.05
    monkeypatch.setenv("CMRAG_TITLE_BOOST_WEIGHT", "0.1")
    assert get_title_boost_weight() == 0.1
    monkeypatch.setenv("CMRAG_TITLE_BOOST_WEIGHT", "0")
    assert get_title_boost_weight() == 0.0  # 0 disables
    monkeypatch.setenv("CMRAG_TITLE_BOOST_WEIGHT", "garbage")
    assert get_title_boost_weight() == 0.05


# --- end-to-end: a title-matching note wins the near-tie ------------------------------------


def _seed(conn: sqlite3.Connection) -> None:
    rows = [
        # Same body relevance; only the title differs. The title-matching source
        # must win the near-tie against the incidental mention.
        ("note", "/vault/Fourier Transform.md", "Fourier Transform",
         "the transform decomposes a signal into frequencies"),
        ("git_commit", "/repo@abc", "commit abc",
         "the transform decomposes a signal into frequencies"),
    ]
    for stype, uri, title, text in rows:
        cur = conn.execute(
            "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
            (stype, uri, "2026-02-01T00:00:00+00:00", title),
        )
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
            (int(cur.lastrowid), 0, text),
        )
    conn.commit()


def test_lexical_retrieve_title_match_wins_near_tie(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The fixture's two bodies are identical (isolating the title as the only
    # difference), which would trip near-duplicate dedupe — disable it here.
    monkeypatch.setenv("CMRAG_DEDUPE_THRESHOLD", "2")
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _seed(conn)
        hits = lexical.retrieve(conn, query="fourier transform frequencies", top_k=2)
        assert [h.source_uri for h in hits] == ["/vault/Fourier Transform.md", "/repo@abc"]
        assert hits[0].title_score > 0.0
        assert hits[1].title_score == 0.0
        # The boost is additive on an otherwise-identical hit: score gap == w * title_score.
        assert hits[0].score - hits[1].score == pytest.approx(
            get_title_boost_weight() * hits[0].title_score
        )
    finally:
        conn.close()


def test_title_boost_disabled_is_score_neutral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CMRAG_TITLE_BOOST_WEIGHT", "0")
    monkeypatch.setenv("CMRAG_DEDUPE_THRESHOLD", "2")
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _seed(conn)
        hits = lexical.retrieve(conn, query="fourier transform frequencies", top_k=2)
        assert hits[0].score == pytest.approx(hits[1].score)
    finally:
        conn.close()
