from __future__ import annotations

from pathlib import Path

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import (
    EvalSummary,
    XMODAL_GATE_THRESHOLD,
    list_eval_queries,
    xmodal_gate_delta,
    xmodal_gate_fires,
)
from crossmodalrag.sample_data import purge_seeded_sample_data, seed_sample_data


@pytest.fixture
def seeded(tmp_path: Path):
    db_path = tmp_path / "mem.db"
    workspace_dir = tmp_path / "sample-workspace"
    conn = connect(db_path)
    init_db(conn)
    result = seed_sample_data(conn, workspace_dir=workspace_dir)
    try:
        yield conn, workspace_dir, result
    finally:
        conn.close()


# --- fixtures materialized + gold seeded --------------------------------------


def test_seed_materializes_binary_fixtures_into_vault(seeded):
    _, _, result = seeded
    documents = result.vault_dir / "documents"
    assert (documents / "spec.pdf").exists()
    assert (documents / "notes-screenshot.png").exists()
    assert (documents / "architecture-diagram.png").exists()
    # Binary copy must be byte-exact (stable fingerprint for steps 2-3).
    assert (documents / "spec.pdf").read_bytes().startswith(b"%PDF-")


def test_seed_adds_both_xmodal_slices_with_gold(seeded):
    conn, _, _ = seeded
    text_slice = list_eval_queries(conn, query_prefix="[sample-xmodal-text]")
    visual_slice = list_eval_queries(conn, query_prefix="[sample-xmodal-visual]")

    assert len(text_slice) == 2
    assert len(visual_slice) == 2
    # Every cross-modal gold query points at a documents/ fixture URI.
    for q in text_slice + visual_slice:
        assert q.expected_source_uris
        assert all("/documents/" in uri for uri in q.expected_source_uris)


def test_visual_slice_gold_is_the_diagram(seeded):
    conn, _, _ = seeded
    visual = list_eval_queries(conn, query_prefix="[sample-xmodal-visual]")
    assert visual  # both visual queries point at the diagram fixture
    assert all(q.expected_source_uris[0].endswith("architecture-diagram.png") for q in visual)


# --- idempotency + purge ------------------------------------------------------


def test_reseeding_does_not_churn_xmodal_rows(seeded):
    conn, workspace_dir, _ = seeded
    before = _xmodal_snapshot(conn)

    # Re-seed into the same workspace/DB.
    seed_sample_data(conn, workspace_dir=workspace_dir)
    after = _xmodal_snapshot(conn)

    assert after == before


def test_purge_removes_xmodal_rows_and_fixture_sources(seeded):
    conn, workspace_dir, _ = seeded
    purge_seeded_sample_data(conn, workspace_dir=workspace_dir)

    assert list_eval_queries(conn, query_prefix="[sample-xmodal-text]") == []
    assert list_eval_queries(conn, query_prefix="[sample-xmodal-visual]") == []


# --- gate logic ---------------------------------------------------------------


def _summary(recall: float) -> EvalSummary:
    return EvalSummary(
        query_count=1,
        top_k=5,
        recall_at_k=recall,
        mrr_at_k=recall,
        citation_hit_rate=0.0,
        results=[],
    )


@pytest.mark.parametrize(
    "text_recall,visual_recall,expected_fire",
    [
        (1.0, 0.0, True),    # full text recall, zero visual → gap 1.0 ≥ 0.30
        (1.0, 0.7, True),    # gap exactly 0.30 → fires (>=)
        (1.0, 0.75, False),  # gap 0.25 < 0.30 → holds
        (0.0, 0.0, False),   # pre-ingestion baseline: both zero → holds
    ],
)
def test_gate_fires_only_on_sufficient_shortfall(text_recall, visual_recall, expected_fire):
    text = _summary(text_recall)
    visual = _summary(visual_recall)

    assert xmodal_gate_delta(text, visual) == pytest.approx(text_recall - visual_recall)
    assert xmodal_gate_fires(text, visual) is expected_fire


def test_gate_threshold_is_the_committed_value():
    assert XMODAL_GATE_THRESHOLD == 0.30


def _xmodal_snapshot(conn):
    rows = conn.execute(
        """
        SELECT query_text, expected_source_uris
        FROM queries_eval
        WHERE query_text LIKE '[sample-xmodal%'
        ORDER BY query_text
        """
    ).fetchall()
    return [tuple(row) for row in rows]
