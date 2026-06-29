from __future__ import annotations

from pathlib import Path

import pytest

from crossmodalrag.config import (
    get_distill_compression_ratio,
    get_distill_epsilon,
    get_drift_window_days,
)
from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import (
    DISTILL_GATE_COMPRESSION_RATIO,
    DISTILL_GATE_EPSILON,
    EvalSummary,
    distill_gate_delta,
    distill_gate_fires,
    list_eval_queries,
)
from crossmodalrag.memory.store import delete_node, insert_node
from crossmodalrag.sample_data import purge_seeded_sample_data, seed_sample_data

PHASE5_TABLES = ("distilled_nodes", "drift_snapshots")


@pytest.fixture
def conn(tmp_path: Path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


@pytest.fixture
def seeded(tmp_path: Path):
    db_path = tmp_path / "mem.db"
    workspace_dir = tmp_path / "sample-workspace"
    connection = connect(db_path)
    init_db(connection)
    result = seed_sample_data(connection, workspace_dir=workspace_dir)
    try:
        yield connection, workspace_dir, result
    finally:
        connection.close()


def _table_names(conn) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {str(row["name"]) for row in rows}


# --- additive schema ----------------------------------------------------------


def test_init_db_creates_phase5_tables(conn):
    tables = _table_names(conn)
    for table in PHASE5_TABLES:
        assert table in tables


def test_phase5_tables_are_additive_and_recreatable(conn):
    # A pre-existing memory node must survive re-running init_db after the Phase 5 tables are
    # dropped (the migration is additive: it never touches existing rows/tables).
    node_id = insert_node(conn, level=3, node_type="concept", title="kept concept")
    conn.commit()

    for table in PHASE5_TABLES:
        conn.execute(f"DROP TABLE {table}")
    conn.commit()
    assert not (set(PHASE5_TABLES) & _table_names(conn))

    init_db(conn)
    assert set(PHASE5_TABLES) <= _table_names(conn)
    # Existing data untouched.
    assert conn.execute("SELECT title FROM memory_nodes WHERE id = ?", (node_id,)).fetchone()[
        "title"
    ] == "kept concept"


def test_phase5_tables_inert_after_seed(seeded):
    # Scaffolding writes nothing to the Phase 5 tables, so the layer cannot affect ranking
    # (ablation: dropping it changes nothing). Derivation lands in a later step.
    conn, _, _ = seeded
    for table in PHASE5_TABLES:
        assert conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"] == 0


# --- delete_node cleanup ------------------------------------------------------


def test_delete_node_purges_phase5_rows(conn):
    node_id = insert_node(conn, level=3, node_type="concept", title="doomed concept")
    conn.execute(
        "INSERT INTO distilled_nodes (node_id, level) VALUES (?, ?)",
        (node_id, 3),
    )
    conn.execute(
        "INSERT INTO drift_snapshots (concept_id, support) VALUES (?, ?)",
        (node_id, 2),
    )
    conn.commit()

    delete_node(conn, node_id)
    conn.commit()

    assert conn.execute(
        "SELECT COUNT(*) AS n FROM distilled_nodes WHERE node_id = ?", (node_id,)
    ).fetchone()["n"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM drift_snapshots WHERE concept_id = ?", (node_id,)
    ).fetchone()["n"] == 0


# --- [sample-drift] slice + fixtures ------------------------------------------


def test_seed_adds_drift_slice_with_grounded_gold(seeded):
    conn, _, _ = seeded
    drift = list_eval_queries(conn, query_prefix="[sample-drift]")
    assert len(drift) == 2
    for q in drift:
        assert q.expected_source_uris
        assert all("/concepts/" in uri for uri in q.expected_source_uris)


def test_drift_concept_query_gold_spans_both_chunking_notes(seeded):
    conn, _, _ = seeded
    drift = list_eval_queries(conn, query_prefix="[sample-drift]")
    drifting = next(q for q in drift if "chunking strategy change" in q.query_text)
    assert len(drifting.expected_source_uris) == 2
    assert any(uri.endswith("chunking-early.md") for uri in drifting.expected_source_uris)
    assert any(uri.endswith("chunking-late.md") for uri in drifting.expected_source_uris)


def test_drift_fixtures_materialized_into_vault(seeded):
    _, _, result = seeded
    concepts = result.vault_dir / "concepts"
    assert (concepts / "chunking-early.md").exists()
    assert (concepts / "chunking-late.md").exists()
    assert (concepts / "provenance.md").exists()


def test_total_eval_slice_count_unchanged_for_existing_slices(seeded):
    # No regression: the existing slices keep their counts; only [sample-drift] is added.
    conn, _, _ = seeded
    # LIKE '[sample]%' matches only the literal "[sample] " prefix (3 positive + 1 negative),
    # not the hyphenated sibling slices.
    assert len(list_eval_queries(conn, query_prefix="[sample]")) == 4
    assert len(list_eval_queries(conn, query_prefix="[sample-synth]")) == 2
    assert len(list_eval_queries(conn, query_prefix="[sample-xmodal-text]")) == 2
    assert len(list_eval_queries(conn, query_prefix="[sample-xmodal-visual]")) == 2
    assert len(list_eval_queries(conn, query_prefix="[sample-usage]")) == 1


# --- idempotency + purge ------------------------------------------------------


def test_reseeding_does_not_churn_drift_rows(seeded):
    conn, workspace_dir, _ = seeded
    before = _drift_snapshot(conn)

    seed_sample_data(conn, workspace_dir=workspace_dir)
    after = _drift_snapshot(conn)

    assert after == before


def test_purge_removes_drift_rows(seeded):
    conn, workspace_dir, _ = seeded
    purge_seeded_sample_data(conn, workspace_dir=workspace_dir)
    assert list_eval_queries(conn, query_prefix="[sample-drift]") == []


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


def test_distill_gate_delta_is_recall_lost():
    assert distill_gate_delta(_summary(1.0), _summary(0.9)) == pytest.approx(0.1)
    # Distilled better than full → negative loss.
    assert distill_gate_delta(_summary(0.8), _summary(0.9)) == pytest.approx(-0.1)


@pytest.mark.parametrize(
    "full,distilled,ratio,expected_fire",
    [
        (1.0, 1.0, 0.40, True),   # no recall loss, compressed → adopt
        (1.0, 0.96, 0.50, True),  # loss 0.04 <= epsilon AND ratio == target (<=) → adopt
        (1.0, 0.90, 0.40, False),  # loss 0.10 > epsilon → hold (recall not preserved)
        (1.0, 1.00, 0.60, False),  # ratio 0.60 > target → hold (no real compression)
    ],
)
def test_distill_gate_fires_only_when_recall_preserved_and_compressed(
    full, distilled, ratio, expected_fire
):
    assert (
        distill_gate_fires(_summary(full), _summary(distilled), compression_ratio=ratio)
        is expected_fire
    )


def test_distill_gate_constants_are_the_committed_values():
    assert DISTILL_GATE_EPSILON == 0.05
    assert DISTILL_GATE_COMPRESSION_RATIO == 0.5


# --- config getters -----------------------------------------------------------


def test_config_defaults(monkeypatch):
    for var in ("CMRAG_DRIFT_WINDOW_DAYS", "CMRAG_DISTILL_EPSILON", "CMRAG_DISTILL_COMPRESSION_RATIO"):
        monkeypatch.delenv(var, raising=False)
    assert get_drift_window_days() == 30.0
    assert get_distill_epsilon() == 0.05
    assert get_distill_compression_ratio() == 0.5


def test_config_env_override(monkeypatch):
    monkeypatch.setenv("CMRAG_DRIFT_WINDOW_DAYS", "14")
    monkeypatch.setenv("CMRAG_DISTILL_EPSILON", "0.1")
    monkeypatch.setenv("CMRAG_DISTILL_COMPRESSION_RATIO", "0.3")
    assert get_drift_window_days() == 14.0
    assert get_distill_epsilon() == 0.1
    assert get_distill_compression_ratio() == 0.3


@pytest.mark.parametrize("bad", ["0", "-1", "nonsense"])
def test_config_compression_ratio_rejects_out_of_range(monkeypatch, bad):
    monkeypatch.setenv("CMRAG_DISTILL_COMPRESSION_RATIO", bad)
    assert get_distill_compression_ratio() == 0.5  # falls back to default


def _drift_snapshot(conn):
    rows = conn.execute(
        """
        SELECT query_text, expected_source_uris
        FROM queries_eval
        WHERE query_text LIKE '[sample-drift%'
        ORDER BY query_text
        """
    ).fetchall()
    return [tuple(row) for row in rows]
