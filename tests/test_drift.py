from __future__ import annotations

import hashlib
import re

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.drift import build_drift, concept_drift_summaries
from crossmodalrag.memory.store import add_edge, resolve_to_evidence

WORD_RE = re.compile(r"[a-z0-9]+")
WINDOW_DAYS = 30.0

# Window 0 anchors at the earliest member; with a 30-day window:
#   2026-01-01 -> window 0
#   2026-02-10 -> window 1 (40 days later, adjacent)
#   2026-03-15 -> window 2 (73 days later, leaves window 1 empty -> a relearning gap)
WIN0 = "2026-01-01T00:00:00+00:00"
WIN1 = "2026-02-10T00:00:00+00:00"
WIN2 = "2026-03-15T00:00:00+00:00"


class StubEmbedProvider:
    """Deterministic bag-of-tokens embeddings (token -> bucket), matching the concepts test stub."""

    def __init__(self, dim: int = 64, name: str = "stub-embed-v1") -> None:
        self.dim = dim
        self.name = name

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            vec = [0.0] * self.dim
            for tok in WORD_RE.findall(text.lower()):
                vec[int(hashlib.md5(tok.encode()).hexdigest(), 16) % self.dim] += 1.0
            out.append(vec)
        return out


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _add_event(conn, title: str, time_start: str) -> int:
    """An L1 event grounded to one L0 chunk; its embedding derives from ``title``."""
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri) VALUES ('note', ?)",
        (f"/v/{title.replace(' ', '_')}-{time_start[:10]}.md",),
    )
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, 0, ?)",
        (sid, title),
    )
    chunk_id = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO memory_nodes (level, node_type, title, time_start, time_end) "
        "VALUES (1, 'event', ?, ?, ?)",
        (title, time_start, time_start),
    )
    event_id = int(cur.lastrowid)
    add_edge(conn, 1, event_id, 0, chunk_id, "derived_from")
    conn.commit()
    return event_id


def _make_concept(conn, title: str, member_ids: list[int]) -> int:
    cur = conn.execute(
        "INSERT INTO memory_nodes (level, node_type, title) VALUES (3, 'concept', ?)", (title,)
    )
    concept_id = int(cur.lastrowid)
    for mid in member_ids:
        add_edge(conn, 3, concept_id, 1, mid, "contains")
    conn.commit()
    return concept_id


def _snapshot_rows(conn):
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT id, concept_id, window_start, window_end, drift_metric, support "
            "FROM drift_snapshots ORDER BY id"
        ).fetchall()
    ]


# --- drift detection ----------------------------------------------------------


def test_drifting_concept_has_nonzero_drift(conn):
    # Content shifts across windows (no shared tokens) -> centroid moves -> drift > 0.
    e1 = _add_event(conn, "alpha beta gamma", WIN0)
    e2 = _add_event(conn, "delta epsilon zeta", WIN1)
    cid = _make_concept(conn, "drifting concept", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)

    summaries = concept_drift_summaries(conn)
    assert len(summaries) == 1
    assert summaries[0].concept_id == cid
    assert summaries[0].window_count == 2
    assert summaries[0].overall_drift > 0.5


def test_drifting_concept_ranks_above_stable_control(conn):
    # Drifting concept.
    d1 = _add_event(conn, "alpha beta", WIN0)
    d2 = _add_event(conn, "gamma delta", WIN1)
    drift_id = _make_concept(conn, "drift", [d1, d2])
    # Stable concept: identical content across adjacent windows -> ~0 movement.
    s1 = _add_event(conn, "stable topic", WIN0)
    s2 = _add_event(conn, "stable topic", WIN1)
    stable_id = _make_concept(conn, "stable", [s1, s2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    summaries = concept_drift_summaries(conn)

    by_id = {s.concept_id: s for s in summaries}
    assert by_id[stable_id].overall_drift == pytest.approx(0.0, abs=1e-6)
    assert by_id[drift_id].overall_drift > by_id[stable_id].overall_drift
    assert summaries[0].concept_id == drift_id  # ranked first


# --- relearning ---------------------------------------------------------------


def test_relearning_flagged_on_window_gap(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN2)  # window 2: window 1 is empty -> a gap
    _make_concept(conn, "gapped", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    summary = concept_drift_summaries(conn)[0]
    assert summary.relearning is True


def test_contiguous_windows_not_relearning(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)  # adjacent window -> no gap
    _make_concept(conn, "contiguous", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    summary = concept_drift_summaries(conn)[0]
    assert summary.relearning is False


# --- grounding ----------------------------------------------------------------


def test_drift_concepts_ground_to_l0(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)
    cid = _make_concept(conn, "grounded", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    summary = concept_drift_summaries(conn)[0]

    assert resolve_to_evidence(conn, 3, cid)  # drills to >=1 L0 chunk
    assert summary.evidence_source_uri is not None


# --- determinism + reconcile --------------------------------------------------


def test_rebuild_is_a_noop(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)
    _make_concept(conn, "c", [e1, e2])

    first = build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    before = _snapshot_rows(conn)
    second = build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    after = _snapshot_rows(conn)

    assert first.snapshots_created > 0
    assert second.snapshots_created == 0
    assert second.snapshots_deleted == 0
    assert second.snapshots_kept == first.snapshots_created
    assert after == before  # identical rows incl. ids


def test_changing_window_length_rebuckets(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)
    _make_concept(conn, "c", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    assert len(_snapshot_rows(conn)) == 2  # two 30-day windows

    # A 365-day window collapses both events into one window (one snapshot, drift 0).
    result = build_drift(conn, StubEmbedProvider(), window_days=365.0)
    rows = _snapshot_rows(conn)
    assert len(rows) == 1
    assert result.snapshots_deleted == 2
    assert rows[0][4] == pytest.approx(0.0)  # drift_metric of a single window


# --- filters + additivity -----------------------------------------------------


def test_min_support_filters_thin_concepts(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)
    _make_concept(conn, "two-member", [e1, e2])

    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    assert concept_drift_summaries(conn, min_support=2)  # 2 total members -> shown
    assert concept_drift_summaries(conn, min_support=3) == []  # filtered out


def test_build_drift_does_not_touch_memory_nodes_or_edges(conn):
    e1 = _add_event(conn, "alpha beta", WIN0)
    e2 = _add_event(conn, "gamma delta", WIN1)
    _make_concept(conn, "c", [e1, e2])

    def _mem_snapshot():
        nodes = conn.execute(
            "SELECT id, level, node_type, title FROM memory_nodes ORDER BY id"
        ).fetchall()
        edges = conn.execute(
            "SELECT parent_level, parent_id, child_level, child_id, relation "
            "FROM memory_edges ORDER BY id"
        ).fetchall()
        return [tuple(n) for n in nodes], [tuple(e) for e in edges]

    before = _mem_snapshot()
    build_drift(conn, StubEmbedProvider(), window_days=WINDOW_DAYS)
    # The drift layer is additive/separable: it writes only drift_snapshots.
    assert _mem_snapshot() == before
