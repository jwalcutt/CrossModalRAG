from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.usage.store import (
    clear_usage_events,
    list_usage_events,
    record_usage_event,
    usage_summaries,
)
from crossmodalrag.usage.strength import summarize

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def test_usage_events_table_exists_after_init(conn):
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(usage_events)").fetchall()}
    assert {"target_kind", "target_id", "event_type", "weight", "event_at", "created_at"} <= cols


def test_record_preserves_event_at_and_default_weight(conn):
    record_usage_event(conn, "chunk", 7, "accepted_answer", event_at="2026-06-01T00:00:00+00:00")
    (event,) = list_usage_events(conn)
    assert event.target_kind == "chunk"
    assert event.target_id == 7
    assert event.event_type == "accepted_answer"
    assert event.event_at == "2026-06-01T00:00:00+00:00"
    assert event.weight == 3.0  # EVENT_WEIGHTS default


def test_explicit_weight_overrides_default(conn):
    record_usage_event(conn, "node", 3, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00", weight=9.0)
    (event,) = list_usage_events(conn)
    assert event.weight == 9.0


def test_list_filters_by_target(conn):
    record_usage_event(conn, "chunk", 1, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00")
    record_usage_event(conn, "chunk", 2, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00")
    record_usage_event(conn, "node", 1, "open", event_at="2026-06-01T00:00:00+00:00")

    assert len(list_usage_events(conn, target_kind="chunk")) == 2
    assert len(list_usage_events(conn, target_kind="chunk", target_id=1)) == 1
    assert len(list_usage_events(conn, target_kind="node")) == 1


def test_usage_summaries_matches_summarize(conn):
    record_usage_event(conn, "chunk", 1, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00")
    record_usage_event(conn, "chunk", 1, "open", event_at="2026-06-20T00:00:00+00:00")
    record_usage_event(conn, "chunk", 2, "retrieval_hit", event_at="2026-01-15T00:00:00+00:00")

    summaries = usage_summaries(conn, now=NOW, halflife_days=30)
    assert set(summaries) == {("chunk", 1), ("chunk", 2)}

    events_1 = list_usage_events(conn, target_kind="chunk", target_id=1)
    expected = summarize(events_1, now=NOW, halflife_days=30)
    assert summaries[("chunk", 1)].strength == pytest.approx(expected.strength)
    assert summaries[("chunk", 1)].count == 2
    # Reinforced target outranks the stale one.
    assert summaries[("chunk", 1)].strength > summaries[("chunk", 2)].strength


def test_clear_scoped_and_global(conn):
    record_usage_event(conn, "chunk", 1, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00")
    record_usage_event(conn, "chunk", 2, "retrieval_hit", event_at="2026-06-01T00:00:00+00:00")

    assert clear_usage_events(conn, target_kind="chunk", target_ids=[1]) == 1
    assert {e.target_id for e in list_usage_events(conn)} == {2}

    assert clear_usage_events(conn) == 1
    assert list_usage_events(conn) == []


def test_init_db_is_additive_for_usage_events(conn):
    # Simulate a pre-Phase-4 DB: drop the table, re-init, confirm it returns without
    # touching the other tables.
    conn.execute("INSERT INTO sources (source_type, source_uri) VALUES ('note', '/x.md')")
    conn.execute("DROP TABLE usage_events")
    conn.commit()

    init_db(conn)

    assert conn.execute("PRAGMA table_info(usage_events)").fetchall()  # re-created
    assert conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 1  # untouched
