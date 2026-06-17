from __future__ import annotations

import json

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.extract import (
    EVENT_PROMPT_VERSION,
    extract_events_for_source,
    extract_pending_sources,
)
from crossmodalrag.memory.integrity import find_dangling_edges, find_unsupported_nodes
from crossmodalrag.memory.store import list_nodes, resolve_to_evidence


class StubLLMProvider:
    """Deterministic stub: returns a fixed JSON event array (no Ollama needed)."""

    def __init__(self, output: str | None = None, name: str = "stub-extract") -> None:
        self.name = name
        self._output = (
            output
            if output is not None
            else json.dumps(
                [
                    {"title": "Decided on schema", "summary": "Chose polymorphic edges."},
                    {"title": "Added drill-down", "summary": "Implemented resolve_to_evidence."},
                ]
            )
        )
        self.calls = 0

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.calls += 1
        return self._output


def _new_db(tmp_path):
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    return conn


def _add_source(conn, uri: str, chunks: list[str], ts: str = "2026-06-01T00:00:00+00:00") -> int:
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", uri, ts, uri),
    )
    sid = int(cur.lastrowid)
    for idx, text in enumerate(chunks):
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
            (sid, idx, text),
        )
    conn.commit()
    return sid


def _snapshot(conn):
    nodes = conn.execute(
        "SELECT level, node_type, title, content, derivation_fingerprint, model, prompt_version "
        "FROM memory_nodes ORDER BY id"
    ).fetchall()
    edges = conn.execute(
        "SELECT parent_level, parent_id, child_level, child_id, relation FROM memory_edges ORDER BY id"
    ).fetchall()
    return [tuple(n) for n in nodes], [tuple(e) for e in edges]


def test_extract_creates_grounded_events(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a", ["first body", "second body"])
    provider = StubLLMProvider()

    created = extract_events_for_source(conn, provider, sid)
    assert created == 2

    events = list_nodes(conn, level=1, node_type="event")
    assert len(events) == 2
    chunk_ids = [int(r["id"]) for r in conn.execute(
        "SELECT id FROM evidence_chunks WHERE source_id = ? ORDER BY id", (sid,)
    )]
    for event in events:
        assert resolve_to_evidence(conn, 1, event.id) == sorted(chunk_ids)
        assert event.model == "stub-extract"
        assert event.prompt_version == EVENT_PROMPT_VERSION
        assert event.derivation_fingerprint
        assert json.loads(event.metadata_json)["source_id"] == sid

    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []


def test_extract_pending_is_deterministic_no_op_on_rerun(tmp_path) -> None:
    conn = _new_db(tmp_path)
    _add_source(conn, "note-a", ["alpha"])
    _add_source(conn, "note-b", ["beta"])
    provider = StubLLMProvider()

    first = extract_pending_sources(conn, provider)
    assert first.sources_processed == 2
    assert first.events_created == 4
    snap1 = _snapshot(conn)

    second = extract_pending_sources(conn, provider)
    assert second.sources_processed == 0
    assert second.sources_skipped == 2
    assert second.events_created == 0
    assert _snapshot(conn) == snap1  # identical nodes/edges


def test_changed_source_triggers_reextraction(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a", ["original body"])
    provider = StubLLMProvider()
    extract_pending_sources(conn, provider)

    # Change the source content -> fingerprint changes -> re-extract.
    conn.execute("UPDATE evidence_chunks SET chunk_text = ? WHERE source_id = ?", ("new body", sid))
    conn.commit()

    result = extract_pending_sources(conn, provider)
    assert result.sources_processed == 1
    assert find_dangling_edges(conn) == []
    # Still exactly the stub's two events for the one source (old ones replaced).
    assert len(list_nodes(conn, level=1, node_type="event")) == 2


def test_prompt_version_change_triggers_reextraction(tmp_path) -> None:
    conn = _new_db(tmp_path)
    _add_source(conn, "note-a", ["alpha"])
    provider = StubLLMProvider()
    extract_pending_sources(conn, provider, prompt_version="v1")
    again = extract_pending_sources(conn, provider, prompt_version="v2")
    assert again.sources_processed == 1


def test_malformed_output_is_graceful(tmp_path) -> None:
    conn = _new_db(tmp_path)
    sid = _add_source(conn, "note-a", ["alpha"])
    provider = StubLLMProvider(output="I could not produce JSON, sorry.")

    result = extract_pending_sources(conn, provider)
    assert result.events_created == 0
    assert result.parse_failures == 1
    assert list_nodes(conn, level=1, node_type="event") == []
    assert find_dangling_edges(conn) == []

    # Unparseable sources are not recorded, so they are retried (not skipped) next run.
    retry = extract_pending_sources(conn, provider)
    assert retry.sources_processed == 1


def test_empty_event_array_is_recorded_and_skipped(tmp_path) -> None:
    conn = _new_db(tmp_path)
    _add_source(conn, "note-a", ["alpha"])
    provider = StubLLMProvider(output="[]")

    first = extract_pending_sources(conn, provider)
    assert first.events_created == 0
    assert first.parse_failures == 0
    # A valid empty result IS recorded, so re-running skips it (no wasted LLM call).
    second = extract_pending_sources(conn, provider)
    assert second.sources_processed == 0
    assert second.sources_skipped == 1


def test_limit_bounds_sources_processed(tmp_path) -> None:
    conn = _new_db(tmp_path)
    _add_source(conn, "note-a", ["a"])
    _add_source(conn, "note-b", ["b"])
    _add_source(conn, "note-c", ["c"])
    provider = StubLLMProvider()

    result = extract_pending_sources(conn, provider, limit=1)
    assert result.sources_processed == 1
    # Remaining two are picked up on subsequent runs.
    result2 = extract_pending_sources(conn, provider, limit=5)
    assert result2.sources_processed == 2
