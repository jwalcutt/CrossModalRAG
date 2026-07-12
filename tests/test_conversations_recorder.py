"""SessionRecorder tests: lazy creation, abstained-turn persistence, /new rotation,
disabled inertness, and the best-effort never-raises guarantee."""

from __future__ import annotations

import json

import pytest

from crossmodalrag.conversations.recorder import SessionRecorder
from crossmodalrag.conversations.store import list_conversations, list_messages
from crossmodalrag.db import connect, init_db
from crossmodalrag.generate.synthesize import (
    ABSTAIN_LLM_INSUFFICIENT,
    INSUFFICIENT_EVIDENCE_TEXT,
    GeneratedAnswer,
)
from crossmodalrag.retrieve.lexical import RetrievalHit


def _hit(chunk_id: int = 1) -> RetrievalHit:
    return RetrievalHit(
        chunk_id=chunk_id,
        source_id=chunk_id,
        source_type="note",
        source_uri="/abs/x.md",
        source_timestamp="2026-06-01T00:00:00+00:00",
        title="x",
        chunk_index=0,
        chunk_text="alpha beta",
        score=0.9,
        lexical_score=0.9,
        recency_score=0.5,
        vector_score=0.0,
    )


def _answer(text: str = "X is y [E1].", *, abstained: bool = False) -> GeneratedAnswer:
    hit = _hit()
    return GeneratedAnswer(
        query="q",
        answer_text=text,
        cited_evidence_ids=[] if abstained else ["E1"],
        invalid_citations=[],
        evidence=[hit],
        abstained=abstained,
        model="stub-llm",
        id_map={"E1": hit},
        abstention_reason=ABSTAIN_LLM_INSUFFICIENT if abstained else None,
    )


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "memory.db"
    conn = connect(path)
    init_db(conn)
    conn.close()
    return path


def _now_seq():
    counter = iter(range(1000))
    return lambda: f"2026-07-11T10:{next(counter):02d}:00+00:00"


def _read(db_path):
    conn = connect(db_path)
    try:
        convs = list_conversations(conn)
        return convs, {c.id: list_messages(conn, c.id) for c in convs}
    finally:
        conn.close()


def test_lazy_creation_no_rows_until_first_turn(db_path):
    recorder = SessionRecorder(db_path, enabled=True, now_fn=_now_seq())
    assert recorder.conversation_id is None
    convs, _ = _read(db_path)
    assert convs == []  # opening a session writes nothing

    recorder.record_turn("what is x, exactly?", _answer())
    convs, messages = _read(db_path)
    assert len(convs) == 1
    assert convs[0].title == "what is x, exactly?"
    assert [m.role for m in messages[convs[0].id]] == ["user", "assistant"]
    assistant = messages[convs[0].id][1]
    assert json.loads(assistant.evidence_json)[0]["evidence_id"] == "E1"
    assert assistant.model == "stub-llm"


def test_abstained_turn_persisted_with_reason(db_path):
    recorder = SessionRecorder(db_path, enabled=True, now_fn=_now_seq())
    recorder.record_turn("unanswerable?", _answer(INSUFFICIENT_EVIDENCE_TEXT, abstained=True))
    convs, messages = _read(db_path)
    assistant = messages[convs[0].id][1]
    assert assistant.abstention_reason == "llm_insufficient"
    assert assistant.text == INSUFFICIENT_EVIDENCE_TEXT


def test_turn_indexes_increment_and_new_conversation_rotates(db_path):
    recorder = SessionRecorder(db_path, enabled=True, now_fn=_now_seq())
    recorder.record_turn("q1", _answer())
    recorder.record_turn("q2", _answer())
    first_id = recorder.conversation_id

    recorder.new_conversation()
    assert recorder.conversation_id is None
    recorder.record_turn("q3", _answer())

    convs, messages = _read(db_path)
    assert len(convs) == 2
    assert [m.turn_index for m in messages[first_id]] == [0, 0, 1, 1]
    second_id = next(c.id for c in convs if c.id != first_id)
    assert [m.turn_index for m in messages[second_id]] == [0, 0]
    assert messages[second_id][0].text == "q3"


def test_disabled_recorder_is_inert(db_path):
    recorder = SessionRecorder(db_path, enabled=False, now_fn=_now_seq())
    recorder.record_turn("q", _answer())
    convs, _ = _read(db_path)
    assert convs == []
    assert recorder.conversation_id is None


def test_record_turn_never_raises(db_path, monkeypatch, capsys):
    import crossmodalrag.conversations.recorder as recorder_mod

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(recorder_mod, "record_message", boom)
    recorder = SessionRecorder(db_path, enabled=True, now_fn=_now_seq())
    recorder.record_turn("q", _answer())  # must not raise
    assert "history save skipped" in capsys.readouterr().err
