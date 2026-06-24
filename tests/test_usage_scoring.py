from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.embed.store import embed_pending_chunks
from crossmodalrag.retrieve import hybrid
from crossmodalrag.usage.store import record_usage_event
from crossmodalrag.usage.strength import normalize_strength

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)
WORD_RE = re.compile(r"[a-zA-Z0-9_]+")


class StubProvider:
    """Deterministic hashed-token embeddings (mirrors tests/test_embeddings.py)."""

    def __init__(self, dim: int = 64, name: str = "stub-usage-v1") -> None:
        self.dim = dim
        self.name = name

    def embed(self, texts: list[str]) -> list[list[float]]:
        out = []
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


def _source(conn, uri, ts="2026-06-01T00:00:00+00:00"):
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", uri, ts, uri),
    )
    return int(cur.lastrowid)


def _chunk(conn, sid, idx, text):
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (sid, idx, text),
    )
    return int(cur.lastrowid)


# --- normalize_strength -------------------------------------------------------


def test_normalize_strength_bounds_and_monotonic():
    assert normalize_strength(0.0, saturation=3.0) == 0.0
    assert normalize_strength(3.0, saturation=3.0) == pytest.approx(0.5)
    assert 0.0 < normalize_strength(1.0, saturation=3.0) < normalize_strength(10.0, saturation=3.0) < 1.0


# --- ablation / separability --------------------------------------------------


def test_non_usage_profiles_are_unaffected_by_usage_events(conn):
    sid = _source(conn, "note-a")
    c1 = _chunk(conn, sid, 0, "alpha beta gamma signal")
    _chunk(conn, sid, 1, "delta epsilon zeta other")
    conn.commit()
    provider = StubProvider()
    embed_pending_chunks(conn, provider)

    baseline = hybrid.retrieve(conn, "alpha beta gamma", top_k=5, profile="relevant", provider=provider, now=NOW)
    baseline_scores = [(h.chunk_id, round(h.score, 6)) for h in baseline]

    # Add a pile of usage to c1 — must NOT change the (usage=0) `relevant` profile at all.
    for _ in range(5):
        record_usage_event(conn, "chunk", c1, "accepted_answer", event_at="2026-07-01T00:00:00+00:00")

    after = hybrid.retrieve(conn, "alpha beta gamma", top_k=5, profile="relevant", provider=provider, now=NOW)
    assert [(h.chunk_id, round(h.score, 6)) for h in after] == baseline_scores
    assert all(h.usage_score == 0.0 for h in after)  # usage term off for `relevant`


# --- the usage profile actually re-ranks --------------------------------------


def test_usage_profile_promotes_reinforced_chunk_on_a_tie(conn, monkeypatch):
    # Disable dedupe so the identical-text tie pair both survive.
    monkeypatch.setenv("CMRAG_DEDUPE_THRESHOLD", "1.5")

    sid_a = _source(conn, "note-a")
    sid_b = _source(conn, "note-b")
    text = "memory retrieval ranking signal"
    c_a = _chunk(conn, sid_a, 0, text)
    c_b = _chunk(conn, sid_b, 0, text)  # identical content + timestamp -> perfect tie
    conn.commit()
    provider = StubProvider()
    embed_pending_chunks(conn, provider)

    # Without usage: a tie; the usage term is off, order is stable (insertion / id order).
    relevant = hybrid.retrieve(conn, text, top_k=2, profile="relevant", provider=provider, now=NOW)
    assert {h.chunk_id for h in relevant} == {c_a, c_b}
    assert relevant[0].score == pytest.approx(relevant[1].score)

    # Reinforce c_b heavily and recently.
    for at in ("2026-06-25T00:00:00+00:00", "2026-06-30T00:00:00+00:00"):
        record_usage_event(conn, "chunk", c_b, "accepted_answer", event_at=at)

    usage_hits = hybrid.retrieve(conn, text, top_k=2, profile="usage", provider=provider, now=NOW)
    assert usage_hits[0].chunk_id == c_b  # reinforced chunk wins under the usage profile
    assert usage_hits[0].usage_score > 0.0
    assert usage_hits[0].score > usage_hits[1].score


def test_unknown_profile_still_raises(conn):
    with pytest.raises(ValueError, match="Unknown profile"):
        hybrid.retrieve(conn, "anything", profile="nonsense", provider=StubProvider())
