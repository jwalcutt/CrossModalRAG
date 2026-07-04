from __future__ import annotations

import hashlib
import re

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.embed.store import count_node_embeddings, embed_pending_nodes
from crossmodalrag.memory.concepts import build_concepts
from crossmodalrag.memory.integrity import find_dangling_edges, find_unsupported_nodes
from crossmodalrag.memory.store import add_edge, delete_node, get_children, list_nodes, resolve_to_evidence

WORD_RE = re.compile(r"[a-z0-9]+")
THRESHOLD = 0.4


class StubEmbedProvider:
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


class StubLLMProvider:
    def __init__(self, label: str = "Parser bounds work", name: str = "stub-llm") -> None:
        self.name = name
        self._label = label
        self.calls = 0

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.calls += 1
        return self._label


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _add_event(conn, title: str) -> tuple[int, int]:
    cur = conn.execute("INSERT INTO sources (source_type, source_uri) VALUES ('note', ?)", (f"/v/{title}.md",))
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, 0, ?)",
        (sid, f"chunk {title}"),
    )
    chunk_id = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO memory_nodes (level, node_type, title) VALUES (1, 'event', ?)", (title,)
    )
    event_id = int(cur.lastrowid)
    add_edge(conn, 1, event_id, 0, chunk_id, "derived_from")
    conn.commit()
    return event_id, chunk_id


def _snapshot(conn):
    nodes = conn.execute(
        "SELECT id, level, node_type, title, derivation_fingerprint FROM memory_nodes ORDER BY id"
    ).fetchall()
    edges = conn.execute(
        "SELECT parent_level, parent_id, child_level, child_id, relation FROM memory_edges ORDER BY id"
    ).fetchall()
    return [tuple(n) for n in nodes], [tuple(e) for e in edges]


def test_clusters_similar_events_and_grounds(conn) -> None:
    e1, c1 = _add_event(conn, "parser bounds fix")
    e2, c2 = _add_event(conn, "parser bounds check bug")
    e3, c3 = _add_event(conn, "cooking pasta recipe")

    embed_pending_nodes(conn, StubEmbedProvider(), level=1, node_type="event")
    result = build_concepts(conn, StubEmbedProvider(), StubLLMProvider(), threshold=THRESHOLD)

    assert result.concepts_created == 1
    assert result.events_clustered == 2
    assert result.events_unclustered == 1

    concepts = list_nodes(conn, level=3, node_type="concept")
    assert len(concepts) == 1
    concept = concepts[0]
    assert concept.title == "Parser bounds work"
    members = {cid for (lvl, cid) in get_children(conn, 3, concept.id, relation="contains")}
    assert members == {e1, e2}
    assert resolve_to_evidence(conn, 3, concept.id) == sorted([c1, c2])

    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []


def test_build_concepts_idempotent_no_rename(conn) -> None:
    _add_event(conn, "parser bounds fix")
    _add_event(conn, "parser bounds check bug")
    embed = StubEmbedProvider()
    llm = StubLLMProvider()
    embed_pending_nodes(conn, embed, level=1, node_type="event")

    first = build_concepts(conn, embed, llm, threshold=THRESHOLD)
    assert first.concepts_created == 1
    calls_after_first = llm.calls
    snap1 = _snapshot(conn)

    second = build_concepts(conn, embed, llm, threshold=THRESHOLD)
    assert second.concepts_created == 0
    assert second.concepts_kept == 1
    assert llm.calls == calls_after_first  # matched concept not re-named
    assert _snapshot(conn) == snap1


def test_fallback_naming_without_llm(conn) -> None:
    _add_event(conn, "parser bounds fix")
    _add_event(conn, "parser bounds check bug")
    embed = StubEmbedProvider()
    embed_pending_nodes(conn, embed, level=1, node_type="event")

    result = build_concepts(conn, embed, None, threshold=THRESHOLD)
    assert result.named_by_fallback == 1
    assert result.named_by_llm == 0
    concept = list_nodes(conn, level=3, node_type="concept")[0]
    assert "(+1 related)" in concept.title


def test_l1_change_rebuilds_concepts(conn) -> None:
    e1, _ = _add_event(conn, "parser bounds fix")
    e2, _ = _add_event(conn, "parser bounds check bug")
    embed = StubEmbedProvider()
    embed_pending_nodes(conn, embed, level=1, node_type="event")
    build_concepts(conn, embed, StubLLMProvider(), threshold=THRESHOLD)

    # Remove one member -> the remaining singleton can't form a concept.
    delete_node(conn, e2)
    result = build_concepts(conn, embed, StubLLMProvider(), threshold=THRESHOLD)
    assert result.concepts_deleted == 1
    assert result.concepts_created == 0
    assert list_nodes(conn, level=3, node_type="concept") == []
    assert find_dangling_edges(conn) == []


def test_embed_pending_nodes_idempotent_and_model_aware(conn) -> None:
    _add_event(conn, "alpha beta")
    _add_event(conn, "gamma delta")
    provider = StubEmbedProvider()
    assert embed_pending_nodes(conn, provider, level=1, node_type="event") == 2
    assert embed_pending_nodes(conn, provider, level=1, node_type="event") == 0
    assert count_node_embeddings(conn, model=provider.name) == 2

    other = StubEmbedProvider(name="stub-embed-v2")
    assert embed_pending_nodes(conn, other, level=1, node_type="event") == 2


class RecordingLLMProvider(StubLLMProvider):
    def __init__(self, label: str) -> None:
        super().__init__(label=label)
        self.prompts: list[str] = []

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.prompts.append(prompt)
        return super().generate(prompt, system=system)


def test_prose_label_falls_back_to_deterministic_name(conn) -> None:
    # Regression: a rambling model response used to be stored verbatim as the
    # concept title ("It appears that the text is a collection of notes ...").
    _add_event(conn, "parser bounds fix")
    _add_event(conn, "parser bounds check bug")
    embed = StubEmbedProvider()
    embed_pending_nodes(conn, embed, level=1, node_type="event")
    prose = "It appears that the text is a collection of notes and explanations on various topics, including:"

    result = build_concepts(conn, embed, StubLLMProvider(label=prose), threshold=THRESHOLD)

    assert result.named_by_llm == 0
    assert result.named_by_fallback == 1
    concept = list_nodes(conn, level=3, node_type="concept")[0]
    assert concept.title != prose
    assert "(+1 related)" in concept.title


@pytest.mark.parametrize(
    "label,valid",
    [
        ("Parser bounds work", True),
        ("Fourier transform theory", True),
        ("It appears that the text is a collection of notes", False),
        ("This is a list of changes made to the project:", False),
        ("A very long label that keeps going and going beyond any reasonable topic name", False),
        ("Trailing colon:", False),
        ("", False),
    ],
)
def test_valid_label_rules(label: str, valid: bool) -> None:
    from crossmodalrag.memory.concepts import _valid_label

    assert _valid_label(label) is valid


def test_naming_prompt_uses_representative_sample(conn) -> None:
    # A big cluster must not dump every member title into the naming prompt —
    # oversized listings are what pushed the model into prose answers.
    from crossmodalrag.memory.concepts import MAX_TITLE_EVENTS

    for i in range(MAX_TITLE_EVENTS * 3):
        _add_event(conn, f"parser bounds fix variant {i}")
    embed = StubEmbedProvider()
    embed_pending_nodes(conn, embed, level=1, node_type="event")
    llm = RecordingLLMProvider(label="Parser bounds work")

    result = build_concepts(conn, embed, llm, threshold=THRESHOLD)

    assert result.concepts_created >= 1
    assert llm.prompts
    for prompt in llm.prompts:
        listed = [line for line in prompt.splitlines() if line.startswith("- ")]
        assert 0 < len(listed) <= MAX_TITLE_EVENTS


def test_default_concept_threshold_is_anisotropy_aware(monkeypatch) -> None:
    from crossmodalrag.config import get_concept_sim_threshold

    monkeypatch.delenv("CMRAG_CONCEPT_SIM_THRESHOLD", raising=False)
    assert get_concept_sim_threshold() == 0.80
    monkeypatch.setenv("CMRAG_CONCEPT_SIM_THRESHOLD", "0.65")
    assert get_concept_sim_threshold() == 0.65
