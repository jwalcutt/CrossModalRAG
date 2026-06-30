from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from crossmodalrag.config import get_concept_sim_threshold
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.embed.store import embed_pending_nodes, load_node_vectors
from crossmodalrag.generate.provider import LLMProvider, LLMUnavailable
from crossmodalrag.memory.store import add_edge, delete_node, insert_node, list_nodes

CONCEPT_CLUSTERING_VERSION = "l3-concepts-v1"
CONCEPT_LEVEL = 3
EVENT_LEVEL = 1
MIN_CONCEPT_SIZE = 2
MAX_TITLE_EVENTS = 8

CONCEPT_NAME_SYSTEM_PROMPT = (
    "You name a cluster of related events with a single short topic label. "
    "Output ONLY the label: a noun phrase of at most 6 words, no quotes, no trailing punctuation."
)


@dataclass(frozen=True)
class ConceptResult:
    concepts_created: int
    concepts_kept: int
    concepts_deleted: int
    events_clustered: int
    events_unclustered: int
    named_by_llm: int
    named_by_fallback: int


def build_concepts(
    conn: sqlite3.Connection,
    embed_provider: EmbeddingProvider,
    llm_provider: LLMProvider | None = None,
    *,
    threshold: float | None = None,
) -> ConceptResult:
    """Cluster L1 events into L3 concepts (semantic, greedy threshold), idempotently.

    Reconciles by fingerprint over the member-event set, so re-running on an
    unchanged L1 layer is a no-op (and matched concepts keep their existing label
    — no re-naming). New concepts are named by the LLM (temp 0) with a
    deterministic fallback when no provider is available or Ollama is unreachable.
    """
    if threshold is None:
        threshold = get_concept_sim_threshold()

    embed_pending_nodes(conn, embed_provider, level=EVENT_LEVEL, node_type="event")
    node_vectors = load_node_vectors(conn, embed_provider.name, level=EVENT_LEVEL)
    vec_by_id = {nid: vec for nid, vec in node_vectors}
    titles = _event_titles(conn)

    clusters = _cluster(node_vectors, threshold)

    desired: dict[str, list[int]] = {}
    events_clustered = 0
    for member_ids in clusters:
        member_ids = sorted(member_ids)
        fingerprint = _concept_fingerprint(threshold, embed_provider.name, member_ids)
        desired[fingerprint] = member_ids
        events_clustered += len(member_ids)

    existing = {
        node.derivation_fingerprint: node.id
        for node in list_nodes(conn, level=CONCEPT_LEVEL, node_type="concept")
        if node.derivation_fingerprint is not None
    }

    created = kept = deleted = named_llm = named_fb = 0
    llm_enabled = llm_provider is not None

    for fingerprint, member_ids in desired.items():
        if fingerprint in existing:
            kept += 1
            continue

        label = None
        if llm_enabled:
            try:
                label = _llm_name(llm_provider, member_ids, titles)
            except LLMUnavailable:
                llm_enabled = False  # stop retrying for the rest of this run
                label = None
        if label:
            naming = "llm"
            named_llm += 1
        else:
            label = _fallback_name(member_ids, vec_by_id, titles)
            naming = "fallback"
            named_fb += 1

        node_id = insert_node(
            conn,
            level=CONCEPT_LEVEL,
            node_type="concept",
            title=label,
            content=_concept_content(member_ids, titles),
            derivation_fingerprint=fingerprint,
            model=embed_provider.name,
            prompt_version=CONCEPT_CLUSTERING_VERSION,
            metadata=json.dumps({"size": len(member_ids), "naming": naming}),
        )
        for member_id in member_ids:
            add_edge(conn, CONCEPT_LEVEL, node_id, EVENT_LEVEL, member_id, "contains")
        created += 1

    for fingerprint, node_id in existing.items():
        if fingerprint not in desired:
            delete_node(conn, node_id)
            deleted += 1

    conn.commit()
    return ConceptResult(
        concepts_created=created,
        concepts_kept=kept,
        concepts_deleted=deleted,
        events_clustered=events_clustered,
        events_unclustered=len(node_vectors) - events_clustered,
        named_by_llm=named_llm,
        named_by_fallback=named_fb,
    )


def list_concept_views(conn: sqlite3.Connection, *, top: int = 20) -> list[dict]:
    """Read-only L3-concept browse view (by centrality). Stable JSON contract for `mem concepts`."""
    rows = conn.execute(
        """
        SELECT n.id AS id, n.title AS title, n.centrality AS centrality,
               COUNT(e.id) AS members
        FROM memory_nodes n
        LEFT JOIN memory_edges e
            ON e.parent_level = 3 AND e.parent_id = n.id AND e.relation = 'contains'
        WHERE n.level = 3 AND n.node_type = 'concept'
        GROUP BY n.id
        ORDER BY n.centrality DESC NULLS LAST, n.id ASC
        LIMIT ?
        """,
        (top,),
    ).fetchall()
    return [
        {
            "node_id": int(r["id"]),
            "title": r["title"],
            "centrality": float(r["centrality"]) if r["centrality"] is not None else 0.0,
            "members": int(r["members"]),
        }
        for r in rows
    ]


def _cluster(node_vectors: list[tuple[int, list[float]]], threshold: float) -> list[list[int]]:
    """Greedy 'leader' threshold clustering. Deterministic by ascending node id."""
    if not node_vectors:
        return []
    import numpy as np

    ids = [nid for nid, _ in node_vectors]
    mat = np.array([vec for _, vec in node_vectors], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    unit = mat / norms

    centroid_sums: list = []  # running sum of unit vectors per cluster
    members: list[list[int]] = []
    for i in range(len(ids)):
        v = unit[i]
        best_j = -1
        best_sim = -1.0
        for j, csum in enumerate(centroid_sums):
            cnorm = float(np.linalg.norm(csum)) or 1.0
            sim = float(v @ (csum / cnorm))
            if sim > best_sim:
                best_sim = sim
                best_j = j
        if best_j >= 0 and best_sim >= threshold:
            members[best_j].append(i)
            centroid_sums[best_j] = centroid_sums[best_j] + v
        else:
            centroid_sums.append(v.copy())
            members.append([i])

    return [[ids[k] for k in group] for group in members if len(group) >= MIN_CONCEPT_SIZE]


def _llm_name(provider: LLMProvider, member_ids: list[int], titles: dict[int, str]) -> str | None:
    listing = "\n".join(f"- {titles.get(mid, '')}" for mid in member_ids if titles.get(mid))
    prompt = f"Events:\n{listing}\n\nTopic label:"
    raw = provider.generate(prompt, system=CONCEPT_NAME_SYSTEM_PROMPT)
    label = raw.strip().strip('"').strip("'").splitlines()[0].strip() if raw.strip() else ""
    return label or None


def _fallback_name(
    member_ids: list[int],
    vec_by_id: dict[int, list[float]],
    titles: dict[int, str],
) -> str:
    central_id = _central_member(member_ids, vec_by_id)
    base = titles.get(central_id) or titles.get(member_ids[0]) or "Concept"
    extra = len(member_ids) - 1
    return f"{base} (+{extra} related)" if extra > 0 else base


def _central_member(member_ids: list[int], vec_by_id: dict[int, list[float]]) -> int:
    vectors = [(mid, vec_by_id[mid]) for mid in member_ids if mid in vec_by_id]
    if not vectors:
        return member_ids[0]
    import numpy as np

    mat = np.array([v for _, v in vectors], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    unit = mat / norms
    centroid = unit.mean(axis=0)
    cnorm = float(np.linalg.norm(centroid)) or 1.0
    sims = unit @ (centroid / cnorm)
    return vectors[int(sims.argmax())][0]


def _concept_content(member_ids: list[int], titles: dict[int, str]) -> str:
    names = [titles[mid] for mid in member_ids if titles.get(mid)]
    shown = names[:MAX_TITLE_EVENTS]
    text = "; ".join(shown)
    if len(names) > MAX_TITLE_EVENTS:
        text += f"; … (+{len(names) - MAX_TITLE_EVENTS} more)"
    return text


def _event_titles(conn: sqlite3.Connection) -> dict[int, str]:
    return {node.id: str(node.title or "") for node in list_nodes(conn, level=EVENT_LEVEL, node_type="event")}


def _concept_fingerprint(threshold: float, model: str, member_ids: list[int]) -> str:
    payload = (
        f"{CONCEPT_CLUSTERING_VERSION}\x1f{threshold:.4f}\x1f{model}\x1f"
        f"{','.join(str(i) for i in member_ids)}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
