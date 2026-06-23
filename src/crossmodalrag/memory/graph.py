from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from itertools import combinations

from crossmodalrag.memory.store import add_edge

CONCEPT_LEVEL = 3
EPISODE_LEVEL = 2
EVENT_LEVEL = 1
PAGERANK_DAMPING = 0.85
PAGERANK_ITERS = 50


@dataclass(frozen=True)
class GraphResult:
    relates_edges_created: int
    relates_edges_deleted: int
    nodes_scored: int


def build_graph(conn: sqlite3.Connection) -> GraphResult:
    """Recompute the concept co-occurrence graph and node centrality.

    Pure function of the current hierarchy (deterministic, idempotent). Runs
    co-occurrence first so the new `relates_to` edges feed into centrality. Needs
    no LLM or embeddings.
    """
    created, deleted = build_concept_cooccurrence(conn)
    nodes_scored = compute_centrality(conn)
    conn.commit()
    return GraphResult(
        relates_edges_created=created,
        relates_edges_deleted=deleted,
        nodes_scored=nodes_scored,
    )


def build_concept_cooccurrence(conn: sqlite3.Connection) -> tuple[int, int]:
    """Link concepts that share an L2 episode via weighted `relates_to` edges.

    Two concepts co-occur if they have member events in the same episode; the
    edge weight is the number of shared episodes. One canonical edge per pair
    (parent = smaller concept id).
    """
    deleted = _delete_concept_relates_edges(conn)

    event_to_concepts: dict[int, set[int]] = {}
    for row in conn.execute(
        "SELECT parent_id AS concept_id, child_id AS event_id FROM memory_edges "
        "WHERE relation = 'contains' AND parent_level = ? AND child_level = ?",
        (CONCEPT_LEVEL, EVENT_LEVEL),
    ).fetchall():
        event_to_concepts.setdefault(int(row["event_id"]), set()).add(int(row["concept_id"]))

    episode_events: dict[int, list[int]] = {}
    for row in conn.execute(
        "SELECT parent_id AS episode_id, child_id AS event_id FROM memory_edges "
        "WHERE relation = 'contains' AND parent_level = ? AND child_level = ?",
        (EPISODE_LEVEL, EVENT_LEVEL),
    ).fetchall():
        episode_events.setdefault(int(row["episode_id"]), []).append(int(row["event_id"]))

    pair_counts: dict[tuple[int, int], int] = {}
    for events in episode_events.values():
        concepts_present: set[int] = set()
        for event_id in events:
            concepts_present |= event_to_concepts.get(event_id, set())
        for a, b in combinations(sorted(concepts_present), 2):
            pair_counts[(a, b)] = pair_counts.get((a, b), 0) + 1

    for (a, b), weight in pair_counts.items():
        add_edge(conn, CONCEPT_LEVEL, a, CONCEPT_LEVEL, b, "relates_to", weight=float(weight))

    return len(pair_counts), deleted


def compute_centrality(conn: sqlite3.Connection) -> int:
    """Compute normalized PageRank over the memory-node graph and store it.

    Undirected graph over memory_nodes (levels 1-3) using `contains` and
    `relates_to` edges (`derived_from`/L0 excluded). Scores normalized to [0,1].
    """
    node_ids = sorted(int(r["id"]) for r in conn.execute("SELECT id FROM memory_nodes").fetchall())
    if not node_ids:
        return 0

    adjacency: dict[int, list[tuple[int, float]]] = {nid: [] for nid in node_ids}
    for row in conn.execute(
        "SELECT parent_id, child_id, weight FROM memory_edges "
        "WHERE relation IN ('contains', 'relates_to') "
        "AND parent_level BETWEEN 1 AND 3 AND child_level BETWEEN 1 AND 3"
    ).fetchall():
        a = int(row["parent_id"])
        b = int(row["child_id"])
        w = float(row["weight"]) if row["weight"] is not None else 1.0
        if a in adjacency and b in adjacency:
            adjacency[a].append((b, w))
            adjacency[b].append((a, w))

    ranks = _pagerank(node_ids, adjacency)
    max_rank = max(ranks.values()) or 1.0
    for nid in node_ids:
        conn.execute(
            "UPDATE memory_nodes SET centrality = ? WHERE id = ?",
            (ranks[nid] / max_rank, nid),
        )
    return len(node_ids)


def _pagerank(
    node_ids: list[int],
    adjacency: dict[int, list[tuple[int, float]]],
    damping: float = PAGERANK_DAMPING,
    iters: int = PAGERANK_ITERS,
) -> dict[int, float]:
    n = len(node_ids)
    rank = {nid: 1.0 / n for nid in node_ids}
    base = (1.0 - damping) / n
    for _ in range(iters):
        new = {nid: base for nid in node_ids}
        dangling_mass = 0.0
        for nid in node_ids:
            neighbors = adjacency[nid]
            total_w = sum(w for _, w in neighbors)
            if total_w == 0.0:
                dangling_mass += rank[nid]
                continue
            for neighbor, w in neighbors:
                new[neighbor] += damping * rank[nid] * (w / total_w)
        if dangling_mass:
            share = damping * dangling_mass / n
            for nid in node_ids:
                new[nid] += share
        rank = new
    return rank


def _delete_concept_relates_edges(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "DELETE FROM memory_edges WHERE relation = 'relates_to' "
        "AND parent_level = ? AND child_level = ?",
        (CONCEPT_LEVEL, CONCEPT_LEVEL),
    )
    return cur.rowcount if cur.rowcount is not None else 0
