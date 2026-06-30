from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from crossmodalrag.config import get_drift_window_days
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.embed.store import embed_pending_nodes, load_node_vectors, pack_vector
from crossmodalrag.memory.store import (
    get_children,
    get_node,
    list_nodes,
    resolve_to_evidence,
)

DRIFT_VERSION = "drift-v1"
CONCEPT_LEVEL = 3
EVENT_LEVEL = 1
CONFIDENCE_SUPPORT_FULL = 3.0


@dataclass(frozen=True)
class DriftResult:
    concepts_analyzed: int
    snapshots_created: int
    snapshots_kept: int
    snapshots_deleted: int


@dataclass(frozen=True)
class DriftSummary:
    concept_id: int
    title: str | None
    overall_drift: float          # max centroid movement between consecutive windows
    window_count: int
    support: int                  # total member events across windows
    confidence: float             # min(1, support / CONFIDENCE_SUPPORT_FULL)
    relearning: bool              # active windows separated by >=1 empty window (engagement recurs)
    span_start: str | None
    span_end: str | None
    evidence_source_uri: str | None


def build_drift(
    conn: sqlite3.Connection,
    embed_provider: EmbeddingProvider,
    *,
    window_days: float | None = None,
    now: datetime | None = None,  # accepted for signature symmetry; windowing is data-anchored
) -> DriftResult:
    """Compute per-concept, per-time-window drift snapshots, idempotently.

    For each L3 concept, its member L1 events are bucketed into fixed-length windows (anchored at the
    concept's earliest member event), a per-window prototype (centroid of member-event embeddings) is
    computed, and the drift metric for each window is ``1 - cosine(prototype, previous_prototype)``.
    Reconciled by fingerprint over (version, window length, model, concept, window index + bounds,
    member set) so re-running on unchanged inputs is a byte-identical no-op. Deterministic, no LLM.
    """
    if window_days is None:
        window_days = get_drift_window_days()
    window_seconds = int(window_days * 86400)

    # Ensure L1 event vectors exist for the active model (mirrors build_concepts).
    embed_pending_nodes(conn, embed_provider, level=EVENT_LEVEL, node_type="event")
    vec_by_id = {nid: vec for nid, vec in load_node_vectors(conn, embed_provider.name, level=EVENT_LEVEL)}
    time_by_id = {
        node.id: node.time_start
        for node in list_nodes(conn, level=EVENT_LEVEL, node_type="event")
    }

    concepts = list_nodes(conn, level=CONCEPT_LEVEL, node_type="concept")
    desired: dict[str, tuple] = {}
    for concept in concepts:
        for snap in _concept_snapshots(
            conn, concept.id, window_seconds, embed_provider.name, vec_by_id, time_by_id
        ):
            desired[snap["fingerprint"]] = snap

    existing = {
        str(row["derivation_fingerprint"]): int(row["id"])
        for row in conn.execute(
            "SELECT id, derivation_fingerprint FROM drift_snapshots "
            "WHERE derivation_fingerprint IS NOT NULL"
        ).fetchall()
    }

    created = kept = 0
    for fingerprint, snap in desired.items():
        if fingerprint in existing:
            kept += 1
            continue
        conn.execute(
            """
            INSERT INTO drift_snapshots
                (concept_id, window_start, window_end, prototype_dim, prototype_vector,
                 drift_metric, support, derivation_fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snap["concept_id"],
                snap["window_start"],
                snap["window_end"],
                snap["dim"],
                snap["vector"],
                snap["drift_metric"],
                snap["support"],
                fingerprint,
            ),
        )
        created += 1

    deleted = 0
    for fingerprint, row_id in existing.items():
        if fingerprint not in desired:
            conn.execute("DELETE FROM drift_snapshots WHERE id = ?", (row_id,))
            deleted += 1

    conn.commit()
    return DriftResult(
        concepts_analyzed=len(concepts),
        snapshots_created=created,
        snapshots_kept=kept,
        snapshots_deleted=deleted,
    )


def _concept_snapshots(
    conn: sqlite3.Connection,
    concept_id: int,
    window_seconds: int,
    model: str,
    vec_by_id: dict[int, list[float]],
    time_by_id: dict[int, str | None],
) -> list[dict]:
    """Build the desired drift-snapshot rows for one concept (empty when it can't be windowed)."""
    member_ids = [cid for lvl, cid in get_children(conn, CONCEPT_LEVEL, concept_id, "contains") if lvl == EVENT_LEVEL]
    dated: list[tuple[datetime, int]] = []
    for mid in member_ids:
        dt = _parse_ts(time_by_id.get(mid))
        if dt is not None and mid in vec_by_id:
            dated.append((dt, mid))
    if not dated:
        return []

    t0 = min(dt for dt, _ in dated)
    buckets: dict[int, list[int]] = {}
    for dt, mid in dated:
        idx = int((dt - t0).total_seconds() // window_seconds)
        buckets.setdefault(idx, []).append(mid)

    snapshots: list[dict] = []
    prev_centroid = None
    for idx in sorted(buckets):
        window_member_ids = sorted(buckets[idx])
        centroid = _centroid([vec_by_id[mid] for mid in window_member_ids])
        window_start = (t0 + timedelta(seconds=idx * window_seconds)).isoformat()
        window_end = (t0 + timedelta(seconds=(idx + 1) * window_seconds)).isoformat()
        drift_metric = 0.0 if prev_centroid is None else _cosine_distance(centroid, prev_centroid)
        fingerprint = _snapshot_fingerprint(
            window_seconds, model, concept_id, idx, window_start, window_member_ids
        )
        snapshots.append(
            {
                "fingerprint": fingerprint,
                "concept_id": concept_id,
                "window_start": window_start,
                "window_end": window_end,
                "dim": len(centroid),
                "vector": pack_vector(centroid),
                "drift_metric": float(drift_metric),
                "support": len(window_member_ids),
            }
        )
        prev_centroid = centroid
    return snapshots


def concept_drift_summaries(
    conn: sqlite3.Connection,
    *,
    top: int | None = None,
    min_support: int = 1,
    now: datetime | None = None,
) -> list[DriftSummary]:
    """Read-only ranked view over persisted drift snapshots (run ``build_drift`` first)."""
    rows = conn.execute(
        """
        SELECT concept_id, window_start, window_end, drift_metric, support
        FROM drift_snapshots
        ORDER BY concept_id, window_start
        """
    ).fetchall()
    by_concept: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        by_concept.setdefault(int(row["concept_id"]), []).append(row)

    summaries: list[DriftSummary] = []
    for concept_id, windows in by_concept.items():
        support = sum(int(w["support"]) for w in windows)
        if support < min_support:
            continue
        node = get_node(conn, concept_id)
        if node is None:
            continue
        overall_drift = max(float(w["drift_metric"]) for w in windows)
        summaries.append(
            DriftSummary(
                concept_id=concept_id,
                title=node.title,
                overall_drift=overall_drift,
                window_count=len(windows),
                support=support,
                confidence=min(1.0, support / CONFIDENCE_SUPPORT_FULL),
                relearning=_is_relearning(windows),
                span_start=str(windows[0]["window_start"]),
                span_end=str(windows[-1]["window_end"]),
                evidence_source_uri=_grounding_uri(conn, concept_id),
            )
        )

    summaries.sort(key=lambda s: (-s.overall_drift, -s.confidence, s.concept_id))
    return summaries[:top] if top is not None else summaries


def _is_relearning(windows: list[sqlite3.Row]) -> bool:
    """True when two active windows are separated by >=1 empty window (a re-engagement gap).

    Windows are contiguous when adjacent (window_end == next window_start); a strictly later start
    means at least one empty window sat between them.
    """
    for prev, cur in zip(windows, windows[1:]):
        prev_end = _parse_ts(str(prev["window_end"]))
        cur_start = _parse_ts(str(cur["window_start"]))
        if prev_end is not None and cur_start is not None and cur_start > prev_end:
            return True
    return False


def _grounding_uri(conn: sqlite3.Connection, concept_id: int) -> str | None:
    chunk_ids = resolve_to_evidence(conn, CONCEPT_LEVEL, concept_id)
    if not chunk_ids:
        return None
    row = conn.execute(
        """
        SELECT s.source_uri AS uri
        FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
        WHERE c.id = ?
        """,
        (chunk_ids[0],),
    ).fetchone()
    return str(row["uri"]) if row else None


def _centroid(vectors: list[list[float]]) -> list[float]:
    """Mean of unit vectors, renormalized to unit length (deterministic; mirrors concepts.py)."""
    import numpy as np

    mat = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    unit = mat / norms
    centroid = unit.mean(axis=0)
    cnorm = float(np.linalg.norm(centroid)) or 1.0
    return (centroid / cnorm).astype(np.float32).tolist()


def _cosine_distance(a: list[float], b: list[float]) -> float:
    import numpy as np

    va = np.array(a, dtype=np.float32)
    vb = np.array(b, dtype=np.float32)
    na = float(np.linalg.norm(va)) or 1.0
    nb = float(np.linalg.norm(vb)) or 1.0
    cos = float(va @ vb) / (na * nb)
    return max(0.0, 1.0 - cos)


def _parse_ts(timestamp: str | None) -> datetime | None:
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _snapshot_fingerprint(
    window_seconds: int,
    model: str,
    concept_id: int,
    window_index: int,
    window_start: str,
    member_ids: list[int],
) -> str:
    payload = (
        f"{DRIFT_VERSION}\x1f{window_seconds}\x1f{model}\x1f{concept_id}\x1f{window_index}\x1f"
        f"{window_start}\x1f{','.join(str(i) for i in member_ids)}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
