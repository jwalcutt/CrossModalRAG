from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from crossmodalrag.config import get_distill_compression_ratio
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.embed.store import pack_vector, unpack_vector
from crossmodalrag.generate.provider import LLMProvider, LLMUnavailable
from crossmodalrag.memory.store import get_node, list_nodes, resolve_to_evidence

DISTILL_VERSION = "distill-v1"
DISTILL_PROMPT_VERSION = "distill-summary-v1"
CONFIDENCE_SUPPORT_FULL = 3.0

# L2 episodes and L3 concepts are the distillable nodes (each rests on many L0 chunks).
LEVEL_NODE_TYPES: tuple[tuple[int, str], ...] = ((2, "episode"), (3, "concept"))

DISTILL_SUMMARY_SYSTEM_PROMPT = (
    "You compress a cluster of related work into a faithful summary. "
    "Output ONLY 1-2 sentences, no preamble, grounded strictly in the provided titles."
)


@dataclass(frozen=True)
class DistillResult:
    nodes_distilled: int
    nodes_kept: int
    nodes_deleted: int
    named_by_llm: int
    named_by_fallback: int


@dataclass(frozen=True)
class DistilledSummary:
    node_id: int
    level: int
    node_type: str | None
    title: str | None
    summary: str | None
    core_count: int          # kept (core) L0 chunks
    full_count: int          # the node's total L0 evidence chunks
    compression_ratio: float  # core_count / full_count (smaller = more compressed)
    confidence: float | None
    evidence_source_uri: str | None


def build_distilled(
    conn: sqlite3.Connection,
    embed_provider: EmbeddingProvider,
    llm_provider: LLMProvider | None = None,
    *,
    target_ratio: float | None = None,
    levels: tuple[tuple[int, str], ...] = LEVEL_NODE_TYPES,
) -> DistillResult:
    """Derive a compact, retrieval-preserving stand-in for each L2/L3 node, idempotently.

    Each distilled node carries a summary (+ its embedding) and a **minimal core-evidence subset of
    real L0 chunk ids** (the most representative chunks, sized to ``target_ratio``). Reconciled by
    fingerprint over the node's full evidence set, so re-running on unchanged inputs is a no-op (and
    matched nodes are not re-summarized — transient-LLM-outage safe). Provenance survives compression:
    the core set is always a subset of ``resolve_to_evidence`` — never a paraphrase that replaces it.
    """
    if target_ratio is None:
        target_ratio = get_distill_compression_ratio()

    desired_node_ids: set[int] = set()
    created = kept = named_llm = named_fb = 0
    llm_enabled = llm_provider is not None

    for level, node_type in levels:
        for node in list_nodes(conn, level=level, node_type=node_type):
            full_ids = sorted(resolve_to_evidence(conn, level, node.id))
            if not full_ids:
                continue  # nothing to distill (no grounded evidence)
            desired_node_ids.add(node.id)

            fingerprint = _distill_fingerprint(target_ratio, embed_provider.name, node.id, full_ids)
            existing = conn.execute(
                "SELECT derivation_fingerprint FROM distilled_nodes WHERE node_id = ?",
                (node.id,),
            ).fetchone()
            if existing is not None and str(existing["derivation_fingerprint"]) == fingerprint:
                kept += 1
                continue

            k = max(1, round(target_ratio * len(full_ids)))
            vec_by_chunk = _load_chunk_vectors(conn, full_ids, embed_provider.name)
            core_ids = _select_core(full_ids, vec_by_chunk, k)

            summary = None
            if llm_enabled:
                try:
                    summary = _llm_summary(llm_provider, node.title, node.content)
                except LLMUnavailable:
                    llm_enabled = False  # stop retrying for the rest of this run
                    summary = None
            if summary:
                named_llm += 1
            else:
                summary = (node.content or node.title or "").strip() or "(no summary)"
                named_fb += 1

            vector = embed_provider.embed([summary])[0]
            conn.execute(
                """
                INSERT INTO distilled_nodes
                    (node_id, level, summary, model, prompt_version, dim, vector,
                     core_evidence_json, derivation_fingerprint, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    level = excluded.level,
                    summary = excluded.summary,
                    model = excluded.model,
                    prompt_version = excluded.prompt_version,
                    dim = excluded.dim,
                    vector = excluded.vector,
                    core_evidence_json = excluded.core_evidence_json,
                    derivation_fingerprint = excluded.derivation_fingerprint,
                    confidence = excluded.confidence,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    node.id,
                    level,
                    summary,
                    embed_provider.name,
                    DISTILL_PROMPT_VERSION,
                    len(vector),
                    pack_vector(vector),
                    json.dumps(core_ids),
                    fingerprint,
                    min(1.0, len(full_ids) / CONFIDENCE_SUPPORT_FULL),
                ),
            )
            created += 1

    # Drop distilled rows for nodes that no longer exist or lost their evidence.
    deleted = 0
    for row in conn.execute("SELECT node_id FROM distilled_nodes").fetchall():
        if int(row["node_id"]) not in desired_node_ids:
            conn.execute("DELETE FROM distilled_nodes WHERE node_id = ?", (int(row["node_id"]),))
            deleted += 1

    conn.commit()
    return DistillResult(
        nodes_distilled=created,
        nodes_kept=kept,
        nodes_deleted=deleted,
        named_by_llm=named_llm,
        named_by_fallback=named_fb,
    )


def _select_core(full_ids: list[int], vec_by_chunk: dict[int, list[float]], k: int) -> list[int]:
    """Keep the ``k`` most representative chunks (nearest the evidence centroid). Deterministic.

    Falls back to the first ``k`` chunk ids when no vectors are available (lexical-only corpora).
    Returns a sorted subset of ``full_ids``.
    """
    full_sorted = sorted(full_ids)
    if k >= len(full_sorted):
        return full_sorted
    vectored = [vec_by_chunk[cid] for cid in full_sorted if cid in vec_by_chunk]
    if not vectored:
        return full_sorted[:k]
    centroid = _centroid(vectored)
    scored = [
        (cid, _cosine(vec_by_chunk[cid], centroid) if cid in vec_by_chunk else -2.0)
        for cid in full_sorted
    ]
    scored.sort(key=lambda t: (-t[1], t[0]))  # most representative first, id tie-break
    return sorted(cid for cid, _ in scored[:k])


def _load_chunk_vectors(
    conn: sqlite3.Connection, chunk_ids: list[int], model: str
) -> dict[int, list[float]]:
    if not chunk_ids:
        return {}
    placeholders = ",".join("?" for _ in chunk_ids)
    rows = conn.execute(
        f"SELECT chunk_id, vector FROM chunk_embeddings WHERE model = ? AND chunk_id IN ({placeholders})",
        (model, *chunk_ids),
    ).fetchall()
    return {int(r["chunk_id"]): unpack_vector(r["vector"]) for r in rows}


def _llm_summary(provider: LLMProvider, title: str | None, content: str | None) -> str | None:
    listing = (content or "").strip()
    prompt = f"Title: {title or ''}\nMembers: {listing}\n\nSummary:"
    raw = provider.generate(prompt, system=DISTILL_SUMMARY_SYSTEM_PROMPT)
    text = " ".join(raw.split()).strip().strip('"').strip("'") if raw else ""
    return text or None


def _centroid(vectors: list[list[float]]) -> list[float]:
    import numpy as np

    mat = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    unit = mat / norms
    centroid = unit.mean(axis=0)
    cnorm = float(np.linalg.norm(centroid)) or 1.0
    return (centroid / cnorm).astype(np.float32).tolist()


def _cosine(a: list[float], b: list[float]) -> float:
    import numpy as np

    va = np.array(a, dtype=np.float32)
    vb = np.array(b, dtype=np.float32)
    na = float(np.linalg.norm(va)) or 1.0
    nb = float(np.linalg.norm(vb)) or 1.0
    return float(va @ vb) / (na * nb)


def distilled_summaries(
    conn: sqlite3.Connection, *, top: int | None = None
) -> list[DistilledSummary]:
    """Read-only view of the distilled stand-ins, most-compressed first.

    For each distilled node, reports the kept (core) vs full L0 evidence counts, the achieved
    compression ratio, and a grounding source URI (from the core subset) so the surface stays
    provenance-anchored.
    """
    rows = conn.execute(
        "SELECT node_id, level, summary, core_evidence_json, confidence FROM distilled_nodes"
    ).fetchall()
    summaries: list[DistilledSummary] = []
    for row in rows:
        node_id = int(row["node_id"])
        level = int(row["level"])
        core_ids = [int(c) for c in json.loads(row["core_evidence_json"] or "[]")]
        full_count = len(resolve_to_evidence(conn, level, node_id))
        core_count = len(core_ids)
        node = get_node(conn, node_id)
        summaries.append(
            DistilledSummary(
                node_id=node_id,
                level=level,
                node_type=node.node_type if node else None,
                title=node.title if node else None,
                summary=row["summary"],
                core_count=core_count,
                full_count=full_count,
                compression_ratio=(core_count / full_count) if full_count else 1.0,
                confidence=(float(row["confidence"]) if row["confidence"] is not None else None),
                evidence_source_uri=_chunk_source_uri(conn, core_ids[0]) if core_ids else None,
            )
        )
    summaries.sort(key=lambda s: (s.compression_ratio, s.node_id))
    return summaries[:top] if top is not None else summaries


def distilled_summary_to_dict(summary: DistilledSummary) -> dict:
    """Stable JSON contract for `mem distill --json`. Keep field names backward-compatible."""
    return {
        "node_id": summary.node_id,
        "level": summary.level,
        "node_type": summary.node_type,
        "title": summary.title,
        "summary": summary.summary,
        "core_count": summary.core_count,
        "full_count": summary.full_count,
        "compression_ratio": summary.compression_ratio,
        "confidence": summary.confidence,
        "evidence_source_uri": summary.evidence_source_uri,
    }


def _chunk_source_uri(conn: sqlite3.Connection, chunk_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT s.source_uri AS uri
        FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
        WHERE c.id = ?
        """,
        (chunk_id,),
    ).fetchone()
    return str(row["uri"]) if row else None


def _distill_fingerprint(
    target_ratio: float, model: str, node_id: int, full_evidence_ids: list[int]
) -> str:
    payload = (
        f"{DISTILL_VERSION}\x1f{target_ratio:.4f}\x1f{model}\x1f{node_id}\x1f"
        f"{','.join(str(i) for i in full_evidence_ids)}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
