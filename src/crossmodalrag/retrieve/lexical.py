from __future__ import annotations

import math
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone


WORD_RE = re.compile(r"[a-zA-Z0-9_]+")


@dataclass
class RetrievalHit:
    chunk_id: int
    source_id: int
    source_type: str
    source_uri: str
    source_timestamp: str | None
    title: str | None
    chunk_index: int
    chunk_text: str
    score: float
    lexical_score: float
    recency_score: float
    vector_score: float = 0.0
    chunk_metadata_json: str | None = None
    usage_score: float = 0.0


def retrieve(
    conn: sqlite3.Connection,
    query: str,
    top_k: int = 5,
    restrict_chunk_ids: set[int] | None = None,
    restrict_source_types: set[str] | None = None,
) -> list[RetrievalHit]:
    query_tokens = tokenize(query)
    if not query_tokens:
        return []

    rows = conn.execute(
        """
        SELECT
            c.id as chunk_id,
            c.source_id as source_id,
            c.chunk_index as chunk_index,
            c.chunk_text as chunk_text,
            c.metadata_json as chunk_metadata_json,
            s.source_type as source_type,
            s.source_uri as source_uri,
            s.timestamp as source_timestamp,
            s.title as title
        FROM evidence_chunks c
        JOIN sources s ON s.id = c.source_id
        """
    ).fetchall()

    now = datetime.now(timezone.utc)
    scored: list[RetrievalHit] = []
    for row in rows:
        if restrict_chunk_ids is not None and int(row["chunk_id"]) not in restrict_chunk_ids:
            continue
        if restrict_source_types is not None and str(row["source_type"]) not in restrict_source_types:
            continue
        tokens = tokenize(str(row["chunk_text"]))
        lex = lexical_overlap_score(query_tokens, tokens)
        if lex <= 0:
            continue

        recency = recency_score(row["source_timestamp"], now=now)
        score = (0.85 * lex) + (0.15 * recency)
        scored.append(
            RetrievalHit(
                chunk_id=int(row["chunk_id"]),
                source_id=int(row["source_id"]),
                source_type=str(row["source_type"]),
                source_uri=str(row["source_uri"]),
                source_timestamp=row["source_timestamp"],
                title=row["title"],
                chunk_index=int(row["chunk_index"]),
                chunk_text=str(row["chunk_text"]),
                score=score,
                lexical_score=lex,
                recency_score=recency,
                chunk_metadata_json=row["chunk_metadata_json"],
            )
        )

    scored.sort(key=lambda hit: hit.score, reverse=True)
    # Local import avoids a circular import (rerank imports from this module).
    from crossmodalrag.retrieve.rerank import dedupe_hits

    # max_kept bounds dedupe to O(top_k * n) over the full scored pool (see hybrid.retrieve).
    return dedupe_hits(scored, max_kept=top_k)


def tokenize(text: str) -> list[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


def lexical_overlap_score(query_tokens: list[str], doc_tokens: list[str]) -> float:
    if not query_tokens or not doc_tokens:
        return 0.0

    q = Counter(query_tokens)
    d = Counter(doc_tokens)
    dot = sum(q[t] * d[t] for t in q)
    q_norm = math.sqrt(sum(v * v for v in q.values()))
    d_norm = math.sqrt(sum(v * v for v in d.values()))
    if q_norm == 0 or d_norm == 0:
        return 0.0
    return dot / (q_norm * d_norm)


def recency_score(timestamp: str | None, now: datetime) -> float:
    if not timestamp:
        return 0.0
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        days_old = max((now - dt).days, 0)
        return math.exp(-days_old / 45.0)
    except ValueError:
        return 0.0

