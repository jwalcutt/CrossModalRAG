from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from crossmodalrag.retrieve.lexical import retrieve


@dataclass(frozen=True)
class EvalQuery:
    id: int | None
    query_text: str
    expected_source_uris: list[str]


@dataclass(frozen=True)
class EvalQueryResult:
    query_text: str
    expected_source_uris: list[str]
    retrieved_source_uris: list[str]
    first_correct_rank: int | None
    recall_hit: bool
    citation_hit: bool


@dataclass(frozen=True)
class EvalSummary:
    query_count: int
    top_k: int
    recall_at_k: float
    mrr_at_k: float
    citation_hit_rate: float
    results: list[EvalQueryResult]


def run_eval(
    conn: sqlite3.Connection,
    *,
    top_k: int = 5,
    query_prefix: str | None = None,
) -> EvalSummary:
    queries = list_eval_queries(conn, query_prefix=query_prefix)
    results: list[EvalQueryResult] = []
    for query in queries:
        hits = retrieve(conn, query=query.query_text, top_k=top_k)
        retrieved_source_uris = _unique_source_uris_in_order([hit.source_uri for hit in hits])
        first_correct_rank = _first_correct_rank(retrieved_source_uris, set(query.expected_source_uris))
        results.append(
            EvalQueryResult(
                query_text=query.query_text,
                expected_source_uris=query.expected_source_uris,
                retrieved_source_uris=retrieved_source_uris,
                first_correct_rank=first_correct_rank,
                recall_hit=first_correct_rank is not None and first_correct_rank <= top_k,
                # Approximation for pre-LLM phase: did the first cited/retrieved source match?
                citation_hit=first_correct_rank == 1,
            )
        )

    if not results:
        return EvalSummary(
            query_count=0,
            top_k=top_k,
            recall_at_k=0.0,
            mrr_at_k=0.0,
            citation_hit_rate=0.0,
            results=[],
        )

    query_count = len(results)
    recall = sum(1 for r in results if r.recall_hit) / query_count
    mrr = sum((1.0 / r.first_correct_rank) if r.first_correct_rank else 0.0 for r in results) / query_count
    citation_hit_rate = sum(1 for r in results if r.citation_hit) / query_count
    return EvalSummary(
        query_count=query_count,
        top_k=top_k,
        recall_at_k=recall,
        mrr_at_k=mrr,
        citation_hit_rate=citation_hit_rate,
        results=results,
    )


def list_eval_queries(
    conn: sqlite3.Connection,
    *,
    query_prefix: str | None = None,
) -> list[EvalQuery]:
    sql = """
        SELECT id, query_text, expected_source_uris
        FROM queries_eval
    """
    params: list[object] = []
    if query_prefix:
        sql += " WHERE query_text LIKE ?"
        params.append(f"{query_prefix}%")
    sql += " ORDER BY id ASC"
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        EvalQuery(
            id=int(row["id"]),
            query_text=str(row["query_text"]),
            expected_source_uris=parse_expected_source_uris(row["expected_source_uris"]),
        )
        for row in rows
    ]


def load_eval_queries_file(path: Path) -> list[EvalQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Evaluation query file must be a JSON list.")

    out: list[EvalQuery] = []
    for idx, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Eval row #{idx} must be an object.")
        query_text = str(item.get("query_text", "")).strip()
        if not query_text:
            raise ValueError(f"Eval row #{idx} is missing non-empty 'query_text'.")
        expected = item.get("expected_source_uris", [])
        if not isinstance(expected, list):
            raise ValueError(f"Eval row #{idx} field 'expected_source_uris' must be a list.")
        expected_uris = [str(x).strip() for x in expected if str(x).strip()]
        out.append(EvalQuery(id=None, query_text=query_text, expected_source_uris=expected_uris))
    return out


def upsert_eval_queries(conn: sqlite3.Connection, queries: list[EvalQuery]) -> int:
    for q in queries:
        expected_json = json.dumps(q.expected_source_uris)
        rows = conn.execute(
            """
            SELECT id, expected_source_uris
            FROM queries_eval
            WHERE query_text = ?
            ORDER BY id ASC
            """,
            (q.query_text,),
        ).fetchall()
        if not rows:
            conn.execute(
                """
                INSERT INTO queries_eval (query_text, expected_source_uris)
                VALUES (?, ?)
                """,
                (q.query_text, expected_json),
            )
            continue

        canonical_id = int(rows[0]["id"])
        if str(rows[0]["expected_source_uris"] or "") != expected_json:
            conn.execute(
                """
                UPDATE queries_eval
                SET expected_source_uris = ?
                WHERE id = ?
                """,
                (expected_json, canonical_id),
            )
        for dup in rows[1:]:
            conn.execute("DELETE FROM queries_eval WHERE id = ?", (int(dup["id"]),))
    conn.commit()
    return len(queries)


def parse_expected_source_uris(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if not isinstance(raw, str):
        return [str(raw)]
    text = raw.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [s.strip() for s in text.split(",") if s.strip()]
    if isinstance(parsed, list):
        return [str(x) for x in parsed]
    if isinstance(parsed, str):
        return [parsed]
    return []


def _unique_source_uris_in_order(source_uris: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for uri in source_uris:
        if uri in seen:
            continue
        seen.add(uri)
        out.append(uri)
    return out


def _first_correct_rank(retrieved_source_uris: list[str], expected: set[str]) -> int | None:
    if not expected:
        return None
    for idx, uri in enumerate(retrieved_source_uris, start=1):
        if uri in expected:
            return idx
    return None
