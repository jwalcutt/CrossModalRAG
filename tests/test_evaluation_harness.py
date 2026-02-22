from __future__ import annotations

import json
from pathlib import Path

from crossmodalrag.cli import build_parser, eval_cmd
from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import load_eval_queries_file, run_eval, upsert_eval_queries
from crossmodalrag.sample_data import seed_sample_data


def test_run_eval_computes_metrics_for_seeded_sample_queries(tmp_path: Path) -> None:
    db_path = tmp_path / "mem.db"
    workspace_dir = tmp_path / "sample-workspace"

    conn = connect(db_path)
    try:
        init_db(conn)
        seed_result = seed_sample_data(conn, workspace_dir=workspace_dir)
        assert seed_result.eval_queries_upserted == 3

        summary = run_eval(conn, top_k=5, query_prefix="[sample]")
        assert summary.query_count == 3
        assert summary.top_k == 5
        assert 0.0 <= summary.recall_at_k <= 1.0
        assert 0.0 <= summary.mrr_at_k <= 1.0
        assert 0.0 <= summary.citation_hit_rate <= 1.0
        assert summary.recall_at_k >= summary.citation_hit_rate
        assert len(summary.results) == 3
        assert all(result.expected_source_uris for result in summary.results)
    finally:
        conn.close()


def test_eval_cmd_can_load_queries_and_print_metrics(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "mem.db"
    conn = connect(db_path)
    try:
        init_db(conn)
        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, source_fingerprint, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "note",
                "test://note/eval-target",
                "fp-1",
                "2026-02-01T00:00:00+00:00",
                "eval-target",
                "{}",
            ),
        )
        source_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
            VALUES (?, ?, ?, ?)
            """,
            (source_id, 0, "parser bounds check off by one bug fix", "{}"),
        )
        conn.commit()
    finally:
        conn.close()

    query_file = tmp_path / "eval_queries.json"
    query_file.write_text(
        json.dumps(
            [
                {
                    "query_text": "What fixed the parser bounds check bug?",
                    "expected_source_uris": ["test://note/eval-target"],
                }
            ]
        ),
        encoding="utf-8",
    )

    # Also exercise the file parser directly.
    parsed = load_eval_queries_file(query_file)
    assert len(parsed) == 1
    assert parsed[0].query_text == "What fixed the parser bounds check bug?"

    monkeypatch.setenv("CMRAG_DB_PATH", str(db_path))
    eval_cmd(top_k=5, load_queries_path=query_file)

    out = capsys.readouterr().out
    assert "Eval queries loaded/upserted: 1" in out
    assert "Queries evaluated: 1" in out
    assert "Recall@5:" in out
    assert "MRR@5:" in out
    assert "Citation hit-rate (top-1):" in out

    # Re-upsert through the library to confirm duplicate query rows collapse cleanly.
    conn = connect(db_path)
    try:
        init_db(conn)
        assert upsert_eval_queries(conn, parsed) == 1
        rows = conn.execute(
            "SELECT query_text FROM queries_eval WHERE query_text = ?",
            ("What fixed the parser bounds check bug?",),
        ).fetchall()
        assert len(rows) == 1
    finally:
        conn.close()


def test_cli_parser_accepts_eval_command() -> None:
    parser = build_parser()
    args = parser.parse_args(["eval", "--top-k", "7", "--query-prefix", "[sample]"])
    assert args.command == "eval"
    assert args.top_k == 7
    assert args.query_prefix == "[sample]"
