from __future__ import annotations

import json
from pathlib import Path

import pytest

from crossmodalrag.cli import eval_cmd
from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import EvalQuery, validate_eval_queries


def _query(text: str, uris: list[str]) -> EvalQuery:
    return EvalQuery(id=None, query_text=text, expected_source_uris=uris)


@pytest.mark.parametrize(
    "uri,expected_issue",
    [
        pytest.param("//Users/test/note.md", "doubled_slash", id="leading-double-slash"),
        pytest.param("/Users/test//notes/note.md", "doubled_slash", id="internal-double-slash"),
        pytest.param("relative/path/note.md", "not_absolute", id="relative-path"),
        pytest.param("note.md", "not_absolute", id="bare-filename"),
        pytest.param("://missing-scheme", "not_absolute", id="empty-scheme"),
        pytest.param("/Users/test/note.md", None, id="absolute-path-ok"),
        pytest.param("/Users/test/repo@abc123", None, id="git-uri-ok"),
        pytest.param("test://note/x", None, id="scheme-uri-ok"),
    ],
)
def test_validate_flags_malformed_uris(uri: str, expected_issue: str | None) -> None:
    warnings = validate_eval_queries([_query("q", [uri])])
    if expected_issue is None:
        assert warnings == []
    else:
        assert len(warnings) == 1
        assert warnings[0].issue == expected_issue
        assert warnings[0].uri == uri
        assert warnings[0].row == 1


def test_validate_flags_unknown_source_when_known_set_provided() -> None:
    queries = [_query("q", ["/vault/known.md", "/vault/missing.md"])]
    warnings = validate_eval_queries(queries, known_source_uris={"/vault/known.md"})
    assert [(w.uri, w.issue) for w in warnings] == [("/vault/missing.md", "unknown_source")]


def test_validate_skips_unknown_source_check_for_empty_known_set() -> None:
    # A fresh/pre-ingestion DB has no sources; that should not flag every row.
    queries = [_query("q", ["/vault/anything.md"])]
    assert validate_eval_queries(queries, known_source_uris=set()) == []
    assert validate_eval_queries(queries, known_source_uris=None) == []


def test_validate_reports_row_numbers_across_queries() -> None:
    queries = [
        _query("first", ["/vault/ok.md"]),
        _query("second", ["//vault/bad.md"]),
        _query("third", ["relative.md"]),
    ]
    warnings = validate_eval_queries(queries)
    assert [(w.row, w.query_text, w.issue) for w in warnings] == [
        (2, "second", "doubled_slash"),
        (3, "third", "not_absolute"),
    ]


def test_malformed_uri_never_matches_a_stored_source() -> None:
    # The failure mode the validator exists to catch: a doubled slash silently
    # breaks exact-match grounding against the ingested source URI.
    stored = "/vault/note.md"
    malformed = "//vault/note.md"
    assert malformed != stored
    assert validate_eval_queries([_query("q", [malformed])])[0].issue == "doubled_slash"


def test_eval_cmd_load_queries_warns_on_stderr_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "mem.db"
    monkeypatch.setenv("CMRAG_DB_PATH", str(db_path))

    conn = connect(db_path)
    try:
        init_db(conn)
        cur = conn.execute(
            "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
            ("note", "/vault/known.md", "2026-02-01T00:00:00+00:00", "known"),
        )
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
            (int(cur.lastrowid), 0, "known note content"),
        )
        conn.commit()
    finally:
        conn.close()

    query_file = tmp_path / "queries.json"
    query_file.write_text(
        json.dumps(
            [
                {"query_text": "good", "expected_source_uris": ["/vault/known.md"]},
                {"query_text": "typo", "expected_source_uris": ["//vault/known.md"]},
                {"query_text": "missing", "expected_source_uris": ["/vault/unknown.md"]},
            ]
        ),
        encoding="utf-8",
    )

    eval_cmd(top_k=5, load_queries_path=query_file, as_json=True)

    captured = capsys.readouterr()
    assert "doubled_slash" in captured.err
    assert "//vault/known.md" in captured.err
    assert "unknown_source" in captured.err
    assert "/vault/unknown.md" in captured.err
    assert "warning" not in captured.out  # stdout stays a clean JSON contract
    payload = json.loads(captured.out)
    # Warnings do not block loading: all three rows are upserted and evaluated.
    assert payload["query_count"] == 3
