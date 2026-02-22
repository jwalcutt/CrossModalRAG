from __future__ import annotations

from pathlib import Path

from crossmodalrag.cli import build_parser, seed_sample_cmd
from crossmodalrag.db import connect, init_db
from crossmodalrag.retrieve.lexical import retrieve
from crossmodalrag.sample_data import default_sample_db_path, purge_seeded_sample_data, seed_sample_data


def test_seed_sample_data_is_reusable_and_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "mem.db"
    workspace_dir = tmp_path / "sample-workspace"

    conn = connect(db_path)
    try:
        init_db(conn)

        first = seed_sample_data(conn, workspace_dir=workspace_dir)
        assert first.notes_chunks_inserted > 0
        assert first.git_chunks_inserted > 0
        assert first.eval_queries_upserted == 3
        assert (first.vault_dir / "projects" / "crossmodalrag.md").exists()
        assert (first.repo_dir / ".git").exists()

        hits = retrieve(conn, query="seeding command scaffold", top_k=5)
        assert hits
        assert any(hit.source_type == "git_commit" for hit in hits)

        baseline = _seed_snapshot(conn)

        second = seed_sample_data(conn, workspace_dir=workspace_dir)
        assert second.notes_chunks_inserted == 0
        assert second.git_chunks_inserted == 0
        assert second.eval_queries_upserted == 3

        assert _seed_snapshot(conn) == baseline
    finally:
        conn.close()


def test_cli_parser_accepts_seed_sample_command() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["seed-sample", "--workspace-dir", "tmp/sample", "--db-path", "tmp/sample.db", "--force"]
    )
    assert args.command == "seed-sample"
    assert str(args.workspace_dir) == "tmp/sample"
    assert str(args.db_path) == "tmp/sample.db"
    assert args.force is True


def test_seed_sample_command_uses_separate_default_temp_db(tmp_path: Path, monkeypatch) -> None:
    main_db = tmp_path / "main-memory.db"
    monkeypatch.setenv("CMRAG_DB_PATH", str(main_db))

    workspace_dir = tmp_path / "sample-workspace"
    seed_sample_cmd(workspace_dir=workspace_dir, force=True)

    assert not main_db.exists()
    assert default_sample_db_path().exists()


def test_purge_seeded_sample_data_removes_only_sample_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "mem.db"
    workspace_dir = tmp_path / "sample-workspace"

    conn = connect(db_path)
    try:
        init_db(conn)
        seed_sample_data(conn, workspace_dir=workspace_dir)

        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, source_fingerprint, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "note",
                str((tmp_path / "personal" / "journal.md").resolve()),
                "abc123",
                "2026-02-01T00:00:00+00:00",
                "journal",
                "{}",
            ),
        )
        personal_source_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
            VALUES (?, ?, ?, ?)
            """,
            (personal_source_id, 0, "personal note", "{}"),
        )
        conn.commit()

        result = purge_seeded_sample_data(conn, workspace_dir=workspace_dir)
        assert result.source_rows_deleted > 0
        assert result.chunk_rows_deleted > 0
        assert result.eval_rows_deleted == 3

        remaining_sources = conn.execute(
            "SELECT source_uri FROM sources ORDER BY id"
        ).fetchall()
        assert [row["source_uri"] for row in remaining_sources] == [
            str((tmp_path / "personal" / "journal.md").resolve())
        ]
    finally:
        conn.close()


def _seed_snapshot(conn) -> tuple[list[tuple], list[tuple], list[tuple]]:
    sources = conn.execute(
        """
        SELECT source_type, source_uri, source_fingerprint, timestamp, title, metadata_json
        FROM sources
        ORDER BY source_type, source_uri
        """
    ).fetchall()
    chunks = conn.execute(
        """
        SELECT s.source_type, s.source_uri, c.chunk_index, c.chunk_text, c.metadata_json
        FROM evidence_chunks c
        JOIN sources s ON s.id = c.source_id
        ORDER BY s.source_type, s.source_uri, c.chunk_index
        """
    ).fetchall()
    eval_rows = conn.execute(
        """
        SELECT query_text, expected_source_uris
        FROM queries_eval
        WHERE query_text LIKE '[sample]%'
        ORDER BY query_text
        """
    ).fetchall()
    return (
        [tuple(row) for row in sources],
        [tuple(row) for row in chunks],
        [tuple(row) for row in eval_rows],
    )
