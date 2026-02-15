from __future__ import annotations

import subprocess
from pathlib import Path

from crossmodalrag.db import connect, init_db
from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.notes import ingest_notes


def test_ingest_notes_skips_unchanged(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    note = vault / "daily.md"
    note.write_text("first version\nline two\n", encoding="utf-8")

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        first_inserted = ingest_notes(conn, vault)
        assert first_inserted > 0

        second_inserted = ingest_notes(conn, vault)
        assert second_inserted == 0

        row = conn.execute(
            "SELECT id, source_fingerprint FROM sources WHERE source_type = 'note'"
        ).fetchone()
        assert row is not None
        assert row["source_fingerprint"] is not None

        note.write_text("first version\nline two\nupdated\n", encoding="utf-8")
        third_inserted = ingest_notes(conn, vault)
        assert third_inserted > 0
    finally:
        conn.close()


def test_ingest_notes_backfills_legacy_fingerprint_without_reingest(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    note = vault / "legacy.md"
    note.write_text("legacy content\n", encoding="utf-8")

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        source_uri = str(note.resolve())
        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("note", source_uri, _iso_from_mtime(note.stat().st_mtime), "legacy", "{}"),
        )
        source_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
            VALUES (?, ?, ?, ?)
            """,
            (source_id, 0, "legacy content", "{}"),
        )
        conn.commit()

        inserted = ingest_notes(conn, vault)
        assert inserted == 0

        row = conn.execute("SELECT source_fingerprint FROM sources WHERE id = ?", (source_id,)).fetchone()
        assert row is not None
        assert row["source_fingerprint"] is not None
    finally:
        conn.close()


def test_ingest_git_skips_unchanged_and_backfills_legacy_fingerprint(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "-C", str(repo), "init"])
    _run(["git", "-C", str(repo), "config", "user.name", "Jacob Walcutt"])
    _run(["git", "-C", str(repo), "config", "user.email", "jwalcutt22@gmail.com"])

    file_path = repo / "main.py"
    file_path.write_text("print('v1')\n", encoding="utf-8")
    _run(["git", "-C", str(repo), "add", "main.py"])
    _run(["git", "-C", str(repo), "commit", "-m", "initial"])

    monkeypatch.setenv("TARGET_AUTHOR_NAME", "Jacob Walcutt")
    monkeypatch.setenv("TARGET_AUTHOR_EMAIL", "jwalcutt22@gmail.com")

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        first_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert first_inserted > 0
        second_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert second_inserted == 0

        row = conn.execute(
            "SELECT id, source_uri, timestamp FROM sources WHERE source_type = 'git_commit'"
        ).fetchone()
        assert row is not None
        source_id = int(row["id"])
        source_uri = str(row["source_uri"])
        timestamp = str(row["timestamp"])

        conn.execute("UPDATE sources SET source_fingerprint = NULL WHERE id = ?", (source_id,))
        conn.commit()

        third_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert third_inserted == 0
        refetched = conn.execute(
            "SELECT source_fingerprint, source_uri, timestamp FROM sources WHERE id = ?",
            (source_id,),
        ).fetchone()
        assert refetched is not None
        assert refetched["source_fingerprint"] is not None
        assert refetched["source_uri"] == source_uri
        assert refetched["timestamp"] == timestamp
    finally:
        conn.close()


def test_repeated_ingestion_is_idempotent_for_persisted_data(tmp_path: Path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "note-a.md").write_text("alpha note\nline two\n", encoding="utf-8")
    (vault / "note-b.md").write_text("beta note\nline two\n", encoding="utf-8")

    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "-C", str(repo), "init"])
    _run(["git", "-C", str(repo), "config", "user.name", "Jacob Walcutt"])
    _run(["git", "-C", str(repo), "config", "user.email", "jwalcutt22@gmail.com"])
    (repo / "main.py").write_text("print('v1')\n", encoding="utf-8")
    _run(["git", "-C", str(repo), "add", "main.py"])
    _run(["git", "-C", str(repo), "commit", "-m", "initial"])

    monkeypatch.setenv("TARGET_AUTHOR_NAME", "Jacob Walcutt")
    monkeypatch.setenv("TARGET_AUTHOR_EMAIL", "jwalcutt22@gmail.com")

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        notes_inserted_first = ingest_notes(conn, vault)
        git_inserted_first = ingest_git(conn, repo_path=repo, max_commits=10)
        assert notes_inserted_first > 0
        assert git_inserted_first > 0

        baseline = _data_snapshot(conn)

        notes_inserted_second = ingest_notes(conn, vault)
        git_inserted_second = ingest_git(conn, repo_path=repo, max_commits=10)
        assert notes_inserted_second == 0
        assert git_inserted_second == 0

        repeated = _data_snapshot(conn)
        assert repeated == baseline
    finally:
        conn.close()


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def _data_snapshot(conn) -> tuple[list[tuple], list[tuple]]:
    sources = conn.execute(
        """
        SELECT source_type, source_uri, source_fingerprint, timestamp, title, metadata_json
        FROM sources
        ORDER BY source_type, source_uri
        """
    ).fetchall()
    chunks = conn.execute(
        """
        SELECT
            s.source_type,
            s.source_uri,
            c.chunk_index,
            c.chunk_text,
            c.metadata_json
        FROM evidence_chunks c
        JOIN sources s ON s.id = c.source_id
        ORDER BY s.source_type, s.source_uri, c.chunk_index
        """
    ).fetchall()
    return ([tuple(row) for row in sources], [tuple(row) for row in chunks])


def _iso_from_mtime(mtime: float) -> str:
    # Mirror ingest_notes timestamp format for a realistic legacy source row.
    from datetime import datetime, timezone

    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
