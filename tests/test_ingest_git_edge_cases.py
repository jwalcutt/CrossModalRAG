from __future__ import annotations

import subprocess
from pathlib import Path

from crossmodalrag.db import connect, init_db
from crossmodalrag.ingest.git import ingest_git


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "git"
TEST_AUTHOR_NAME = "Test User"
TEST_AUTHOR_EMAIL = "test@example.com"


def test_ingest_git_handles_empty_diff_commit_stably(tmp_path: Path, monkeypatch) -> None:
    repo = _init_repo(tmp_path)
    msg_path = FIXTURES_DIR / "empty_diff_commit_message.txt"

    _run(["git", "-C", str(repo), "commit", "--allow-empty", "-F", str(msg_path)])

    monkeypatch.setenv("TARGET_AUTHOR_NAME", TEST_AUTHOR_NAME)
    monkeypatch.setenv("TARGET_AUTHOR_EMAIL", TEST_AUTHOR_EMAIL)

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        first_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert first_inserted > 0

        baseline = _git_snapshot(conn)
        second_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert second_inserted == 0
        assert _git_snapshot(conn) == baseline
    finally:
        conn.close()


def test_ingest_git_handles_non_utf_content_and_encoding_stably(
    tmp_path: Path, monkeypatch
) -> None:
    repo = _init_repo(tmp_path)
    payload = FIXTURES_DIR / "non_utf_payload.bin"
    commit_msg = FIXTURES_DIR / "non_utf_commit_message_latin1.txt"

    repo_payload = repo / "binary_payload.bin"
    repo_payload.write_bytes(payload.read_bytes())
    _run(["git", "-C", str(repo), "add", "binary_payload.bin"])
    _run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "i18n.commitEncoding=ISO-8859-1",
            "commit",
            "-F",
            str(commit_msg),
        ]
    )

    monkeypatch.setenv("TARGET_AUTHOR_NAME", TEST_AUTHOR_NAME)
    monkeypatch.setenv("TARGET_AUTHOR_EMAIL", TEST_AUTHOR_EMAIL)

    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        first_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert first_inserted > 0

        row = conn.execute(
            """
            SELECT title, source_fingerprint
            FROM sources
            WHERE source_type = 'git_commit'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        assert row is not None
        assert row["source_fingerprint"] is not None
        assert row["title"] is not None

        baseline = _git_snapshot(conn)
        second_inserted = ingest_git(conn, repo_path=repo, max_commits=10)
        assert second_inserted == 0
        assert _git_snapshot(conn) == baseline
    finally:
        conn.close()


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "-C", str(repo), "init"])
    _run(["git", "-C", str(repo), "config", "user.name", TEST_AUTHOR_NAME])
    _run(["git", "-C", str(repo), "config", "user.email", TEST_AUTHOR_EMAIL])
    return repo


def _git_snapshot(conn) -> tuple[list[tuple], list[tuple]]:
    sources = conn.execute(
        """
        SELECT source_uri, source_fingerprint, timestamp, title, metadata_json
        FROM sources
        WHERE source_type = 'git_commit'
        ORDER BY source_uri
        """
    ).fetchall()
    chunks = conn.execute(
        """
        SELECT s.source_uri, c.chunk_index, c.chunk_text, c.metadata_json
        FROM evidence_chunks c
        JOIN sources s ON s.id = c.source_id
        WHERE s.source_type = 'git_commit'
        ORDER BY s.source_uri, c.chunk_index
        """
    ).fetchall()
    return ([tuple(row) for row in sources], [tuple(row) for row in chunks])


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, capture_output=True)
