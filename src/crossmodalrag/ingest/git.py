from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

from crossmodalrag.chunking import chunk_text


def ingest_git(conn: sqlite3.Connection, repo_path: Path, max_commits: int = 300) -> int:
    if not (repo_path / ".git").exists():
        raise FileNotFoundError(f"Not a git repository: {repo_path}")

    rows = _load_commit_rows(repo_path, max_commits=max_commits)
    inserted_chunks = 0
    for row in rows:
        sha, ts, subject, body, patch = row
        source_uri = f"{repo_path.resolve()}@{sha}"
        combined = f"commit: {subject}\n\n{body}\n\n{patch}".strip()

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO sources (source_type, source_uri, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "git_commit",
                source_uri,
                ts,
                subject[:200],
                json.dumps({"repo": str(repo_path.resolve()), "sha": sha}),
            ),
        )
        source_id = cur.lastrowid
        if not source_id:
            existing = conn.execute(
                """
                SELECT id FROM sources
                WHERE source_type = ? AND source_uri = ? AND timestamp = ?
                """,
                ("git_commit", source_uri, ts),
            ).fetchone()
            if not existing:
                continue
            source_id = int(existing["id"])

        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
        for idx, chunk in enumerate(chunk_text(combined, max_chars=1400, overlap=180)):
            conn.execute(
                """
                INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    source_id,
                    idx,
                    chunk,
                    json.dumps({"modality": "code+text", "source_type": "git_commit", "sha": sha}),
                ),
            )
            inserted_chunks += 1

    conn.commit()
    return inserted_chunks


def _load_commit_rows(repo_path: Path, max_commits: int) -> list[tuple[str, str, str, str, str]]:
    fmt = "%H%x1f%cI%x1f%s%x1f%b%x1e"
    log_cmd = [
        "git",
        "-C",
        str(repo_path),
        "log",
        f"--max-count={max_commits}",
        f"--pretty=format:{fmt}",
        "--no-merges",
    ]
    out = subprocess.run(log_cmd, check=True, capture_output=True, text=True).stdout
    commits: list[tuple[str, str, str, str, str]] = []
    for record in out.split("\x1e"):
        record = record.strip()
        if not record:
            continue
        parts = record.split("\x1f")
        if len(parts) < 4:
            continue
        sha, ts, subject, body = parts[0], parts[1], parts[2], parts[3]
        patch = _commit_patch(repo_path, sha)
        commits.append((sha, ts, subject, body, patch))
    return commits


def _commit_patch(repo_path: Path, sha: str) -> str:
    cmd = ["git", "-C", str(repo_path), "show", "--format=", "--patch", "--stat", sha]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout

