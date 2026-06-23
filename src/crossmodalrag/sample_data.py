from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.notes import ingest_notes

SAMPLE_AUTHOR_NAME = "Test User"
SAMPLE_AUTHOR_EMAIL = "test@example.com"
SAMPLE_SEED_VERSION = "v1"


@dataclass(frozen=True)
class SeedSampleResult:
    workspace_dir: Path
    vault_dir: Path
    repo_dir: Path
    notes_chunks_inserted: int
    git_chunks_inserted: int
    eval_queries_upserted: int


@dataclass(frozen=True)
class PurgeSampleResult:
    source_rows_deleted: int
    chunk_rows_deleted: int
    eval_rows_deleted: int


def seed_sample_data(
    conn: sqlite3.Connection,
    workspace_dir: Path,
    *,
    force: bool = False,
) -> SeedSampleResult:
    workspace_dir = workspace_dir.expanduser().resolve()
    if force and workspace_dir.exists():
        shutil.rmtree(workspace_dir)

    workspace_dir.mkdir(parents=True, exist_ok=True)
    vault_dir = workspace_dir / "sample_vault"
    repo_dir = workspace_dir / "sample_repo"

    _materialize_sample_vault(vault_dir)
    _materialize_sample_git_repo(repo_dir)

    notes_chunks_inserted = ingest_notes(conn, vault_path=vault_dir)
    git_chunks_inserted = ingest_git(
        conn,
        repo_path=repo_dir,
        max_commits=50,
        target_author_name=SAMPLE_AUTHOR_NAME,
        target_author_email=SAMPLE_AUTHOR_EMAIL,
    )
    eval_queries_upserted = _seed_eval_queries(conn, vault_dir=vault_dir, repo_dir=repo_dir)
    conn.commit()

    return SeedSampleResult(
        workspace_dir=workspace_dir,
        vault_dir=vault_dir,
        repo_dir=repo_dir,
        notes_chunks_inserted=notes_chunks_inserted,
        git_chunks_inserted=git_chunks_inserted,
        eval_queries_upserted=eval_queries_upserted,
    )


def default_sample_db_path() -> Path:
    return Path(tempfile.gettempdir()) / "crossmodalrag-sample" / "memory.db"


def purge_seeded_sample_data(conn: sqlite3.Connection, *, workspace_dir: Path) -> PurgeSampleResult:
    workspace_dir = workspace_dir.expanduser().resolve()
    vault_prefix = str((workspace_dir / "sample_vault").resolve())
    repo_prefix = str((workspace_dir / "sample_repo").resolve())

    source_rows = conn.execute(
        """
        SELECT id
        FROM sources
        WHERE source_uri LIKE ? OR source_uri LIKE ?
        """,
        (f"{vault_prefix}/%", f"{repo_prefix}@%"),
    ).fetchall()
    source_ids = [int(row["id"]) for row in source_rows]

    chunk_rows_deleted = 0
    source_rows_deleted = 0
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        chunk_rows_deleted = conn.execute(
            f"DELETE FROM evidence_chunks WHERE source_id IN ({placeholders})",
            tuple(source_ids),
        ).rowcount
        source_rows_deleted = conn.execute(
            f"DELETE FROM sources WHERE id IN ({placeholders})",
            tuple(source_ids),
        ).rowcount

    # '[sample%' (not '[sample]%') so both '[sample]' and '[sample-synth]' rows are purged.
    # SQLite LIKE treats '[' literally; only '%'/'_' are wildcards.
    eval_rows_deleted = conn.execute(
        "DELETE FROM queries_eval WHERE query_text LIKE '[sample%'"
    ).rowcount
    conn.commit()

    return PurgeSampleResult(
        source_rows_deleted=max(source_rows_deleted, 0),
        chunk_rows_deleted=max(chunk_rows_deleted, 0),
        eval_rows_deleted=max(eval_rows_deleted, 0),
    )


def _materialize_sample_vault(vault_dir: Path) -> None:
    fixtures_root = _sample_seed_fixtures_root()
    fixture_vault = fixtures_root / "vault"
    if not fixture_vault.exists():
        raise FileNotFoundError(f"Sample vault fixture not found: {fixture_vault}")

    for src in sorted(fixture_vault.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(fixture_vault)
        dest = vault_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.suffix == ".md":
            dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            # Cross-modal fixtures (PDF/image) are binary; copy bytes verbatim so
            # their fingerprint is stable. Ingestion of these lands in steps 2-3.
            dest.write_bytes(src.read_bytes())


def _materialize_sample_git_repo(repo_dir: Path) -> None:
    marker = repo_dir / ".cmrag_sample_seed_version"
    if (repo_dir / ".git").exists() and marker.exists() and marker.read_text(encoding="utf-8").strip() == SAMPLE_SEED_VERSION:
        return

    if repo_dir.exists():
        if any(repo_dir.iterdir()):
            raise FileExistsError(
                f"Sample repo directory already exists and is not a recognized seed repo: {repo_dir}. "
                "Use --force to rebuild it."
            )
    else:
        repo_dir.mkdir(parents=True, exist_ok=True)

    _run_git(["git", "-C", str(repo_dir), "init"])
    _run_git(["git", "-C", str(repo_dir), "config", "user.name", SAMPLE_AUTHOR_NAME])
    _run_git(["git", "-C", str(repo_dir), "config", "user.email", SAMPLE_AUTHOR_EMAIL])

    commit_plan_path = _sample_seed_fixtures_root() / "git_commit_plan.json"
    plan = json.loads(commit_plan_path.read_text(encoding="utf-8"))
    for step in plan:
        for rel_path, content in step["files"].items():
            file_path = repo_dir / rel_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")
        _run_git(["git", "-C", str(repo_dir), "add", "-A"])
        env = {
            "GIT_AUTHOR_NAME": SAMPLE_AUTHOR_NAME,
            "GIT_AUTHOR_EMAIL": SAMPLE_AUTHOR_EMAIL,
            "GIT_COMMITTER_NAME": SAMPLE_AUTHOR_NAME,
            "GIT_COMMITTER_EMAIL": SAMPLE_AUTHOR_EMAIL,
            "GIT_AUTHOR_DATE": step["date"],
            "GIT_COMMITTER_DATE": step["date"],
        }
        _run_git(["git", "-C", str(repo_dir), "commit", "-m", step["message"]], env=env)

    marker.write_text(SAMPLE_SEED_VERSION + "\n", encoding="utf-8")


def _seed_eval_queries(conn: sqlite3.Connection, *, vault_dir: Path, repo_dir: Path) -> int:
    note_project_uri = str((vault_dir / "projects" / "crossmodalrag.md").resolve())
    note_retro_uri = str((vault_dir / "retros" / "2026-01-14.md").resolve())
    scaffold_sha = _git_rev_parse_subject(repo_dir, "cli: add sample seeding command scaffold")

    # Cross-modal fixtures (Phase 3). These are materialized into the vault now, but
    # not ingested until Phase 3 steps 2-3 — so these queries score ~0 recall today.
    # That failing baseline is intentional: it is what the native-embedding gate is
    # measured against once OCR-text-first ingestion exists.
    pdf_spec_uri = str((vault_dir / "documents" / "spec.pdf").resolve())
    screenshot_uri = str((vault_dir / "documents" / "notes-screenshot.png").resolve())
    diagram_uri = str((vault_dir / "documents" / "architecture-diagram.png").resolve())

    rows = [
        (
            "[sample] Where is the pipeline integrity smoke-test plan documented?",
            json.dumps([note_project_uri]),
        ),
        (
            "[sample] Which commit added the sample seeding command scaffold?",
            json.dumps([f"{repo_dir.resolve()}@{scaffold_sha}"]),
        ),
        (
            "[sample] What issue was noted in the retrieval smoke test retro?",
            json.dumps([note_retro_uri]),
        ),
        (
            # Negative case: nothing in the sample corpus answers this, so a
            # grounded system should abstain. Used by generation eval.
            "[sample] What were the quarterly revenue figures for the Tokyo office?",
            json.dumps([]),
        ),
        (
            # Synthesis case: answer aggregates across BOTH notes (the project plan
            # describes the smoke-test workflow + namespaced eval queries; the retro
            # decides to track the query in queries_eval). Used to measure source
            # coverage of memory-level vs flat retrieval in generation eval.
            "[sample-synth] How does the sample-seed smoke-test workflow help measure and "
            "track retrieval quality over time?",
            json.dumps([note_project_uri, note_retro_uri]),
        ),
        (
            # Synthesis case: spans the retro note (ranking issue) and the commit that
            # added the seeding-command scaffold referenced by that issue.
            "[sample-synth] What ranking issue was observed around the sample seeding command "
            "scaffold, and which commit added that scaffold?",
            json.dumps([note_retro_uri, f"{repo_dir.resolve()}@{scaffold_sha}"]),
        ),
        (
            # Text-heavy cross-modal slice: the answer lives in the PDF's
            # extractable text — OCR/PDF-text-first should retrieve it.
            "[sample-xmodal-text] What is the minimum retrieval score below which the "
            "grounded answer gate abstains?",
            json.dumps([pdf_spec_uri]),
        ),
        (
            # Text-heavy cross-modal slice: the answer is OCR-readable text rendered
            # in the screenshot.
            "[sample-xmodal-text] What action was recorded in the embeddings backfill retro "
            "screenshot?",
            json.dumps([screenshot_uri]),
        ),
        (
            # Visual-heavy slice: the answer (the red middle/bottleneck stage) is encoded
            # only in the diagram's layout/colour. The image's only text is a generic
            # title, so OCR-text-first should FAIL to retrieve it — this is the slice the
            # native-embedding gate exists to rescue.
            "[sample-xmodal-visual] In the architecture diagram, which processing stage is "
            "highlighted as the bottleneck?",
            json.dumps([diagram_uri]),
        ),
    ]
    for query_text, expected_source_uris in rows:
        existing = conn.execute(
            """
            SELECT id, expected_source_uris
            FROM queries_eval
            WHERE query_text = ?
            ORDER BY id ASC
            """,
            (query_text,),
        ).fetchall()
        if not existing:
            conn.execute(
                """
                INSERT INTO queries_eval (query_text, expected_source_uris)
                VALUES (?, ?)
                """,
                (query_text, expected_source_uris),
            )
            continue

        canonical_id = int(existing[0]["id"])
        if str(existing[0]["expected_source_uris"]) != expected_source_uris:
            conn.execute(
                """
                UPDATE queries_eval
                SET expected_source_uris = ?
                WHERE id = ?
                """,
                (expected_source_uris, canonical_id),
            )
        for dup in existing[1:]:
            conn.execute("DELETE FROM queries_eval WHERE id = ?", (int(dup["id"]),))
    return len(rows)


def _sample_seed_fixtures_root() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "tests" / "fixtures" / "sample_seed"


def _run_git(cmd: list[str], env: dict[str, str] | None = None) -> None:
    full_env = None
    if env is not None:
        import os

        full_env = os.environ.copy()
        full_env.update(env)
    subprocess.run(cmd, check=True, capture_output=True, env=full_env)


def _git_rev_parse_subject(repo_dir: Path, subject: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(repo_dir),
            "log",
            "--format=%H%x1f%s",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in completed.stdout.splitlines():
        if not line.strip() or "\x1f" not in line:
            continue
        sha, found_subject = line.split("\x1f", 1)
        if found_subject == subject:
            return sha
    raise RuntimeError(f"Unable to locate seeded commit subject: {subject}")
