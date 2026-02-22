from __future__ import annotations

import argparse
from pathlib import Path

from crossmodalrag.config import get_db_path, get_numbered_env_paths, load_dotenv
from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import load_eval_queries_file, run_eval, upsert_eval_queries
from crossmodalrag.generate.answer import format_grounded_answer
from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.notes import ingest_notes
from crossmodalrag.retrieve.lexical import retrieve
from crossmodalrag.sample_data import default_sample_db_path, seed_sample_data

def init_db_cmd() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()
    print(f"Initialized database at {db_path}")


def ingest_notes_cmd(vault_paths: list[Path]) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        total_inserted = 0
        for vault_path in vault_paths:
            inserted = ingest_notes(conn, vault_path=vault_path)
            total_inserted += inserted
            print(f"Ingested notes from {vault_path} into {db_path}. Inserted chunks: {inserted}")
    finally:
        conn.close()
    print(
        f"Completed note ingestion for {len(vault_paths)} vault(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ingest_git_cmd(repo_paths: list[Path], max_commits: int = 300) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        total_inserted = 0
        for repo_path in repo_paths:
            inserted = ingest_git(conn, repo_path=repo_path, max_commits=max_commits)
            total_inserted += inserted
            print(
                f"Ingested git history from {repo_path} into {db_path}. Inserted chunks: {inserted}"
            )
    finally:
        conn.close()
    print(
        f"Completed git ingestion for {len(repo_paths)} repo(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ask_cmd(query: str, top_k: int = 5) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        hits = retrieve(conn, query=query, top_k=top_k)
    finally:
        conn.close()
    print(format_grounded_answer(query, hits))


def eval_cmd(
    top_k: int = 5,
    query_prefix: str | None = None,
    load_queries_path: Path | None = None,
) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        loaded = 0
        if load_queries_path is not None:
            queries = load_eval_queries_file(load_queries_path)
            loaded = upsert_eval_queries(conn, queries)
        summary = run_eval(conn, top_k=top_k, query_prefix=query_prefix)
    finally:
        conn.close()

    print(f"Evaluation DB: {db_path}")
    if load_queries_path is not None:
        print(f"Eval queries loaded/upserted: {loaded} from {load_queries_path}")
    if query_prefix:
        print(f"Query prefix filter: {query_prefix}")
    if summary.query_count == 0:
        print("No evaluation queries found. Load queries into 'queries_eval' and run again.")
        return
    print(f"Queries evaluated: {summary.query_count}")
    print(f"Recall@{summary.top_k}: {summary.recall_at_k:.3f}")
    print(f"MRR@{summary.top_k}: {summary.mrr_at_k:.3f}")
    print(f"Citation hit-rate (top-1): {summary.citation_hit_rate:.3f}")


def seed_sample_cmd(
    workspace_dir: Path,
    force: bool = False,
    db_path: Path | None = None,
) -> None:
    db_path = (db_path or default_sample_db_path()).expanduser().resolve()
    conn = connect(db_path)
    try:
        init_db(conn)
        result = seed_sample_data(conn, workspace_dir=workspace_dir, force=force)
    finally:
        conn.close()
    print(f"Seeded sample data into sample DB: {db_path}")
    print("Main DB was not modified.")
    print(f"Workspace: {result.workspace_dir}")
    print(f"Sample vault: {result.vault_dir}")
    print(f"Sample git repo: {result.repo_dir}")
    print(
        "Inserted chunks "
        f"(notes={result.notes_chunks_inserted}, git={result.git_chunks_inserted}); "
        f"eval queries upserted={result.eval_queries_upserted}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CrossModalRAG local memory CLI.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize SQLite database schema.")

    p_notes = sub.add_parser(
        "ingest-notes",
        help="Ingest markdown notes from one or more vault paths (or use OBSIDIAN_VAULT_PATH_* from .env).",
    )
    p_notes.add_argument("vault_paths", nargs="*", type=Path)

    p_git = sub.add_parser(
        "ingest-git",
        help="Ingest git commits and diffs from one or more repos (or use REPO_PATH_* from .env).",
    )
    p_git.add_argument("repo_paths", nargs="*", type=Path)
    p_git.add_argument("--max-commits", type=int, default=300)

    p_ask = sub.add_parser("ask", help="Query indexed evidence.")
    p_ask.add_argument("query", type=str)
    p_ask.add_argument("--top-k", type=int, default=5)

    p_eval = sub.add_parser(
        "eval",
        help="Run retrieval evaluation using queries stored in queries_eval (optionally load from JSON).",
    )
    p_eval.add_argument("--top-k", type=int, default=5)
    p_eval.add_argument(
        "--query-prefix",
        type=str,
        default=None,
        help="Only evaluate queries whose text starts with this prefix (e.g. '[sample]').",
    )
    p_eval.add_argument(
        "--load-queries",
        type=Path,
        default=None,
        help="JSON file of eval query rows to upsert before running evaluation.",
    )

    p_seed = sub.add_parser(
        "seed-sample",
        help="Create deterministic synthetic notes/git fixtures and ingest them into the DB.",
    )
    p_seed.add_argument(
        "--workspace-dir",
        type=Path,
        default=Path("data") / "sample-seed-workspace",
        help="Directory for generated synthetic sample vault and git repo.",
    )
    p_seed.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="DB path for synthetic sample data (defaults to a temp DB, not the main memory DB).",
    )
    p_seed.add_argument(
        "--force",
        action="store_true",
        help="Rebuild the sample workspace directory if it already exists (destructive).",
    )

    return parser


def main() -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init-db":
        init_db_cmd()
        return
    if args.command == "ingest-notes":
        vault_paths = _resolve_ingest_paths(
            args.vault_paths,
            env_prefix="OBSIDIAN_VAULT_PATH",
            command_name="ingest-notes",
        )
        if not vault_paths:
            parser.error(
                "No vault paths provided. Use `mem ingest-notes <vault_path> [<vault_path> ...]` "
                "or define `OBSIDIAN_VAULT_PATH_1`, `OBSIDIAN_VAULT_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_notes_cmd(vault_paths)
        return
    if args.command == "ingest-git":
        repo_paths = _resolve_ingest_paths(
            args.repo_paths,
            env_prefix="REPO_PATH",
            command_name="ingest-git",
        )
        if not repo_paths:
            parser.error(
                "No repo paths provided. Use `mem ingest-git <repo_path> [<repo_path> ...] [--max-commits N]` "
                "or define `REPO_PATH_1`, `REPO_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_git_cmd(repo_paths, max_commits=args.max_commits)
        return
    if args.command == "ask":
        ask_cmd(args.query, top_k=args.top_k)
        return
    if args.command == "eval":
        eval_cmd(top_k=args.top_k, query_prefix=args.query_prefix, load_queries_path=args.load_queries)
        return
    if args.command == "seed-sample":
        seed_sample_cmd(args.workspace_dir, force=args.force, db_path=args.db_path)
        return
    parser.error(f"Unknown command: {args.command}")


def _resolve_ingest_paths(
    explicit_paths: list[Path],
    *,
    env_prefix: str,
    command_name: str,
) -> list[Path]:
    if explicit_paths:
        return [path.expanduser().resolve() for path in explicit_paths]
    env_paths = get_numbered_env_paths(env_prefix)
    if env_paths:
        print(
            f"No explicit paths provided for `{command_name}`. "
            f"Using {len(env_paths)} path(s) from {env_prefix}_* in local environment."
        )
        return env_paths
    return []


if __name__ == "__main__":
    main()
