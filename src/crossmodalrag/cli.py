from __future__ import annotations

import argparse
from pathlib import Path

from crossmodalrag.config import get_db_path
from crossmodalrag.db import connect, init_db
from crossmodalrag.generate.answer import format_grounded_answer
from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.notes import ingest_notes
from crossmodalrag.retrieve.lexical import retrieve

def init_db_cmd() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()
    print(f"Initialized database at {db_path}")


def ingest_notes_cmd(vault_path: Path) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        inserted = ingest_notes(conn, vault_path=vault_path)
    finally:
        conn.close()
    print(f"Ingested notes into {db_path}. Inserted chunks: {inserted}")


def ingest_git_cmd(repo_path: Path, max_commits: int = 300) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        inserted = ingest_git(conn, repo_path=repo_path, max_commits=max_commits)
    finally:
        conn.close()
    print(
        f"Ingested git history from {repo_path} into {db_path}. Inserted chunks: {inserted}"
    )


def ask_cmd(query: str, top_k: int = 5) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        hits = retrieve(conn, query=query, top_k=top_k)
    finally:
        conn.close()
    print(format_grounded_answer(query, hits))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CrossModalRAG local memory CLI.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize SQLite database schema.")

    p_notes = sub.add_parser("ingest-notes", help="Ingest markdown notes from a vault path.")
    p_notes.add_argument("vault_path", type=Path)

    p_git = sub.add_parser("ingest-git", help="Ingest git commits and diffs from a repo.")
    p_git.add_argument("repo_path", type=Path)
    p_git.add_argument("--max-commits", type=int, default=300)

    p_ask = sub.add_parser("ask", help="Query indexed evidence.")
    p_ask.add_argument("query", type=str)
    p_ask.add_argument("--top-k", type=int, default=5)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init-db":
        init_db_cmd()
        return
    if args.command == "ingest-notes":
        ingest_notes_cmd(args.vault_path)
        return
    if args.command == "ingest-git":
        ingest_git_cmd(args.repo_path, max_commits=args.max_commits)
        return
    if args.command == "ask":
        ask_cmd(args.query, top_k=args.top_k)
        return
    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
