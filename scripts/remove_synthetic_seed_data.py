#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from crossmodalrag.config import get_db_path, load_dotenv  # noqa: E402
from crossmodalrag.db import connect, init_db  # noqa: E402
from crossmodalrag.sample_data import purge_seeded_sample_data  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Remove synthetic sample-seed data from a CrossModalRAG database."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Database path to clean (defaults to CMRAG_DB_PATH or ./data/memory.db).",
    )
    parser.add_argument(
        "--workspace-dir",
        type=Path,
        default=Path("data") / "sample-seed-workspace",
        help="Sample seed workspace path that was used to generate synthetic sources.",
    )
    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    db_path = (args.db_path or get_db_path()).expanduser().resolve()
    workspace_dir = args.workspace_dir.expanduser().resolve()

    conn = connect(db_path)
    try:
        init_db(conn)
        result = purge_seeded_sample_data(conn, workspace_dir=workspace_dir)
    finally:
        conn.close()

    print(f"Cleaned DB: {db_path}")
    print(f"Target sample workspace: {workspace_dir}")
    print(f"Deleted source rows: {result.source_rows_deleted}")
    print(f"Deleted chunk rows: {result.chunk_rows_deleted}")
    print(f"Deleted eval rows: {result.eval_rows_deleted}")


if __name__ == "__main__":
    main()
