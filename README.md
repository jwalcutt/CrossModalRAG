# CrossModalRAG

This repository contains a local-first foundation for a cross-modal memory system.

Current scope:
- Ingest markdown notes into SQLite.
- Ingest git commits and diffs into SQLite.
- Create searchable chunks from evidence.
- Query with a simple lexical retriever and get cited evidence.

## Quickstart

1. Create a virtual environment and install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

2. Initialize the local database:

```bash
mem init-db
```

Optional: seed deterministic synthetic notes + git history for smoke testing:

```bash
mem seed-sample
```

3. Ingest notes:

```bash
mem ingest-notes /path/to/obsidian/vault
```

4. Ingest git history:

```bash
mem ingest-git /path/to/repo --max-commits 300
```

5. Ask a question:

```bash
mem ask "Why did I change the parser?" --top-k 5
```

## CLI Commands

- `mem init-db`
- `mem seed-sample [--workspace-dir PATH] [--force]`
- `mem ingest-notes <vault_path>`
- `mem ingest-git <repo_path> [--max-commits N]`
- `mem ask "<query>" [--top-k N]`

## Synthetic Sample Seed Workflow

Use `mem seed-sample` to create a tiny deterministic sample vault + sample git repo and ingest them into an isolated sample DB.

- Creates a local workspace at `./data/sample-seed-workspace` by default
- Writes to a separate temp sample DB by default (does not modify your main `./data/memory.db`)
- Seeds synthetic markdown notes and git commits (no personal data)
- Populates namespaced sample rows in `queries_eval` for future eval/smoke tests
- Safe to re-run; unchanged content is reused and ingestion remains idempotent

Use `--force` to rebuild the sample workspace directory from scratch.
Use `--db-path` if you want the sample dataset in a specific non-main database path.

If you ran an older version that seeded sample data into your main DB, remove it with:

```bash
python scripts/remove_synthetic_seed_data.py
```

## Data Location

By default, data is stored at:

- `./data/memory.db`

Set `CMRAG_DB_PATH` to override:

```bash
export CMRAG_DB_PATH=/absolute/path/to/memory.db
```
