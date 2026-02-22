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

Or ingest multiple vaults in one command:

```bash
mem ingest-notes /path/to/vault-a /path/to/vault-b
```

If you omit paths, `mem ingest-notes` will use all `OBSIDIAN_VAULT_PATH_<n>` values from your local `.env`.

4. Ingest git history:

```bash
mem ingest-git /path/to/repo --max-commits 300
```

Or ingest multiple repos in one command:

```bash
mem ingest-git /path/to/repo-a /path/to/repo-b --max-commits 300
```

If you omit paths, `mem ingest-git` will use all `REPO_PATH_<n>` values from your local `.env`.

5. Ask a question:

```bash
mem ask "Why did I change the parser?" --top-k 5
```

6. Run retrieval evaluation (using seeded sample queries or your own `queries_eval` rows):

```bash
mem eval --top-k 5
```

## CLI Commands

- `mem init-db`
- `mem seed-sample [--workspace-dir PATH] [--force]`
- `mem ingest-notes [<vault_path> ...]` (falls back to `.env` `OBSIDIAN_VAULT_PATH_*`)
- `mem ingest-git [<repo_path> ...] [--max-commits N]` (falls back to `.env` `REPO_PATH_*`)
- `mem ask "<query>" [--top-k N]`
- `mem eval [--top-k N] [--query-prefix PREFIX] [--load-queries PATH.json]`

## Synthetic Sample Seed Workflow

Use `mem seed-sample` to create a tiny deterministic sample vault + sample git repo and ingest them into an isolated sample DB.

- Creates a local workspace at `./data/sample-seed-workspace` by default
- Writes to a separate temp sample DB by default (does not modify your main `./data/memory.db`)
- Seeds synthetic markdown notes and git commits (no personal data)
- Populates namespaced sample rows in `queries_eval` for future eval/smoke tests
- Safe to re-run; unchanged content is reused and ingestion remains idempotent

Run the sample retrieval benchmark:

```bash
mem eval --query-prefix "[sample]" --top-k 5
```

Use `--force` to rebuild the sample workspace directory from scratch.
Use `--db-path` if you want the sample dataset in a specific non-main database path.

## Data Location

By default, data is stored at:

- `./data/memory.db`

Set `CMRAG_DB_PATH` to override:

```bash
export CMRAG_DB_PATH=/absolute/path/to/memory.db
```

## Evaluation Query File Format (`mem eval --load-queries`)

Use a JSON array of rows:

```json
[
  {
    "query_text": "What fixed the parser bounds check bug?",
    "expected_source_uris": [
      "/abs/path/to/repo@abc123",
      "/abs/path/to/note/http-parser.md"
    ]
  }
]
```

`mem eval --load-queries file.json` upserts rows into `queries_eval` and then runs metrics (`Recall@K`, `MRR@K`, and an approximate citation hit-rate based on the top retrieved source).
