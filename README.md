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
- `mem ingest-notes <vault_path>`
- `mem ingest-git <repo_path> [--max-commits N]`
- `mem ask "<query>" [--top-k N]`

## Data Location

By default, data is stored at:

- `./data/memory.db`

Set `CMRAG_DB_PATH` to override:

```bash
export CMRAG_DB_PATH=/absolute/path/to/memory.db
```

