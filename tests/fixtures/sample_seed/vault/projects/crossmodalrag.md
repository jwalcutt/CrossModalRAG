# CrossModalRAG Project Notes

## Pipeline Integrity

Use a deterministic sample dataset to verify the ingestion -> retrieval pipeline on any machine.

- Seed synthetic markdown notes and git commits.
- Re-run seeding to confirm idempotent ingestion (no chunk churn for unchanged content).
- Use namespaced evaluation queries for quick smoke tests.

## Next Steps

- Add `mem seed-sample` for one-command setup.
- Add baseline eval harness after sample data is available.
