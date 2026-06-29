# Chunking Strategy (early)

Date: 2026-02-01

## Approach

Split every source into fixed-size character windows with a small overlap. Simple and
predictable: each chunk is the same length regardless of structure.

- Fixed-size windows of N characters.
- Constant overlap between adjacent windows.
- No awareness of markdown headers or diff hunks.

## Rationale

Fixed-size chunking is easy to reason about and deterministic, so it is the first chunking
approach for the retrieval pipeline.
