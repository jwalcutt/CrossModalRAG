# Chunking Strategy (revised)

Date: 2026-05-15

## Approach

Move chunking from fixed-size character windows to structure-aware splitting. The chunk boundary
now follows the document's own structure instead of an arbitrary character count.

- Markdown header-aware sections become chunks.
- Diff hunk-aware splitting for git commits.
- Fixed-size windowing kept only as a fallback for oversized sections.

## Rationale

Structure-aware chunking improves retrieval because a chunk now matches a coherent unit of meaning
(a section, a hunk) rather than a fixed span. This revises the earlier fixed-size chunking approach.
