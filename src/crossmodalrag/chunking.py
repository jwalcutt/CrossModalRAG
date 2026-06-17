from __future__ import annotations

import re
from typing import Iterable


HEADER_RE = re.compile(r"^#{1,6}\s")


def chunk_text(text: str, max_chars: int = 900, overlap: int = 120) -> list[str]:
    """Fixed-character chunking with overlap.

    Used directly for unstructured text and as the fallback splitter for
    oversized sections/hunks produced by the structure-aware chunkers below.
    """
    cleaned = text.strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]
    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(start + max_chars, len(cleaned))
        chunks.append(cleaned[start:end])
        if end == len(cleaned):
            break
        start = max(0, end - overlap)
    return chunks


def chunk_markdown(text: str, max_chars: int = 900, overlap: int = 120) -> list[str]:
    """Markdown header-aware chunking.

    Splits text into sections delimited by ATX headers (`#`..`######`) so each
    chunk stays within a single logical section. Sections that exceed
    ``max_chars`` fall back to overlapping character chunks, with the section
    header prepended to continuation chunks to preserve context.
    """
    cleaned = text.strip()
    if not cleaned:
        return []

    chunks: list[str] = []
    for header, section in _split_markdown_sections(cleaned):
        section_text = section.strip()
        if not section_text:
            continue
        if len(section_text) <= max_chars:
            chunks.append(section_text)
            continue
        sub_chunks = chunk_text(section_text, max_chars=max_chars, overlap=overlap)
        for idx, sub in enumerate(sub_chunks):
            if idx == 0 or not header:
                chunks.append(sub)
            else:
                chunks.append(f"{header}\n{sub}")
    return chunks


def chunk_diff(text: str, max_chars: int = 1400, overlap: int = 180) -> list[str]:
    """Diff hunk-aware chunking for git commit payloads.

    Keeps the commit message / stat preamble as one segment, then splits each
    changed file into per-hunk segments with the file header (``diff --git`` and
    ``---``/``+++`` lines) prepended so every hunk chunk is self-describing.
    Oversized segments fall back to overlapping character chunks.
    """
    cleaned = text.strip()
    if not cleaned:
        return []

    chunks: list[str] = []
    for segment in _split_diff_segments(cleaned):
        segment_text = segment.strip()
        if not segment_text:
            continue
        if len(segment_text) <= max_chars:
            chunks.append(segment_text)
        else:
            chunks.extend(chunk_text(segment_text, max_chars=max_chars, overlap=overlap))
    return chunks


def _split_markdown_sections(text: str) -> Iterable[tuple[str | None, str]]:
    """Yield (header_line, section_text) pairs.

    The section_text includes its own header line. Content before the first
    header is yielded with a ``None`` header.
    """
    sections: list[tuple[str | None, str]] = []
    current_header: str | None = None
    current: list[str] = []
    for line in text.split("\n"):
        if HEADER_RE.match(line):
            if current:
                sections.append((current_header, "\n".join(current)))
            current_header = line.strip()
            current = [line]
        else:
            current.append(line)
    if current:
        sections.append((current_header, "\n".join(current)))
    return sections


def _split_diff_segments(text: str) -> list[str]:
    """Split a commit payload into preamble + per-file/per-hunk segments."""
    lines = text.split("\n")
    n = len(lines)
    segments: list[str] = []

    # Preamble: commit message, body, and stat summary before the first file diff.
    i = 0
    preamble: list[str] = []
    while i < n and not lines[i].startswith("diff --git "):
        preamble.append(lines[i])
        i += 1
    if any(line.strip() for line in preamble):
        segments.append("\n".join(preamble))

    while i < n:
        # lines[i] starts a file block ("diff --git ...").
        file_header = [lines[i]]
        i += 1
        while i < n and not lines[i].startswith("@@ ") and not lines[i].startswith("diff --git "):
            file_header.append(lines[i])
            i += 1
        header_text = "\n".join(file_header)

        # File block with no hunks (mode change, rename, binary): emit header only.
        if i >= n or lines[i].startswith("diff --git "):
            segments.append(header_text)
            continue

        while i < n and lines[i].startswith("@@ "):
            hunk = [lines[i]]
            i += 1
            while i < n and not lines[i].startswith("@@ ") and not lines[i].startswith("diff --git "):
                hunk.append(lines[i])
                i += 1
            segments.append(f"{header_text}\n" + "\n".join(hunk))

    return segments
