from __future__ import annotations

import re
from typing import Iterable


HEADER_RE = re.compile(r"^#{1,6}\s")

# Version of the chunking logic, folded into source fingerprints by every ingester.
# Bump it whenever chunk boundaries or chunk text composition change, so the
# fingerprint-skip is invalidated and the next ingest re-chunks existing sources
# deterministically (same content + same version always yields identical chunks).
# "2": boundary-aware splitting (no mid-word cuts) + title/heading-path context line.
CHUNKER_VERSION = "2"

_SENTENCE_END = ".!?"


def chunk_text(text: str, max_chars: int = 900, overlap: int = 120) -> list[str]:
    """Overlapping chunking that splits on sentence/whitespace boundaries.

    Used directly for unstructured text and as the fallback splitter for
    oversized sections/hunks produced by the structure-aware chunkers below.
    Each split point prefers, in order: a sentence end, any whitespace, and only
    hard-cuts mid-token when the text has no whitespace at all (e.g. a giant
    identifier blob). Chunk starts are aligned to word boundaries so no chunk
    begins mid-word.
    """
    cleaned = text.strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]
    n = len(cleaned)
    overlap = max(0, min(overlap, max_chars - 1))
    chunks: list[str] = []
    start = 0
    while start < n:
        hard_end = min(start + max_chars, n)
        end = hard_end if hard_end == n else _split_point(cleaned, start, hard_end)
        piece = cleaned[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= n:
            break
        start = _overlap_start(cleaned, start, end, overlap)
    return chunks


def chunk_markdown(
    text: str,
    max_chars: int = 900,
    overlap: int = 120,
    title: str | None = None,
) -> list[str]:
    """Markdown header- and paragraph-aware chunking with a context breadcrumb.

    Splits text into sections delimited by ATX headers (`#`..`######`), then
    into paragraphs (blank-line separated blocks), so each chunk is one
    self-contained semantic unit — a definition, a list, a formula block —
    rather than a fixed-size window mixing neighbours. A section's header line
    stays attached to its first paragraph; paragraphs longer than ``max_chars``
    fall back to boundary-aware overlapping chunks (``chunk_text``). Every
    chunk is prefixed with a single context line — ``title > heading >
    subheading`` — built from the source ``title`` (when given) and the
    section's full heading path, so a chunk's subject is present in its
    indexed/embedded text even when the body (formulas, tables) carries few
    usable terms. The context line is additive on top of ``max_chars``.
    """
    cleaned = text.strip()
    if not cleaned:
        return []

    chunks: list[str] = []
    for heading_path, section in _split_markdown_sections(cleaned):
        section_text = section.strip()
        if not section_text:
            continue
        context = _context_line(title, heading_path)
        for para in _split_paragraphs(section_text):
            pieces = [para] if len(para) <= max_chars else chunk_text(
                para, max_chars=max_chars, overlap=overlap
            )
            for sub in pieces:
                chunks.append(f"{context}\n\n{sub}" if context else sub)
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


def _split_point(text: str, start: int, hard_end: int) -> int:
    """Best split position in ``(floor, hard_end]``.

    Prefers, in order: the last paragraph break, the last sentence end, the
    last line break, the last whitespace — falling back to ``hard_end``
    (mid-token) only when the window has none of these. Paragraph breaks rank
    above sentence ends because notes are often period-light markdown (bullet
    lists, formulas) where the blank line is the only real semantic boundary.
    The floor is the window midpoint so boundary-seeking never produces
    degenerate tiny chunks.
    """
    floor = start + (hard_end - start) // 2
    para = text.rfind("\n\n", floor + 1, hard_end)
    if para != -1:
        return para
    for i in range(hard_end - 1, floor, -1):
        if text[i].isspace() and text[i - 1] in _SENTENCE_END:
            return i
    newline = text.rfind("\n", floor + 1, hard_end)
    if newline != -1:
        return newline
    for i in range(hard_end - 1, floor, -1):
        if text[i].isspace():
            return i
    return hard_end


def _overlap_start(text: str, start: int, end: int, overlap: int) -> int:
    """Start of the next chunk: ``end - overlap`` aligned to a line or word start.

    A nearby line start is preferred (list items / paragraphs resume cleanly),
    but only within ``overlap`` extra characters so a single soft-wrapped mega-
    line cannot balloon the overlap; otherwise the offset widens to the
    enclosing word so chunk starts stay off mid-word positions. When the region
    has no whitespace at all (hard-cut regime), the raw offset is kept so
    progress is unaffected.
    """
    next_start = end - overlap
    if next_start <= start:
        next_start = end
    newline = text.rfind("\n", start, next_start)
    if newline != -1 and next_start - (newline + 1) <= max(overlap, 1):
        aligned = newline + 1
    else:
        aligned = next_start
        while aligned > start + 1 and not text[aligned - 1].isspace():
            aligned -= 1
        if aligned <= start + 1 and not text[aligned - 1].isspace():
            aligned = next_start  # no word boundary in range: keep the raw overlap
    while aligned < len(text) and text[aligned].isspace():
        aligned += 1
    return aligned if aligned > start else end


_PARAGRAPH_RE = re.compile(r"\n\s*\n")


def _split_paragraphs(section_text: str) -> list[str]:
    """Split a section into blank-line separated paragraphs.

    A paragraph that is just the section's header line is merged into the next
    paragraph (a bare ``## Heading`` chunk carries no evidence on its own).
    """
    paragraphs = [p.strip() for p in _PARAGRAPH_RE.split(section_text) if p.strip()]
    if len(paragraphs) > 1 and HEADER_RE.match(paragraphs[0]) and "\n" not in paragraphs[0]:
        paragraphs[1] = f"{paragraphs[0]}\n\n{paragraphs[1]}"
        paragraphs = paragraphs[1:]
    return paragraphs


def _split_markdown_sections(text: str) -> Iterable[tuple[tuple[str, ...], str]]:
    """Yield (heading_path, section_text) pairs.

    ``heading_path`` is the stack of heading titles (hashes stripped) from the
    outermost ancestor down to the section's own heading; content before the
    first header gets an empty path. The section_text includes its own header
    line.
    """
    sections: list[tuple[tuple[str, ...], str]] = []
    stack: list[tuple[int, str]] = []  # (level, heading_text)
    current: list[str] = []
    for line in text.split("\n"):
        if HEADER_RE.match(line):
            if current:
                sections.append((tuple(t for _, t in stack), "\n".join(current)))
            level = len(line) - len(line.lstrip("#"))
            heading = line.lstrip("#").strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, heading))
            current = [line]
        else:
            current.append(line)
    if current:
        sections.append((tuple(t for _, t in stack), "\n".join(current)))
    return sections


def _context_line(title: str | None, heading_path: tuple[str, ...]) -> str:
    """Single breadcrumb line: ``title > heading > subheading``.

    Consecutive duplicates are collapsed (a note titled like its own H1 should
    not repeat itself). Empty when there is neither a title nor any heading.
    """
    parts = [p for p in ((title or "").strip(), *heading_path) if p]
    deduped = [p for i, p in enumerate(parts) if i == 0 or p != parts[i - 1]]
    return " > ".join(deduped)


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
