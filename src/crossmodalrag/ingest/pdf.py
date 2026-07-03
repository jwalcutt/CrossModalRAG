from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crossmodalrag.capabilities import require_pdf
from crossmodalrag.chunking import CHUNKER_VERSION, chunk_text
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.ingest._embed import embed_source_chunks, purge_source_embeddings
from crossmodalrag.modality import MODALITY_PDF_PAGE, build_chunk_metadata

SOURCE_TYPE = "pdf"


def ingest_pdf(
    conn: sqlite3.Connection,
    pdf_path: Path,
    embedder: EmbeddingProvider | None = None,
    progress=None,
) -> int:
    """Ingest one PDF file or a directory of PDFs into L0 ``evidence_chunks``.

    Text-first: each page's extractable text is split into chunks that carry a
    1-based ``page`` locator (see :mod:`crossmodalrag.modality`). One ``sources``
    row per file; re-ingesting an unchanged file (same bytes + same extractor
    version) is a no-op.
    """
    require_pdf()
    # Deferred import so the module imports without the optional [pdf] extra.
    import pypdf

    pdf_files = _resolve_pdf_files(pdf_path)
    inserted_chunks = 0
    total = len(pdf_files)
    for scanned, path in enumerate(pdf_files, start=1):
        if progress is not None:
            progress(scanned, total)
        inserted_chunks += _ingest_one_pdf(conn, path, embedder, pypdf)
    conn.commit()
    return inserted_chunks


def _resolve_pdf_files(pdf_path: Path) -> list[Path]:
    if pdf_path.is_dir():
        return sorted(pdf_path.rglob("*.pdf"))
    if pdf_path.suffix.lower() == ".pdf":
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF path does not exist: {pdf_path}")
        return [pdf_path]
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF path does not exist: {pdf_path}")
    # An existing path that is neither a dir nor a .pdf: nothing to ingest.
    return []


def _ingest_one_pdf(
    conn: sqlite3.Connection,
    path: Path,
    embedder: EmbeddingProvider | None,
    pypdf,
) -> int:
    file_bytes = path.read_bytes()
    stat = path.stat()
    source_uri = str(path.resolve())
    timestamp = _iso_mtime(stat.st_mtime)
    source_fingerprint = _source_fingerprint(file_bytes)

    reader = _open_reader(pypdf, path, file_bytes)
    page_count = len(reader.pages)
    metadata_json = json.dumps(
        {
            "extractor": _extractor_id(),
            "extractor_version": _extractor_version(),
            "pages": page_count,
            "fingerprint": source_fingerprint,
        }
    )

    source_id, unchanged = _upsert_pdf_source(
        conn=conn,
        source_uri=source_uri,
        source_fingerprint=source_fingerprint,
        timestamp=timestamp,
        title=path.stem,
        metadata_json=metadata_json,
    )
    if unchanged:
        return 0

    purge_source_embeddings(conn, source_id)
    conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))

    new_chunks: list[tuple[int, str]] = []
    chunk_index = 0
    for page_number, page in enumerate(reader.pages, start=1):
        page_text = page.extract_text() or ""
        for chunk in chunk_text(page_text):
            cur = conn.execute(
                """
                INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    source_id,
                    chunk_index,
                    chunk,
                    json.dumps(
                        build_chunk_metadata(
                            modality=MODALITY_PDF_PAGE,
                            source_type=SOURCE_TYPE,
                            page=page_number,
                        )
                    ),
                ),
            )
            new_chunks.append((int(cur.lastrowid), chunk))
            chunk_index += 1

    embed_source_chunks(conn, embedder, new_chunks)
    return len(new_chunks)


def _open_reader(pypdf, path: Path, file_bytes: bytes):
    """Open a PdfReader, attempting an empty-password decrypt for encrypted files."""
    import io

    try:
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
    except Exception as exc:  # pragma: no cover - corrupt-file guard
        raise ValueError(f"Could not read PDF: {path} ({exc})") from exc

    if reader.is_encrypted:
        try:
            result = reader.decrypt("")
        except Exception:
            result = 0
        # pypdf returns a falsy/PasswordType.NOT_DECRYPTED when the empty password fails.
        if not result:
            raise ValueError(f"Cannot read password-protected PDF: {path}")
    return reader


def _extractor_id() -> str:
    return "pypdf"


def _extractor_version() -> str:
    import pypdf

    return str(getattr(pypdf, "__version__", "unknown"))


def _iso_mtime(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _source_fingerprint(file_bytes: bytes) -> str:
    # Fold the extractor identity + version and the chunker version into the
    # fingerprint so an extractor or chunker upgrade re-derives intentionally,
    # while unchanged input never churns.
    hasher = hashlib.sha256()
    hasher.update(_extractor_id().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(_extractor_version().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(CHUNKER_VERSION.encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(file_bytes)
    return hasher.hexdigest()


def _upsert_pdf_source(
    conn: sqlite3.Connection,
    source_uri: str,
    source_fingerprint: str,
    timestamp: str,
    title: str,
    metadata_json: str,
) -> tuple[int, bool]:
    rows = conn.execute(
        """
        SELECT id, source_fingerprint, timestamp FROM sources
        WHERE source_type = ? AND source_uri = ?
        ORDER BY id ASC
        """,
        (SOURCE_TYPE, source_uri),
    ).fetchall()

    if not rows:
        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, source_fingerprint, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (SOURCE_TYPE, source_uri, source_fingerprint, timestamp, title, metadata_json),
        )
        return int(cur.lastrowid), False

    canonical_id = int(rows[-1]["id"])
    existing_fingerprint = rows[-1]["source_fingerprint"]
    is_unchanged = existing_fingerprint == source_fingerprint

    # Collapse any historical duplicates so each PDF path has a single source row.
    for row in rows[:-1]:
        old_id = int(row["id"])
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (old_id,))
        conn.execute("DELETE FROM sources WHERE id = ?", (old_id,))

    if is_unchanged:
        return canonical_id, True

    conn.execute(
        """
        UPDATE sources
        SET source_fingerprint = ?, timestamp = ?, title = ?, metadata_json = ?
        WHERE id = ?
        """,
        (source_fingerprint, timestamp, title, metadata_json, canonical_id),
    )
    return canonical_id, False
