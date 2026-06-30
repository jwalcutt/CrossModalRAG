from __future__ import annotations

import hashlib
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crossmodalrag.capabilities import require_ocr
from crossmodalrag.chunking import chunk_text
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.ingest._embed import embed_source_chunks, purge_source_embeddings
from crossmodalrag.modality import MODALITY_OCR, build_chunk_metadata

SOURCE_TYPE = "image"
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp"}


def ingest_images(
    conn: sqlite3.Connection,
    image_path: Path,
    embedder: EmbeddingProvider | None = None,
    progress=None,
) -> int:
    """Ingest one image file or a directory of images into L0 ``evidence_chunks``.

    OCR-text-first: each image's recognized text becomes searchable chunks tagged
    ``modality="ocr"`` with an ``ocr_confidence`` signal (see
    :mod:`crossmodalrag.modality`). One ``sources`` row per image file; re-ingesting
    an unchanged image (same bytes + same OCR engine version) is a no-op. An image
    that yields no text registers the source with zero chunks.
    """
    require_ocr()
    # Deferred imports so the module imports without the optional [ocr] extra.
    import pytesseract
    from PIL import Image

    image_files = _resolve_image_files(image_path)
    inserted_chunks = 0
    total = len(image_files)
    for scanned, path in enumerate(image_files, start=1):
        if progress is not None:
            progress(scanned, total)
        inserted_chunks += _ingest_one_image(conn, path, embedder, pytesseract, Image)
    conn.commit()
    return inserted_chunks


def _resolve_image_files(image_path: Path) -> list[Path]:
    if image_path.is_dir():
        return sorted(
            p for p in image_path.rglob("*") if p.is_file() and p.suffix.lower() in IMAGE_SUFFIXES
        )
    if image_path.suffix.lower() in IMAGE_SUFFIXES:
        if not image_path.exists():
            raise FileNotFoundError(f"Image path does not exist: {image_path}")
        return [image_path]
    if not image_path.exists():
        raise FileNotFoundError(f"Image path does not exist: {image_path}")
    # An existing path that is neither a dir nor a known image type: nothing to do.
    return []


def _ingest_one_image(
    conn: sqlite3.Connection,
    path: Path,
    embedder: EmbeddingProvider | None,
    pytesseract,
    Image,
) -> int:
    file_bytes = path.read_bytes()
    stat = path.stat()
    source_uri = str(path.resolve())
    timestamp = _iso_mtime(stat.st_mtime)
    source_fingerprint = _source_fingerprint(file_bytes)

    ocr_text, confidence = _run_ocr(pytesseract, Image, file_bytes)

    metadata_json = json.dumps(
        {
            "ocr_engine": _ocr_engine_id(),
            "ocr_engine_version": _ocr_engine_version(),
            "ocr_confidence": confidence,
            "fingerprint": source_fingerprint,
        }
    )

    source_id, unchanged = _upsert_image_source(
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
    for idx, chunk in enumerate(chunk_text(ocr_text)):
        cur = conn.execute(
            """
            INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                source_id,
                idx,
                chunk,
                json.dumps(
                    build_chunk_metadata(
                        modality=MODALITY_OCR,
                        source_type=SOURCE_TYPE,
                        ocr_confidence=confidence,
                    )
                ),
            ),
        )
        new_chunks.append((int(cur.lastrowid), chunk))

    embed_source_chunks(conn, embedder, new_chunks)
    return len(new_chunks)


def _run_ocr(pytesseract, Image, file_bytes: bytes) -> tuple[str, float | None]:
    """Return (recognized_text, mean_word_confidence in [0,1] or None)."""
    from pytesseract import Output

    with Image.open(io.BytesIO(file_bytes)) as img:
        img = img.convert("RGB")
        text = pytesseract.image_to_string(img) or ""
        data = pytesseract.image_to_data(img, output_type=Output.DICT)

    confidences = [
        float(conf)
        for word, conf in zip(data.get("text", []), data.get("conf", []))
        if str(word).strip() and float(conf) >= 0
    ]
    mean_conf = (sum(confidences) / len(confidences) / 100.0) if confidences else None
    return text, mean_conf


def _ocr_engine_id() -> str:
    return "tesseract"


def _ocr_engine_version() -> str:
    import pytesseract

    return str(pytesseract.get_tesseract_version())


def _iso_mtime(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _source_fingerprint(file_bytes: bytes) -> str:
    # Fold the OCR engine identity + version into the fingerprint so an engine
    # upgrade re-derives intentionally, while unchanged input never churns.
    hasher = hashlib.sha256()
    hasher.update(_ocr_engine_id().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(_ocr_engine_version().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(file_bytes)
    return hasher.hexdigest()


def _upsert_image_source(
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

    # Collapse any historical duplicates so each image path has a single source row.
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
