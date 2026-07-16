#!/usr/bin/env python3
"""Regenerate the synthetic cross-modal eval fixtures.

These tiny, synthetic, no-personal-data files back the ``[sample-xmodal-*]``
eval slices. They are committed under ``tests/fixtures/sample_seed/documents/``;
this script only exists so they are *reproducible* and their design is documented.

It is a **dev-only** tool: it imports Pillow (the ``[ocr]`` extra ships it) and is
never imported by the ``mem seed-sample`` runtime path, so the core/seed stay
dependency-free. The PDF is hand-rolled (no reportlab/pypdf needed).

Two deliberate slices (see the pre-committed native-embedding gate):

- **Text-heavy** (`spec.pdf`, `notes-screenshot.png`): the answer is in the
  extractable PDF text / OCR-readable rendered text. OCR-text-first should find it.
- **Visual-heavy** (`architecture-diagram.png`): the answer is encoded purely in
  layout/colour (a red "bottleneck" box). The only text is a generic title, so OCR
  yields nothing discriminating — OCR-text-first should FAIL to retrieve it. That
  failure is the whole point: it is what lets the native-embedding gate fire.

Run: ``python scripts/generate_xmodal_fixtures.py`` (needs Pillow).
"""

from __future__ import annotations

from pathlib import Path


def _fixtures_dir() -> Path:
    # Under the sample vault so seed materialization + purge cover them via the
    # existing sample_vault path prefix (see sample_data.py).
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "tests" / "fixtures" / "sample_seed" / "vault" / "documents"


# --- PDF (dependency-free, deterministic, text-extractable) -------------------


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_minimal_pdf(lines: list[str]) -> bytes:
    """A minimal single-page PDF whose text is extractable by pypdf/pdfplumber."""
    content_ops = ["BT", "/F1 14 Tf", "1 0 0 1 72 720 Tm", "24 TL"]
    for idx, line in enumerate(lines):
        if idx > 0:
            content_ops.append("T*")
        content_ops.append(f"({_pdf_escape(line)}) Tj")
    content_ops.append("ET")
    content = "\n".join(content_ops).encode("latin-1")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode("latin-1") + obj + b"\nendobj\n"

    xref_pos = len(out)
    size = len(objects) + 1
    out += f"xref\n0 {size}\n".encode("latin-1")
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode("latin-1")
    out += b"trailer\n"
    out += f"<< /Size {size} /Root 1 0 R >>\n".encode("latin-1")
    out += b"startxref\n" + f"{xref_pos}\n".encode("latin-1") + b"%%EOF"
    return bytes(out)


# --- PNGs (Pillow; deterministic given a fixed Pillow version) ----------------


def _font(size: int):
    from PIL import ImageFont

    # The bundled default font is deterministic across machines (no system-font
    # dependency); size= is supported on Pillow >= 10.1.
    return ImageFont.load_default(size=size)


def build_text_screenshot(lines: list[str]) -> "object":
    """A clean black-on-white 'screenshot' of text that OCR can read."""
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (560, 220), "white")
    draw = ImageDraw.Draw(img)
    font = _font(22)
    y = 24
    for line in lines:
        draw.text((24, y), line, fill="black", font=font)
        y += 40
    return img


def build_visual_diagram() -> "object":
    """A layout/colour-only diagram. Only a generic title is textual, so OCR
    yields nothing that answers the visual query — OCR-text-first must fail."""
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (560, 260), "white")
    draw = ImageDraw.Draw(img)

    # Generic, non-discriminating title (the ONLY text in the image).
    draw.text((24, 16), "Pipeline Overview", fill="black", font=_font(22))

    # Three stages left -> right; the MIDDLE one is the red "bottleneck".
    boxes = [
        (40, 110, 170, 190, (210, 210, 210)),   # stage 1 (grey)
        (215, 110, 345, 190, (220, 60, 60)),     # stage 2 (RED = the answer)
        (390, 110, 520, 190, (210, 210, 210)),   # stage 3 (grey)
    ]
    for x0, y0, x1, y1, fill in boxes:
        draw.rectangle((x0, y0, x1, y1), fill=fill, outline="black", width=3)

    # Arrows between stages (plain lines; no text).
    draw.line((170, 150, 215, 150), fill="black", width=3)
    draw.line((345, 150, 390, 150), fill="black", width=3)

    return img


def _write_if_changed(path: Path, data: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == data:
        return False
    path.write_bytes(data)
    return True


def _png_bytes(img) -> bytes:
    import io

    buf = io.BytesIO()
    # No metadata/timestamps; keep bytes stable across runs.
    img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


def main() -> None:
    out_dir = _fixtures_dir()

    spec_pdf = build_minimal_pdf(
        [
            "CrossModalRAG Retrieval Specification",
            "The grounded answer gate abstains when the top retrieval",
            "score falls below 0.15 (the minimum evidence score).",
            "Citations are rendered inline as bracketed evidence markers.",
        ]
    )
    screenshot = _png_bytes(
        build_text_screenshot(
            [
                "Retro: embeddings backfill",
                "Action: re-run reindex-embeddings",
                "after swapping the embedding model.",
            ]
        )
    )
    diagram = _png_bytes(build_visual_diagram())

    results = {
        "spec.pdf": _write_if_changed(out_dir / "spec.pdf", spec_pdf),
        "notes-screenshot.png": _write_if_changed(out_dir / "notes-screenshot.png", screenshot),
        "architecture-diagram.png": _write_if_changed(out_dir / "architecture-diagram.png", diagram),
    }
    for name, changed in results.items():
        print(f"{'wrote' if changed else 'unchanged'}: {out_dir / name}")


if __name__ == "__main__":
    main()
