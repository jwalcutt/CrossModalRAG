"""Cross-modal evidence flows up into L1-L3 and drills back to L0 locators."""

from __future__ import annotations

import json

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.episodes import build_episodes
from crossmodalrag.memory.extract import extract_events_for_source
from crossmodalrag.memory.integrity import find_dangling_edges, find_unsupported_nodes
from crossmodalrag.memory.store import list_nodes, resolve_to_evidence
from crossmodalrag.modality import (
    MODALITY_OCR,
    MODALITY_PDF_PAGE,
    build_chunk_metadata,
    parse_locator,
)
from crossmodalrag.retrieve import hybrid


class StubLLMProvider:
    def __init__(self, title: str, summary: str, name: str = "stub-extract") -> None:
        self._output = json.dumps([{"title": title, "summary": summary}])
        self.name = name

    def generate(self, prompt: str, system: str | None = None) -> str:
        return self._output


def _new_db(tmp_path):
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    return conn


def _add_source(conn, *, source_type, uri, chunks, ts="2026-06-01T00:00:00+00:00") -> int:
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        (source_type, uri, ts, uri),
    )
    sid = int(cur.lastrowid)
    for idx, (text, meta) in enumerate(chunks):
        conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json) VALUES (?, ?, ?, ?)",
            (sid, idx, text, json.dumps(meta)),
        )
    conn.commit()
    return sid


def _pdf_source(conn, uri="/vault/documents/spec.pdf"):
    return _add_source(
        conn,
        source_type="pdf",
        uri=uri,
        chunks=[
            (
                "the grounded answer gate abstains below the minimum evidence score",
                build_chunk_metadata(modality=MODALITY_PDF_PAGE, source_type="pdf", page=1),
            )
        ],
    )


# --- L1 grounds to cross-modal L0 ---------------------------------------------


def test_l1_event_grounds_to_pdf_page_l0(tmp_path):
    conn = _new_db(tmp_path)
    sid = _pdf_source(conn)
    provider = StubLLMProvider("PDF spec gate", "The spec defines the evidence gate.")

    created = extract_events_for_source(conn, provider, sid)

    assert created == 1
    (event,) = list_nodes(conn, level=1, node_type="event")
    chunk_ids = resolve_to_evidence(conn, 1, event.id)
    # The event drills down to the pdf's L0 chunk, which carries the pdf-page locator.
    assert len(chunk_ids) == 1
    meta = conn.execute(
        "SELECT metadata_json FROM evidence_chunks WHERE id = ?", (chunk_ids[0],)
    ).fetchone()["metadata_json"]
    loc = parse_locator(meta)
    assert loc is not None and loc.modality == "pdf-page" and loc.page == 1
    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []


# --- L2 cross-modal episode (note + pdf in same folder) -----------------------


def test_note_and_pdf_in_same_folder_form_one_cross_modal_episode(tmp_path):
    conn = _new_db(tmp_path)
    note_sid = _add_source(
        conn,
        source_type="note",
        uri="/vault/documents/retro.md",
        chunks=[("retro about the evidence gate decision", {"modality": "text", "source_type": "note"})],
    )
    pdf_sid = _pdf_source(conn, uri="/vault/documents/spec.pdf")

    extract_events_for_source(conn, StubLLMProvider("Note event", "From the note."), note_sid)
    extract_events_for_source(conn, StubLLMProvider("PDF event", "From the pdf."), pdf_sid)

    result = build_episodes(conn)

    episodes = list_nodes(conn, level=2, node_type="episode")
    assert result.episodes_created == 1
    assert len(episodes) == 1  # one shared-folder episode spanning note + pdf

    # The episode drills to BOTH the note chunk and the pdf-page chunk.
    chunk_ids = resolve_to_evidence(conn, 2, episodes[0].id)
    source_types = {
        conn.execute(
            "SELECT s.source_type FROM evidence_chunks c JOIN sources s ON s.id = c.source_id WHERE c.id = ?",
            (cid,),
        ).fetchone()["source_type"]
        for cid in chunk_ids
    }
    assert source_types == {"note", "pdf"}
    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []


# --- drill-down surfaces the cross-modal locator through restricted retrieval ---


def test_drilldown_retrieval_surfaces_pdf_locator(tmp_path):
    conn = _new_db(tmp_path)
    sid = _pdf_source(conn)
    extract_events_for_source(conn, StubLLMProvider("PDF spec gate", "The spec."), sid)
    (event,) = list_nodes(conn, level=1, node_type="event")

    candidate = set(resolve_to_evidence(conn, 1, event.id))
    hits = hybrid.retrieve(
        conn,
        query="grounded answer gate minimum evidence score",
        top_k=5,
        restrict_chunk_ids=candidate,
    )

    assert hits
    loc = parse_locator(hits[0].chunk_metadata_json)
    assert loc is not None and loc.modality == "pdf-page" and loc.page == 1


# --- image OCR event also grounds + drills ------------------------------------


def test_l1_event_grounds_to_image_ocr_l0(tmp_path):
    conn = _new_db(tmp_path)
    sid = _add_source(
        conn,
        source_type="image",
        uri="/vault/documents/diagram.png",
        chunks=[
            (
                "pipeline overview retriever cache synthesizer",
                build_chunk_metadata(modality=MODALITY_OCR, source_type="image", ocr_confidence=0.95),
            )
        ],
    )
    extract_events_for_source(conn, StubLLMProvider("Diagram", "An architecture diagram."), sid)

    (event,) = list_nodes(conn, level=1, node_type="event")
    chunk_ids = resolve_to_evidence(conn, 1, event.id)
    meta = conn.execute(
        "SELECT metadata_json FROM evidence_chunks WHERE id = ?", (chunk_ids[0],)
    ).fetchone()["metadata_json"]
    loc = parse_locator(meta)
    assert loc is not None and loc.modality == "ocr" and loc.ocr_confidence == 0.95
    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []
