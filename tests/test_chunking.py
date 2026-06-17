from crossmodalrag.chunking import chunk_diff, chunk_markdown, chunk_text


def test_chunk_text_handles_short_text() -> None:
    chunks = chunk_text("hello world", max_chars=50, overlap=10)
    assert chunks == ["hello world"]


def test_chunk_text_splits_large_text() -> None:
    text = "a" * 200
    chunks = chunk_text(text, max_chars=80, overlap=10)
    assert len(chunks) >= 3


def test_chunk_markdown_splits_on_headers() -> None:
    text = "# Title\n\nIntro line.\n\n## Section A\n\nAlpha body.\n\n## Section B\n\nBeta body."
    chunks = chunk_markdown(text)
    # Each header begins its own section, so no single chunk spans two headers.
    assert any(c.startswith("## Section A") for c in chunks)
    assert any(c.startswith("## Section B") for c in chunks)
    assert not any("Section A" in c and "Section B" in c for c in chunks)


def test_chunk_markdown_oversized_section_prepends_header() -> None:
    body = "word " * 400  # well over max_chars
    text = f"## Big\n\n{body}"
    chunks = chunk_markdown(text, max_chars=200, overlap=20)
    assert len(chunks) > 1
    # First chunk carries the real header; continuation chunks repeat it for context.
    assert chunks[0].startswith("## Big")
    assert all(c.startswith("## Big") for c in chunks[1:])


def test_chunk_markdown_no_headers_falls_back() -> None:
    assert chunk_markdown("just a sentence") == ["just a sentence"]
    assert chunk_markdown("   ") == []


def test_chunk_diff_separates_message_and_hunks() -> None:
    payload = (
        "commit: add feature\n\n"
        "Body text.\n\n"
        "diff --git a/foo.py b/foo.py\n"
        "index 111..222 100644\n"
        "--- a/foo.py\n"
        "+++ b/foo.py\n"
        "@@ -1,2 +1,3 @@\n"
        " context\n"
        "+added line\n"
        "@@ -10,2 +11,3 @@\n"
        " more context\n"
        "+another add\n"
    )
    chunks = chunk_diff(payload)
    # Commit message is its own segment, separate from the code hunks.
    assert any(c.startswith("commit: add feature") for c in chunks)
    hunk_chunks = [c for c in chunks if "@@ " in c]
    assert len(hunk_chunks) == 2
    # Every hunk chunk carries the file header for provenance.
    assert all(c.startswith("diff --git a/foo.py b/foo.py") for c in hunk_chunks)


def test_chunk_diff_empty_returns_empty() -> None:
    assert chunk_diff("") == []
