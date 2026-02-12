from crossmodalrag.chunking import chunk_text


def test_chunk_text_handles_short_text() -> None:
    chunks = chunk_text("hello world", max_chars=50, overlap=10)
    assert chunks == ["hello world"]


def test_chunk_text_splits_large_text() -> None:
    text = "a" * 200
    chunks = chunk_text(text, max_chars=80, overlap=10)
    assert len(chunks) >= 3

