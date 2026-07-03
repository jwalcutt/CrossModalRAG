from crossmodalrag.chunking import chunk_diff, chunk_markdown, chunk_text


def _word_set(text: str) -> set[str]:
    return set(text.split())


def test_chunk_text_handles_short_text() -> None:
    chunks = chunk_text("hello world", max_chars=50, overlap=10)
    assert chunks == ["hello world"]


def test_chunk_text_splits_large_text() -> None:
    text = "a" * 200
    chunks = chunk_text(text, max_chars=80, overlap=10)
    assert len(chunks) >= 3


def test_chunk_text_never_starts_or_ends_mid_word() -> None:
    # Distinct words so any mid-word fragment cannot coincide with a real word.
    text = " ".join(f"word{i:04d}" for i in range(400))
    chunks = chunk_text(text, max_chars=150, overlap=30)
    assert len(chunks) > 1
    original_words = _word_set(text)
    for chunk in chunks:
        assert _word_set(chunk) <= original_words, f"mid-word fragment in: {chunk[:40]!r}"


def test_chunk_text_prefers_sentence_boundaries() -> None:
    text = " ".join(f"This is sentence number {i} in the note." for i in range(40))
    chunks = chunk_text(text, max_chars=200, overlap=40)
    assert len(chunks) > 1
    # Every chunk ends at a sentence boundary when sentences fit the window.
    assert all(chunk.endswith(".") for chunk in chunks)


def test_chunk_text_loses_no_content() -> None:
    text = " ".join(f"token{i:04d}" for i in range(500))
    chunks = chunk_text(text, max_chars=180, overlap=40)
    covered = set().union(*(_word_set(c) for c in chunks))
    assert _word_set(text) <= covered


def test_chunk_text_respects_max_chars() -> None:
    text = " ".join(f"This is sentence number {i} in the note." for i in range(40))
    chunks = chunk_text(text, max_chars=200, overlap=40)
    assert all(len(chunk) <= 200 for chunk in chunks)


def test_chunk_text_is_deterministic() -> None:
    text = " ".join(f"Sentence {i} balances both metrics carefully." for i in range(60))
    first = chunk_text(text, max_chars=250, overlap=50)
    second = chunk_text(text, max_chars=250, overlap=50)
    assert first == second


def test_chunk_text_prefers_paragraph_breaks_in_period_light_text() -> None:
    # Period-light markdown (bullets, formulas): the blank line is the only real
    # boundary, and a definitional line must survive splitting intact.
    para = "* bullet about precision\n* bullet about recall\n"
    text = para * 20 + "\nF1-Score: the harmonic mean of precision and recall\n\n" + para * 20
    chunks = chunk_text(text, max_chars=600, overlap=80)
    assert len(chunks) > 1
    assert any("F1-Score: the harmonic mean of precision and recall" in c for c in chunks)
    # Paragraph-preferred splitting keeps every chunk starting at a line start.
    assert all(c.startswith("*") or c.startswith("F1-Score") for c in chunks)


def test_chunk_text_regression_no_mid_word_continuation() -> None:
    # Regression: the fixed-char fallback used to cut words in half at chunk
    # boundaries (a continuation chunk began "alances both metrics:").
    text = (
        "Base accuracy is the fraction of correct classifications. "
        "The F1 score balances both metrics precisely. "
    ) * 30
    chunks = chunk_text(text, max_chars=300, overlap=60)
    assert len(chunks) > 1
    for chunk in chunks:
        assert not chunk.startswith("alances")
        assert _word_set(chunk) <= _word_set(text)


def test_chunk_markdown_splits_on_headers() -> None:
    text = "# Title\n\nIntro line.\n\n## Section A\n\nAlpha body.\n\n## Section B\n\nBeta body."
    chunks = chunk_markdown(text)
    # Each header begins its own section, so no single chunk spans two headers.
    assert any("## Section A" in c for c in chunks)
    assert any("## Section B" in c for c in chunks)
    assert not any("Alpha body" in c and "Beta body" in c for c in chunks)


def test_chunk_markdown_prepends_title_and_heading_path() -> None:
    text = "# Metrics\n\nIntro.\n\n## F1 Score\n\nBalances precision and recall."
    chunks = chunk_markdown(text, title="Model Evaluation Metrics")
    assert chunks[0].startswith("Model Evaluation Metrics > Metrics\n\n")
    assert chunks[1].startswith("Model Evaluation Metrics > Metrics > F1 Score\n\n")


def test_chunk_markdown_heading_path_resets_between_siblings() -> None:
    text = "## Alpha\n\nA body.\n\n### Alpha Child\n\nAC body.\n\n## Beta\n\nB body."
    chunks = chunk_markdown(text, title="Note")
    assert chunks[0].startswith("Note > Alpha\n\n")
    assert chunks[1].startswith("Note > Alpha > Alpha Child\n\n")
    # A sibling H2 pops the previous H2 and its children off the path.
    assert chunks[2].startswith("Note > Beta\n\n")


def test_chunk_markdown_splits_section_into_paragraphs() -> None:
    text = (
        "## Metrics\n\n"
        "Precision: fraction of positive calls that are right.\n\n"
        "Recall: fraction of true positives that are found.\n\n"
        "F1-Score: the harmonic mean of precision and recall."
    )
    chunks = chunk_markdown(text, title="Note")
    # Header merges into the first paragraph; each further paragraph stands alone
    # with its own breadcrumb, so a definitional block is a self-contained chunk.
    assert chunks == [
        "Note > Metrics\n\n## Metrics\n\nPrecision: fraction of positive calls that are right.",
        "Note > Metrics\n\nRecall: fraction of true positives that are found.",
        "Note > Metrics\n\nF1-Score: the harmonic mean of precision and recall.",
    ]


def test_chunk_markdown_keeps_single_newline_blocks_together() -> None:
    text = "## List\n\nIntro line:\n* item one\n* item two"
    chunks = chunk_markdown(text, title="Note")
    assert chunks == ["Note > List\n\n## List\n\nIntro line:\n* item one\n* item two"]


def test_chunk_markdown_context_on_oversized_section_continuations() -> None:
    body = "word " * 400  # well over max_chars
    text = f"## Big\n\n{body}"
    chunks = chunk_markdown(text, max_chars=200, overlap=20, title="Note")
    assert len(chunks) > 1
    # Every chunk of the section, including continuations, carries the breadcrumb.
    assert all(c.startswith("Note > Big\n\n") for c in chunks)


def test_chunk_markdown_title_prefixes_headerless_text() -> None:
    chunks = chunk_markdown("just a sentence", title="My Note")
    assert chunks == ["My Note\n\njust a sentence"]


def test_chunk_markdown_without_title_or_headers_is_unchanged() -> None:
    assert chunk_markdown("just a sentence") == ["just a sentence"]
    assert chunk_markdown("   ") == []


def test_chunk_markdown_collapses_title_repeated_as_heading() -> None:
    text = "# Model Evaluation Metrics\n\nBody text."
    chunks = chunk_markdown(text, title="Model Evaluation Metrics")
    assert chunks[0].startswith("Model Evaluation Metrics\n\n# Model Evaluation Metrics")


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
