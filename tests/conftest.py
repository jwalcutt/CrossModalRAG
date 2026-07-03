"""Shared test fixtures.

The autouse guard below makes it impossible for any test — through any entry
point — to write the *real* shared sample DB (``$TMPDIR/crossmodalrag-sample/``).
That DB is a user-facing artifact (`mem seed-sample`); tests that seeded it left
sources/gold pointing at deleted pytest workspaces, silently zeroing its eval
metrics later.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_default_sample_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import crossmodalrag.cli as cli_mod
    import crossmodalrag.sample_data as sample_data_mod

    def _isolated() -> Path:
        return tmp_path / "crossmodalrag-sample" / "memory.db"

    # `cli` imports the function by name, so both references need the patch.
    monkeypatch.setattr(sample_data_mod, "default_sample_db_path", _isolated)
    monkeypatch.setattr(cli_mod, "default_sample_db_path", _isolated)
