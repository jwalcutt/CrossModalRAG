from __future__ import annotations

import sys
from pathlib import Path

import pytest

from crossmodalrag import cli
from crossmodalrag.config import get_numbered_env_paths


def test_get_numbered_env_paths_sorts_numeric_suffixes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_10", str(tmp_path / "vault-10"))
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_2", str(tmp_path / "vault-2"))
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_1", str(tmp_path / "vault-1"))
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_X", str(tmp_path / "ignored"))
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_3", "   ")

    paths = get_numbered_env_paths("OBSIDIAN_VAULT_PATH")

    assert paths == [
        (tmp_path / "vault-1").resolve(),
        (tmp_path / "vault-2").resolve(),
        (tmp_path / "vault-10").resolve(),
    ]


def test_main_ingest_notes_uses_local_dotenv_paths_when_no_args(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"OBSIDIAN_VAULT_PATH_2={tmp_path / 'vault-b'}",
                f"OBSIDIAN_VAULT_PATH_1={tmp_path / 'vault-a'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    captured_paths: list[Path] = []

    def _fake_ingest_notes_cmd(vault_paths: list[Path]) -> None:
        captured_paths.extend(vault_paths)

    monkeypatch.setattr(cli, "ingest_notes_cmd", _fake_ingest_notes_cmd)
    monkeypatch.setattr(sys, "argv", ["mem", "ingest-notes"])

    cli.main()

    out = capsys.readouterr().out
    assert "Using 2 path(s) from OBSIDIAN_VAULT_PATH_*" in out
    assert captured_paths == [
        (tmp_path / "vault-a").resolve(),
        (tmp_path / "vault-b").resolve(),
    ]


def test_main_ingest_git_errors_when_no_args_and_no_env(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("REPO_PATH_1", raising=False)
    monkeypatch.delenv("REPO_PATH_2", raising=False)
    monkeypatch.setattr(sys, "argv", ["mem", "ingest-git"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "No repo paths provided" in err
    assert "mem ingest-git <repo_path> [<repo_path> ...] [--max-commits N]" in err
    assert "REPO_PATH_1" in err


def test_parser_accepts_multiple_ingest_paths() -> None:
    parser = cli.build_parser()

    notes_args = parser.parse_args(["ingest-notes", "vault1", "vault2"])
    git_args = parser.parse_args(["ingest-git", "repo1", "repo2", "--max-commits", "25"])

    assert notes_args.command == "ingest-notes"
    assert [str(path) for path in notes_args.vault_paths] == ["vault1", "vault2"]
    assert git_args.command == "ingest-git"
    assert [str(path) for path in git_args.repo_paths] == ["repo1", "repo2"]
    assert git_args.max_commits == 25
