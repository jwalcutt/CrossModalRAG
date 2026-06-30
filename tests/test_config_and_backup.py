from __future__ import annotations

import json
import os
import sys

import pytest

from crossmodalrag import cli
from crossmodalrag.config import (
    get_config_path,
    get_connector_paths,
    get_default_profile,
    get_default_top_k,
    load_config,
)
from crossmodalrag.db import connect, init_db

CONNECTOR_PREFIXES = ("OBSIDIAN_VAULT_PATH", "REPO_PATH", "PDF_PATH", "IMAGE_PATH")


def _clear_connectors(monkeypatch) -> None:
    for key in list(os.environ):
        if any(key.startswith(f"{p}_") for p in CONNECTOR_PREFIXES):
            monkeypatch.delenv(key, raising=False)


def _write_config(tmp_path, body: str):
    path = tmp_path / "crossmodalrag.toml"
    path.write_text(body, encoding="utf-8")
    return path


# --- config parsing + precedence ----------------------------------------------


def test_no_config_returns_defaults(monkeypatch, tmp_path):
    monkeypatch.delenv("CMRAG_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)  # no crossmodalrag.toml here
    assert load_config() == {}
    assert get_config_path() is None
    assert get_default_profile("balanced") == "balanced"
    assert get_default_top_k() == 5


def test_config_retrieval_defaults(monkeypatch, tmp_path):
    cfg = _write_config(tmp_path, '[retrieval]\nprofile = "recent"\ntop_k = 9\n')
    monkeypatch.setenv("CMRAG_CONFIG", str(cfg))
    assert get_default_profile("balanced") == "recent"
    assert get_default_top_k() == 5 + 4  # 9


def test_malformed_config_degrades_to_empty(monkeypatch, tmp_path):
    cfg = _write_config(tmp_path, "this is = not valid = toml ===\n")
    monkeypatch.setenv("CMRAG_CONFIG", str(cfg))
    assert load_config() == {}
    assert get_default_profile("balanced") == "balanced"  # falls back to builtin


def test_connector_paths_env_beats_config(monkeypatch, tmp_path):
    _clear_connectors(monkeypatch)
    vault_cfg = tmp_path / "from-config"
    vault_env = tmp_path / "from-env"
    cfg = _write_config(tmp_path, f'[connectors]\nnotes = ["{vault_cfg}"]\n')
    monkeypatch.setenv("CMRAG_CONFIG", str(cfg))

    # Config provides the path when env is unset...
    assert get_connector_paths("notes") == [vault_cfg.resolve()]

    # ...but the environment wins when both are present.
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH_1", str(vault_env))
    assert get_connector_paths("notes") == [vault_env.resolve()]


# --- sync uses config connectors ----------------------------------------------


def test_sync_uses_config_connector_paths(monkeypatch, tmp_path, capsys):
    _clear_connectors(monkeypatch)
    monkeypatch.setattr(cli, "load_dotenv", lambda *a, **k: None)  # don't reload the project .env
    monkeypatch.setattr(cli, "get_default_provider", lambda *a, **k: None)  # offline/fast
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "n.md").write_text("# N\n\nconfig-driven note\n", encoding="utf-8")
    cfg = _write_config(tmp_path, f'[connectors]\nnotes = ["{vault}"]\n')
    monkeypatch.setenv("CMRAG_CONFIG", str(cfg))
    monkeypatch.setenv("CMRAG_DB_PATH", str(tmp_path / "memory.db"))

    monkeypatch.setattr(sys, "argv", ["mem", "sync", "--json"])
    cli.main()
    payload = json.loads(capsys.readouterr().out)
    by = {c["connector"]: c for c in payload["connectors"]}
    assert by["notes"]["inserted"] > 0  # ingested from the config path (no env vars set)


# --- backup / restore ---------------------------------------------------------


def _seed_db(path) -> None:
    conn = connect(path)
    try:
        init_db(conn)
        conn.execute("INSERT INTO sources (source_type, source_uri) VALUES ('note', '/marker.md')")
        conn.commit()
    finally:
        conn.close()


def _marker_count(path) -> int:
    conn = connect(path)
    try:
        return int(conn.execute("SELECT COUNT(*) AS n FROM sources").fetchone()["n"])
    finally:
        conn.close()


def test_backup_then_restore_roundtrip(monkeypatch, tmp_path):
    db = tmp_path / "memory.db"
    monkeypatch.setenv("CMRAG_DB_PATH", str(db))
    _seed_db(db)

    backup = tmp_path / "snap.db"
    cli.backup_cmd(dest=backup)
    assert backup.exists() and _marker_count(backup) == 1

    db.unlink()  # simulate loss
    cli.restore_cmd(src=backup)  # no --force needed when target is gone
    assert db.exists() and _marker_count(db) == 1


def test_restore_refuses_overwrite_without_force(monkeypatch, tmp_path):
    db = tmp_path / "memory.db"
    monkeypatch.setenv("CMRAG_DB_PATH", str(db))
    _seed_db(db)
    backup = tmp_path / "snap.db"
    cli.backup_cmd(dest=backup)

    with pytest.raises(cli.CLIError):
        cli.restore_cmd(src=backup, force=False)  # existing DB -> must confirm
    cli.restore_cmd(src=backup, force=True)  # --force overwrites
    assert _marker_count(db) == 1


def test_restore_rejects_non_sqlite(monkeypatch, tmp_path):
    monkeypatch.setenv("CMRAG_DB_PATH", str(tmp_path / "memory.db"))
    junk = tmp_path / "junk.bin"
    junk.write_text("not a database", encoding="utf-8")
    with pytest.raises(cli.CLIError):
        cli.restore_cmd(src=junk)


def test_backup_errors_when_no_db(monkeypatch, tmp_path):
    monkeypatch.setenv("CMRAG_DB_PATH", str(tmp_path / "absent.db"))
    with pytest.raises(cli.CLIError):
        cli.backup_cmd()


def test_restore_force_guard_via_cli_exit_1(monkeypatch, tmp_path, capsys):
    db = tmp_path / "memory.db"
    monkeypatch.setattr(cli, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("CMRAG_DB_PATH", str(db))
    _seed_db(db)
    backup = tmp_path / "snap.db"
    cli.backup_cmd(dest=backup)

    monkeypatch.setattr(sys, "argv", ["mem", "restore", str(backup)])  # no --force, DB exists
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert capsys.readouterr().err.startswith("error: ")
