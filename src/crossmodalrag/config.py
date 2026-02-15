from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(dotenv_path: Path | None = None) -> None:
    path = dotenv_path or (Path.cwd() / ".env")
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def get_db_path() -> Path:
    raw = os.getenv("CMRAG_DB_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd() / "data" / "memory.db"

