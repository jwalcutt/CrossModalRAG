from __future__ import annotations

import os
import re
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


def get_numbered_env_paths(prefix: str) -> list[Path]:
    pattern = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    indexed: list[tuple[int, Path]] = []
    for key, raw in os.environ.items():
        match = pattern.match(key)
        if not match:
            continue
        value = raw.strip()
        if not value:
            continue
        indexed.append((int(match.group(1)), Path(value).expanduser().resolve()))
    indexed.sort(key=lambda item: item[0])
    return [path for _, path in indexed]
