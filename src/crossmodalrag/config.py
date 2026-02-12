from __future__ import annotations

import os
from pathlib import Path


def get_db_path() -> Path:
    raw = os.getenv("CMRAG_DB_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd() / "data" / "memory.db"

