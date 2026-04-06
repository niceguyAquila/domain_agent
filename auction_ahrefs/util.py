from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path


def load_dotenv_from_repo_root() -> None:
    """Populate os.environ from ``<repo>/.env`` if present (does not override existing vars)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")


def parse_usd_price(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"[^\d.]", "", s.replace(",", ""))
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None
