from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from auction_ahrefs.config import ExportConfig

_CSV_FIELDS = [
    "run_id",
    "domain",
    "source",
    "bids",
    "price_usd",
    "auction_end_utc",
    "auction_type",
    "detail_url",
    "domain_age_years",
    "domain_rating",
    "ahrefs_rank",
    "org_keywords",
    "org_traffic",
    "org_traffic_value_usd",
    "ahrefs_report_date",
    "ahrefs_fetched_utc",
]


def export_path_for_run(cfg: ExportConfig, run_id: int) -> Path:
    fmt = cfg.format
    ext = "xlsx" if fmt == "xlsx" else "csv"
    now = datetime.now(timezone.utc)
    name = cfg.filename_template.format(
        run_id=run_id,
        utc_date=now.strftime("%Y-%m-%d"),
        utc_time=now.strftime("%H%M%S"),
        utc_datetime=now.strftime("%Y-%m-%d_%H%M%S"),
    )
    if not name.lower().endswith(f".{ext}"):
        name = f"{name}.{ext}"
    return Path(cfg.output_dir) / name


def write_run_export(
    cfg: ExportConfig, run_id: int, rows: list[dict[str, Any]]
) -> Path:
    out_dir = Path(cfg.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = export_path_for_run(cfg, run_id)

    if cfg.format == "xlsx":
        _write_xlsx(path, rows)
    else:
        _write_csv(path, rows)
    return path.resolve()


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in _CSV_FIELDS})


def _write_xlsx(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        from openpyxl import Workbook
    except ImportError as e:
        raise RuntimeError(
            "xlsx export requires openpyxl; pip install openpyxl or use format: csv"
        ) from e

    wb = Workbook()
    ws = wb.active
    ws.title = "run"
    ws.append(_CSV_FIELDS)
    for r in rows:
        ws.append([r.get(k, "") for k in _CSV_FIELDS])
    wb.save(path)
