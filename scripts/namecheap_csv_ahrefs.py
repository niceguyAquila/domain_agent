"""
Local-only: read a Namecheap-style CSV, call Ahrefs Site Explorer for each domain, write a results CSV.

Loads ``<repo>/.env`` if present (``python-dotenv``). ``AHREFS_API_KEY`` must be set there
or in the environment. Optional YAML config supplies Ahrefs tuning
(same `ahrefs:` block as the main auction bot — rate limits, which endpoints to call).

Run from anywhere:

  python scripts/namecheap_csv_ahrefs.py path/to/namecheap.csv -o ahrefs_out.csv

Or from repo root with PYTHONPATH=. :

  python -m scripts.namecheap_csv_ahrefs ...
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from auction_ahrefs.ahrefs_client import AhrefsClient
from auction_ahrefs.config import AhrefsConfig, load_config
from auction_ahrefs.models import AuctionListing, AhrefsBundle
from auction_ahrefs.namecheap import load_namecheap_csv_file
from auction_ahrefs.util import load_dotenv_from_repo_root

log = logging.getLogger(__name__)

_DEFAULT_OUTPUT_DIR = _REPO_ROOT / "export" / "namecheap"

_OUTPUT_FIELDS = [
    "domain",
    "price_usd",
    "bids",
    "auction_end_utc",
    "auction_type",
    "detail_url",
    "domain_rating",
    "ahrefs_rank",
    "org_keywords",
    "org_traffic",
    "org_traffic_value_usd",
    "ahrefs_report_date",
    "ahrefs_fetched_utc",
    "error",
]


def _resolve_ahrefs_config(config_path: Path | None) -> AhrefsConfig:
    if config_path is not None:
        if not config_path.is_file():
            raise FileNotFoundError(f"Config not found: {config_path}")
        return load_config(config_path).ahrefs
    env_path = os.environ.get("CONFIG_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return load_config(p).ahrefs
    default = Path("config.yaml")
    if default.is_file():
        return load_config(default).ahrefs
    return AhrefsConfig()


def _dedupe_listings(listings: list[AuctionListing]) -> list[AuctionListing]:
    seen: set[str] = set()
    out: list[AuctionListing] = []
    for L in listings:
        d = L.domain.lower()
        if d in seen:
            continue
        seen.add(d)
        out.append(L)
    return out


def _listing_row_base(L: AuctionListing) -> dict[str, object]:
    end = L.auction_end_time.isoformat() if L.auction_end_time else ""
    return {
        "domain": L.domain,
        "price_usd": L.price_usd if L.price_usd is not None else "",
        "bids": L.bids if L.bids is not None else "",
        "auction_end_utc": end,
        "auction_type": L.auction_type or "",
        "detail_url": L.detail_url or "",
    }


def _bundle_to_row(
    base: dict[str, object], bundle: AhrefsBundle, *, error: str = ""
) -> dict[str, object]:
    org_val = ""
    if bundle.org_cost_usd_cents is not None:
        org_val = round(bundle.org_cost_usd_cents / 100.0, 2)
    fetched = datetime.now(timezone.utc).isoformat()
    return {
        **base,
        "domain_rating": bundle.domain_rating if bundle.domain_rating is not None else "",
        "ahrefs_rank": bundle.ahrefs_rank if bundle.ahrefs_rank is not None else "",
        "org_keywords": bundle.org_keywords if bundle.org_keywords is not None else "",
        "org_traffic": bundle.org_traffic if bundle.org_traffic is not None else "",
        "org_traffic_value_usd": org_val,
        "ahrefs_report_date": bundle.report_date,
        "ahrefs_fetched_utc": fetched if not error else "",
        "error": error,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Namecheap CSV → Ahrefs metrics (local CSV in/out, no database)."
    )
    p.add_argument(
        "csv_path",
        type=Path,
        help="Path to Namecheap export CSV (domain column required)",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write results CSV here (default: /export/namecheap/<input>_ahrefs.csv)",
    )
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default=None,
        help="YAML config path (uses its ahrefs: section; default CONFIG_PATH or ./config.yaml)",
    )
    p.add_argument(
        "--max",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N domains after de-duplication (for testing)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse CSV and print domain count only (no Ahrefs calls)",
    )
    args = p.parse_args(argv)

    load_dotenv_from_repo_root()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    inp = args.csv_path
    if not inp.is_file():
        log.error("Input CSV not found: %s", inp)
        return 1

    listings = load_namecheap_csv_file(inp)
    listings = _dedupe_listings(listings)
    if args.max is not None:
        listings = listings[: max(0, args.max)]

    log.info("Loaded %s domain(s) from %s", len(listings), inp)
    if args.dry_run:
        for L in listings[:20]:
            print(L.domain)
        if len(listings) > 20:
            print(f"... and {len(listings) - 20} more")
        return 0

    api_key = os.environ.get("AHREFS_API_KEY", "").strip()
    if not api_key:
        log.error("Set AHREFS_API_KEY in the environment.")
        return 1

    ahrefs_cfg = _resolve_ahrefs_config(args.config)
    client = AhrefsClient(api_key, ahrefs_cfg)

    out_path = args.output
    if out_path is None:
        out_path = _DEFAULT_OUTPUT_DIR / f"{inp.stem}_ahrefs.csv"

    rows_out: list[dict[str, object]] = []
    for L in listings:
        base = _listing_row_base(L)
        try:
            bundle = client.fetch_bundle(L.domain)
            log.info(
                "Ahrefs %s DR=%s traffic=%s",
                L.domain,
                bundle.domain_rating,
                bundle.org_traffic,
            )
            rows_out.append(_bundle_to_row(base, bundle))
        except Exception as e:
            log.exception("Ahrefs failed for %s", L.domain)
            empty = AhrefsBundle(domain=L.domain, report_date=client.report_date_str())
            rows_out.append(
                _bundle_to_row(base, empty, error=str(e)[:500]),
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_OUTPUT_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_out)

    log.info("Wrote %s", out_path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
