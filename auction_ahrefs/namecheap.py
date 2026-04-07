from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

import httpx

from auction_ahrefs.models import AuctionListing
from auction_ahrefs.util import parse_iso_datetime, parse_usd_price


def _get_cell(row: dict[str, str], *keys: str) -> str | None:
    lower = {k.lower().strip(): v for k, v in row.items()}
    for k in keys:
        if k in row and row[k]:
            return row[k]
        lk = k.lower()
        if lk in lower and lower[lk]:
            return lower[lk]
    return None


def _parse_row(row: dict[str, str]) -> AuctionListing | None:
    domain_raw = _get_cell(row, "domain", "Domain", "domain_name", "name")
    if not domain_raw:
        return None
    domain = domain_raw.strip().lower()
    if not domain:
        return None

    price_s = _get_cell(row, "price_usd", "price", "Price", "buy_now", "current_bid")
    bids_s = _get_cell(row, "bids", "Bids", "number_of_bids")
    end_s = _get_cell(
        row, "auction_end_time", "end_time", "ends", "AuctionEndTime", "expires"
    )

    bids: int | None = None
    if bids_s is not None:
        try:
            bids = int(float(bids_s))
        except (TypeError, ValueError):
            bids = None

    price = parse_usd_price(price_s) if price_s else None
    end = parse_iso_datetime(end_s) if end_s else None

    return AuctionListing(
        source="namecheap",
        domain=domain,
        auction_end_time=end,
        price_usd=price,
        bids=bids,
        auction_type=_get_cell(row, "type", "listing_type"),
        detail_url=_get_cell(row, "url", "link"),
        raw={k: v for k, v in row.items()},
    )


def load_namecheap_csv_file(path: str | Path) -> list[AuctionListing]:
    p = Path(path)
    text = p.read_text(encoding="utf-8-sig", errors="replace")
    return _parse_csv_text(text)


def load_namecheap_csv_url(url: str, timeout_sec: float = 60.0) -> list[AuctionListing]:
    with httpx.Client(timeout=timeout_sec, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
    return _parse_csv_text(r.text)


def _dict_reader(text: str) -> csv.DictReader:
    """Use comma, semicolon, or tab as delimiter (Namecheap exports often use ``;``)."""
    sample = text[:16384]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        return csv.DictReader(io.StringIO(text), dialect=dialect)
    except csv.Error:
        pass
    first = next((ln for ln in text.splitlines() if ln.strip()), "")
    delim = ";" if first.count(";") > first.count(",") else ","
    return csv.DictReader(io.StringIO(text), delimiter=delim)


def _parse_csv_text(text: str) -> list[AuctionListing]:
    if not text.strip():
        return []
    reader = _dict_reader(text)
    out: list[AuctionListing] = []
    for row in reader:
        if not row:
            continue
        clean = {k: (v or "").strip() for k, v in row.items() if k is not None}
        listing = _parse_row(clean)
        if listing:
            out.append(listing)
    return out


def load_namecheap(cfg: Any) -> list[AuctionListing]:
    rows: list[AuctionListing] = []
    if cfg.csv_url:
        rows.extend(load_namecheap_csv_url(cfg.csv_url))
    if cfg.csv_path:
        rows.extend(load_namecheap_csv_file(cfg.csv_path))
    return rows
