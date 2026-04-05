from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime
from typing import Any

import httpx

from auction_ahrefs.models import AuctionListing
from auction_ahrefs.util import parse_iso_datetime, parse_usd_price


def _row_to_listing(row: dict[str, Any]) -> AuctionListing | None:
    name = row.get("domainName") or row.get("domain") or row.get("Domain")
    if not name or not isinstance(name, str):
        return None
    domain = name.strip().lower()
    if not domain:
        return None

    end = parse_iso_datetime(row.get("auctionEndTime"))
    bids = row.get("numberOfBids")
    if bids is not None:
        try:
            bids = int(bids)
        except (TypeError, ValueError):
            bids = None

    price = parse_usd_price(row.get("price"))
    if price is None:
        price = parse_usd_price(row.get("valuation"))

    return AuctionListing(
        source="godaddy",
        domain=domain,
        auction_end_time=end,
        price_usd=price,
        bids=bids,
        auction_type=row.get("auctionType"),
        detail_url=row.get("link"),
        raw=dict(row),
    )


def fetch_godaddy_listings(zip_url: str, timeout_sec: float = 120.0) -> list[AuctionListing]:
    with httpx.Client(timeout=timeout_sec, follow_redirects=True) as client:
        r = client.get(zip_url)
        r.raise_for_status()
        buf = io.BytesIO(r.content)

    with zipfile.ZipFile(buf) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".json")]
        if not names:
            raise ValueError("No JSON file found in GoDaddy inventory zip")
        names.sort()
        payload = json.loads(zf.read(names[0]).decode("utf-8"))

    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and "data" in payload:
        rows = payload["data"]
    else:
        raise ValueError("Unexpected GoDaddy JSON shape")

    out: list[AuctionListing] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        listing = _row_to_listing(row)
        if listing:
            out.append(listing)
    return out
