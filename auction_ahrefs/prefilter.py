from __future__ import annotations

from datetime import datetime, timezone

from auction_ahrefs.config import PrefilterConfig
from auction_ahrefs.models import AuctionListing


def _tld(domain: str) -> str:
    parts = domain.lower().split(".")
    if len(parts) < 2:
        return ""
    return "." + parts[-1]


def _is_idn(domain: str) -> bool:
    d = domain.lower()
    if any(ord(c) > 127 for c in d):
        return True
    return any(label.startswith("xn--") for label in d.split("."))


def apply_rules(
    listings: list[AuctionListing],
    cfg: PrefilterConfig,
    *,
    now: datetime | None = None,
) -> list[AuctionListing]:
    allowed = {t.lower() if t.startswith(".") else f".{t.lower()}" for t in cfg.allowed_tlds}
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    out: list[AuctionListing] = []

    for L in listings:
        d = L.domain.lower()
        if len(d) < cfg.min_domain_length or len(d) > cfg.max_domain_length:
            continue
        if allowed and _tld(d) not in allowed:
            continue
        if cfg.exclude_idn and _is_idn(d):
            continue
        if cfg.exclude_adult:
            adult = L.raw.get("isAdult")
            if adult is True:
                continue
        if cfg.min_domain_age_years is not None:
            age = L.raw.get("domainAge")
            try:
                age_i = int(age) if age is not None else None
            except (TypeError, ValueError):
                age_i = None
            if age_i is None or age_i < cfg.min_domain_age_years:
                continue
        if cfg.auction_types_allowed:
            at = L.auction_type or L.raw.get("auctionType")
            if at not in cfg.auction_types_allowed:
                continue
        if cfg.max_hours_until_auction_end is not None:
            end = L.auction_end_time
            if end is None:
                continue
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            hours_left = (end - now).total_seconds() / 3600.0
            if hours_left < 0:
                continue
            if hours_left > cfg.max_hours_until_auction_end:
                continue
            if cfg.min_hours_until_auction_end is not None:
                if hours_left < cfg.min_hours_until_auction_end:
                    continue
        elif cfg.min_hours_until_auction_end is not None and L.auction_end_time:
            end = L.auction_end_time
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            hours_left = (end - now).total_seconds() / 3600.0
            if hours_left < cfg.min_hours_until_auction_end:
                continue
        if L.bids is not None and L.bids < cfg.min_bids:
            continue
        if cfg.min_price_usd is not None:
            if L.price_usd is None or L.price_usd < cfg.min_price_usd:
                continue
        if cfg.max_price_usd is not None:
            if L.price_usd is not None and L.price_usd > cfg.max_price_usd:
                continue
        if cfg.domain_contains:
            if not any(s.lower() in d for s in cfg.domain_contains):
                continue
        out.append(L)
    return out


def sort_listings(listings: list[AuctionListing], cfg: PrefilterConfig) -> list[AuctionListing]:
    if cfg.sort_before_cap == "bids_desc":
        listings.sort(
            key=lambda x: (
                -(x.bids or 0),
                x.price_usd if x.price_usd is not None else float("inf"),
                x.domain,
            )
        )
    elif cfg.sort_before_cap == "price_asc":
        listings.sort(
            key=lambda x: (
                x.price_usd if x.price_usd is not None else float("inf"),
                -(x.bids or 0),
                x.domain,
            )
        )
    else:
        listings.sort(key=lambda x: x.domain)
    return listings


def cap_for_ahrefs(listings: list[AuctionListing], cfg: PrefilterConfig) -> list[AuctionListing]:
    cap = max(0, cfg.max_domains_to_ahrefs)
    return listings[:cap]
