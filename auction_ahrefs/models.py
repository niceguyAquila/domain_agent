from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class AuctionListing:
    source: str
    domain: str
    auction_end_time: datetime | None = None
    price_usd: float | None = None
    bids: int | None = None
    auction_type: str | None = None
    detail_url: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def listing_fingerprint(self) -> str:
        end = self.auction_end_time.isoformat() if self.auction_end_time else ""
        return f"{self.source}|{self.domain.lower()}|{end}"


@dataclass
class AhrefsBundle:
    domain: str
    report_date: str
    domain_rating: float | None = None
    ahrefs_rank: int | None = None
    org_keywords: int | None = None
    org_traffic: int | None = None
    org_cost_usd_cents: int | None = None
    raw_domain_rating: dict[str, Any] = field(default_factory=dict)
    raw_metrics: dict[str, Any] = field(default_factory=dict)
