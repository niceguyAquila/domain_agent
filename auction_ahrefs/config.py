from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class GodaddyConfig(BaseModel):
    enabled: bool = True
    inventory_zip_url: str = (
        "https://inventory.auctions.godaddy.com/expiring_auctions_non_adult.json.zip"
    )


class NamecheapConfig(BaseModel):
    enabled: bool = False
    csv_path: str | None = None
    csv_url: str | None = None


class PrefilterConfig(BaseModel):
    max_domains_to_ahrefs: int = 50
    allowed_tlds: list[str] = Field(
        default_factory=lambda: [".com", ".net", ".org", ".co"]
    )
    min_bids: int = 0
    min_price_usd: float | None = None
    max_price_usd: float | None = None
    min_domain_length: int = 3
    max_domain_length: int = 63
    domain_contains: list[str] = Field(default_factory=list)
    exclude_adult: bool = True
    exclude_idn: bool = True
    min_domain_age_years: int | None = None
    max_hours_until_auction_end: float | None = None
    min_hours_until_auction_end: float | None = None
    auction_types_allowed: list[str] | None = None
    sort_before_cap: Literal["bids_desc", "price_asc", "domain"] = "bids_desc"


class AhrefsConfig(BaseModel):
    api_base: str = "https://api.ahrefs.com/v3"
    report_date_offset_days: int = 1
    cache_ttl_days: int = 14
    sleep_between_requests_sec: float = 0.6
    max_retries: int = 4
    fetch_domain_rating: bool = True
    fetch_metrics: bool = True


class StorageConfig(BaseModel):
    sqlite_path: str = "./data/pipeline.db"


class AlertsConfig(BaseModel):
    webhook_url: str | None = None
    webhook_top_n: int = 15
    min_domain_rating_to_include: float = 5.0
    webhook_format: Literal["slack", "discord"] = "slack"


class AppConfig(BaseModel):
    godaddy: GodaddyConfig = Field(default_factory=GodaddyConfig)
    namecheap: NamecheapConfig = Field(default_factory=NamecheapConfig)
    prefilter: PrefilterConfig = Field(default_factory=PrefilterConfig)
    ahrefs: AhrefsConfig = Field(default_factory=AhrefsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    return AppConfig.model_validate(data)
