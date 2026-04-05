from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import httpx

from auction_ahrefs.config import AhrefsConfig
from auction_ahrefs.models import AhrefsBundle


class AhrefsClient:
    def __init__(self, api_key: str, cfg: AhrefsConfig) -> None:
        self._api_key = api_key
        self._cfg = cfg
        self._base = cfg.api_base.rstrip("/")

    def report_date_str(self) -> str:
        d = date.today() - timedelta(days=self._cfg.report_date_offset_days)
        return d.isoformat()

    def fetch_bundle(self, domain: str) -> AhrefsBundle:
        rep = self.report_date_str()
        bundle = AhrefsBundle(domain=domain, report_date=rep)

        if self._cfg.fetch_domain_rating:
            dr = self._get_json(
                "/site-explorer/domain-rating",
                {"target": domain, "date": rep, "output": "json"},
            )
            bundle.raw_domain_rating = dr
            obj = dr.get("domain_rating")
            if isinstance(obj, dict):
                bundle.domain_rating = _to_float(obj.get("domain_rating"))
                bundle.ahrefs_rank = _to_int(obj.get("ahrefs_rank"))
            elif isinstance(obj, (int, float)):
                bundle.domain_rating = float(obj)
                bundle.ahrefs_rank = _to_int(dr.get("ahrefs_rank"))

        if self._cfg.fetch_metrics:
            met = self._get_json(
                "/site-explorer/metrics",
                {
                    "target": domain,
                    "date": rep,
                    "mode": "domain",
                    "output": "json",
                },
            )
            bundle.raw_metrics = met
            m = met.get("metrics")
            if isinstance(m, dict):
                bundle.org_keywords = _to_int(m.get("org_keywords"))
                bundle.org_traffic = _to_int(m.get("org_traffic"))
                bundle.org_cost_usd_cents = _to_int(m.get("org_cost"))

        return bundle

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base}{path}"
        headers = {"Authorization": f"Bearer {self._api_key}"}
        delay = self._cfg.sleep_between_requests_sec
        last_err: Exception | None = None
        for attempt in range(self._cfg.max_retries):
            with httpx.Client(timeout=60.0) as client:
                r = client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                wait = min(60.0, 2.0**attempt)
                time.sleep(wait)
                last_err = RuntimeError(f"Ahrefs rate limited: {r.text[:500]}")
                continue
            if 500 <= r.status_code < 600:
                time.sleep(min(30.0, 1.5**attempt))
                last_err = RuntimeError(f"Ahrefs server error {r.status_code}")
                continue
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError("Ahrefs returned non-object JSON")
            time.sleep(delay)
            return data

        if last_err:
            raise last_err
        raise RuntimeError("Ahrefs request failed")


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
