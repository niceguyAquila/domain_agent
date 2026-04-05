from __future__ import annotations

from typing import Any, Literal

import httpx


def send_webhook_summary(
    url: str,
    *,
    title: str,
    run_id: int,
    rows: list[dict[str, Any]],
    new_domains: list[str],
    fmt: Literal["slack", "discord"] = "slack",
) -> None:
    lines = [f"{title} | run_id={run_id}"]
    if new_domains:
        lines.append("New vs previous run: " + ", ".join(new_domains[:50]))
        if len(new_domains) > 50:
            lines.append(f"(+{len(new_domains) - 50} more)")
    lines.append("")
    for r in rows:
        dr = r.get("domain_rating")
        ot = r.get("org_traffic")
        lines.append(
            f"- {r.get('domain')} (src={r.get('source')}) DR={dr} org_traffic={ot} bids={r.get('bids')}"
        )
    text = "\n".join(lines)
    if fmt == "discord":
        payload: dict[str, Any] = {"content": text[:1900]}
    else:
        payload = {"text": text[:1900]}
    with httpx.Client(timeout=30.0) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
