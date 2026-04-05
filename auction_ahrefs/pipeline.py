from __future__ import annotations

import logging
import os
from pathlib import Path

from auction_ahrefs.ahrefs_client import AhrefsClient
from auction_ahrefs.alerts import send_webhook_summary
from auction_ahrefs.config import AppConfig, load_config
from auction_ahrefs.database import (
    domains_for_run,
    export_rows_for_run,
    fetch_latest_run_domains_with_ahrefs,
    get_cached_ahrefs,
    init_db,
    insert_run,
    make_engine,
    previous_run_id,
    session_factory,
    upsert_ahrefs_cache,
)
from auction_ahrefs.export_report import write_run_export
from auction_ahrefs.report_email import send_report_email
from auction_ahrefs.godaddy import fetch_godaddy_listings
from auction_ahrefs.namecheap import load_namecheap
from auction_ahrefs.prefilter import apply_rules, cap_for_ahrefs, sort_listings

log = logging.getLogger(__name__)


def _config_path() -> Path:
    return Path(os.environ.get("CONFIG_PATH", "config.yaml"))


def load_app_config(path: str | Path | None = None) -> AppConfig:
    if path is not None:
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"Config not found: {p}")
        return load_config(p)
    p = _config_path()
    if not p.is_file():
        example = Path("config.example.yaml")
        if example.is_file():
            return load_config(example)
        raise FileNotFoundError(
            f"Missing config at {p}. Copy config.example.yaml to config.yaml."
        )
    return load_config(p)


def ingest_listings(cfg: AppConfig) -> list:
    rows = []
    if cfg.godaddy.enabled:
        log.info("Fetching GoDaddy inventory: %s", cfg.godaddy.inventory_zip_url)
        rows.extend(fetch_godaddy_listings(cfg.godaddy.inventory_zip_url))
    if cfg.namecheap.enabled:
        log.info("Loading Namecheap CSV")
        rows.extend(load_namecheap(cfg.namecheap))
    return rows


def run_pipeline(cfg: AppConfig | None = None, *, skip_ahrefs: bool = False) -> int:
    cfg = cfg or load_app_config()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    all_rows = ingest_listings(cfg)
    ingested = len(all_rows)

    filtered = apply_rules(all_rows, cfg.prefilter)
    sort_listings(filtered, cfg.prefilter)
    shortlist = cap_for_ahrefs(filtered, cfg.prefilter)

    stats = {
        "ingested": ingested,
        "after_rules": len(filtered),
        "ahrefs_shortlist": len(shortlist),
    }

    sqlite_default = cfg.storage.sqlite_path
    if not os.environ.get("DATABASE_URL") and not os.environ.get("SQLITE_PATH"):
        os.environ.setdefault("SQLITE_PATH", sqlite_default)

    engine = make_engine()
    init_db(engine)
    SF = session_factory(engine)
    session = SF()

    run_id = insert_run(session, shortlist, note="auction_ahrefs", stats=stats)
    log.info("Run %s stats %s", run_id, stats)

    api_key = os.environ.get("AHREFS_API_KEY", "").strip()
    if skip_ahrefs:
        api_key = ""

    if not api_key:
        log.warning(
            "AHREFS_API_KEY not set; skipping Ahrefs enrichment. Listings still saved."
        )
    else:
        client = AhrefsClient(api_key, cfg.ahrefs)
        for L in shortlist:
            cached = get_cached_ahrefs(
                session, L.domain, cfg.ahrefs.cache_ttl_days
            )
            if cached:
                log.debug("Cache hit %s", L.domain)
                continue
            try:
                bundle = client.fetch_bundle(L.domain)
                upsert_ahrefs_cache(session, bundle)
                log.info(
                    "Ahrefs %s DR=%s traffic=%s",
                    L.domain,
                    bundle.domain_rating,
                    bundle.org_traffic,
                )
            except Exception:
                log.exception("Ahrefs failed for %s", L.domain)

    enriched = fetch_latest_run_domains_with_ahrefs(session, run_id)
    min_dr = cfg.alerts.min_domain_rating_to_include
    ranked = [r for r in enriched if (r.get("domain_rating") or 0) >= min_dr][
        : cfg.alerts.webhook_top_n
    ]

    prev = previous_run_id(session, run_id)
    new_domains: list[str] = []
    if prev is not None:
        prev_d = domains_for_run(session, prev)
        cur_d = {r.domain.lower() for r in shortlist}
        new_domains = sorted(cur_d - prev_d)

    wh = (cfg.alerts.webhook_url or "").strip() or os.environ.get(
        "ALERT_WEBHOOK_URL", ""
    ).strip()
    if wh and ranked:
        try:
            send_webhook_summary(
                wh,
                title="Auction Ahrefs top picks",
                run_id=run_id,
                rows=ranked,
                new_domains=new_domains,
                fmt=cfg.alerts.webhook_format,
            )
        except Exception:
            log.exception("Webhook alert failed")

    export_path = None
    if cfg.export.enabled:
        try:
            rows = export_rows_for_run(session, run_id)
            export_path = write_run_export(cfg.export, run_id, rows)
            log.info("Export written: %s", export_path)
        except Exception:
            log.exception("Export failed")

    if cfg.email_report.enabled:
        if export_path is None:
            log.warning(
                "email_report enabled but no export file was produced "
                "(set export.enabled: true or fix export errors)"
            )
        else:
            try:
                subj = cfg.email_report.subject_template.format(run_id=run_id)
                body = (
                    f"Auction Ahrefs finished run_id={run_id}.\n"
                    f"Stats: {stats}\n\n"
                    f"See attached {export_path.name}."
                )
                send_report_email(
                    cfg.email_report,
                    subject=subj,
                    body=body,
                    attachments=[export_path],
                )
                log.info("Report email sent to %s", cfg.email_report.to_addrs)
            except Exception:
                log.exception("Report email failed")

    session.close()
    return run_id


def dry_run(cfg: AppConfig | None = None) -> dict:
    cfg = cfg or load_app_config()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    all_rows = ingest_listings(cfg)
    filtered = apply_rules(all_rows, cfg.prefilter)
    sort_listings(filtered, cfg.prefilter)
    shortlist = cap_for_ahrefs(filtered, cfg.prefilter)
    return {
        "ingested": len(all_rows),
        "after_rules": len(filtered),
        "ahrefs_shortlist": len(shortlist),
        "sample": [x.domain for x in shortlist[:20]],
    }
