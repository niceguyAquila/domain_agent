from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from auction_ahrefs.pipeline import (
    dry_run,
    load_app_config,
    resolve_config_path,
    run_pipeline,
)


def main(argv: list[str] | None = None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--config",
        "-c",
        type=Path,
        default=None,
        help="Path to config YAML (default: CONFIG_PATH or config.yaml)",
    )

    p = argparse.ArgumentParser(description="Auction listings + Ahrefs enrichment")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser(
        "run",
        parents=[common],
        help="Full pipeline (ingest, filter, Ahrefs, store, alert)",
    ).add_argument(
        "--skip-ahrefs",
        action="store_true",
        help="Ingest and persist shortlist only (no Ahrefs API calls)",
    )

    dr = sub.add_parser(
        "dry-run",
        parents=[common],
        help="Ingest + filter only; print counts",
    )
    dr.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON",
    )

    args = p.parse_args(argv)
    cfg_path = resolve_config_path(args.config)
    cfg = load_app_config(args.config) if args.config else load_app_config()

    if args.cmd == "dry-run":
        out = dry_run(cfg)
        if args.json:
            print(json.dumps(out, indent=2))
        else:
            print(
                f"ingested={out['ingested']} after_rules={out['after_rules']} "
                f"ahrefs_shortlist={out['ahrefs_shortlist']}"
            )
            if out.get("sample"):
                print("sample:", ", ".join(out["sample"]))
        return 0

    if args.cmd == "run":
        rid = run_pipeline(cfg, skip_ahrefs=args.skip_ahrefs, config_path=cfg_path)
        print(f"run_id={rid}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
