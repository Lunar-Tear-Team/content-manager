#!/usr/bin/env python3
"""
Generate a bundle_index.json from dumped master data.

Reads the master data JSON tables and groups content into monthly bundles,
mapping event quest chapters, gacha banners, login bonuses, and side stories
to the month when they were originally released.

Usage:
  python generate_bundle_index.py
  python generate_bundle_index.py --dump-dir /path/to/master_data
  python generate_bundle_index.py --output bundle_index.json
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone


def load_table(dump_dir, filename):
    path = os.path.join(dump_dir, filename)
    if not os.path.exists(path):
        print(f"  Warning: {filename} not found, skipping", file=sys.stderr)
        return []
    with open(path) as f:
        return json.load(f)


def ms_to_month(ms):
    """Convert millisecond-epoch timestamp to 'YYYY-MM' bucket."""
    if not ms or ms <= 0:
        return "unknown"
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m")
    except (OSError, OverflowError, ValueError):
        return "overflow"


def is_unreleased(ms):
    """Check if timestamp indicates unreleased content (2099+)."""
    if not ms or ms <= 0:
        return False
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).year >= 2099
    except (OSError, OverflowError, ValueError):
        return False


def is_permanent(end_ms):
    """Check for sentinel end datetimes (year 2090+)."""
    if not end_ms or end_ms <= 0:
        return False
    try:
        return datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).year >= 2090
    except (OSError, OverflowError, ValueError):
        return True


MOM_BANNER_DOMAIN_GACHA = 1


def main():
    parser = argparse.ArgumentParser(
        description="Generate bundle_index.json from dumped master data."
    )
    parser.add_argument(
        "--dump-dir", default="master_data",
        help="Directory containing dumped master data JSON files"
    )
    parser.add_argument(
        "--output", default="bundle_index.json",
        help="Output file path (default: bundle_index.json)"
    )
    args = parser.parse_args()
    # Validate input
    if os.path.isfile(args.dump_dir):
        if args.dump_dir.endswith('.bin.e') or args.dump_dir.endswith('.bin'):
            print(f"ERROR: --dump-dir expects a directory of extracted JSON table files, not a binary.", file=sys.stderr)
            print(f"  You passed: {args.dump_dir}", file=sys.stderr)
            print(f"  First extract the tables: python dump_masterdata.py --input {args.dump_dir} --output-dir master_data", file=sys.stderr)
            print(f"  Then run: python {sys.argv[0]} --dump-dir master_data", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"ERROR: --dump-dir must be a directory, got a file: {args.dump_dir}", file=sys.stderr)
            sys.exit(1)

    if not os.path.isdir(args.dump_dir):
        print(f"ERROR: directory not found: {args.dump_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading master data from {args.dump_dir}/...")

    # --- Load tables ---
    events = load_table(args.dump_dir, "EntityMEventQuestChapterTable.json")
    banners = load_table(args.dump_dir, "EntityMMomBannerTable.json")
    login_bonuses = load_table(args.dump_dir, "EntityMLoginBonusTable.json")
    ss_limit = load_table(args.dump_dir, "EntityMSideStoryQuestLimitContentTable.json")

    # --- Build bundles ---
    bundles = defaultdict(lambda: {
        "event_chapters": [],
        "gacha_ids": [],
        "login_bonuses": [],
        "side_stories": [],
    })
    permanent = {
        "label": "Permanent Content",
        "event_chapters": [],
        "gacha_ids": [],
        "login_bonuses": [],
        "side_stories": [],
    }
    unreleased = {
        "label": "Unreleased Content",
        "event_chapters": [],
        "gacha_ids": [],
        "login_bonuses": [],
        "side_stories": [],
    }

    # Event Quest Chapters
    chapter_month = {}
    for e in events:
        chapter_id = e["EventQuestChapterId"]
        start = e.get("StartDatetime", 0)
        end = e.get("EndDatetime", 0)
        month = ms_to_month(start)
        chapter_month[chapter_id] = month

        if is_unreleased(start):
            unreleased["event_chapters"].append(chapter_id)
        elif is_permanent(end):
            permanent["event_chapters"].append(chapter_id)
            bundles[month]["event_chapters"].append(chapter_id)
        else:
            bundles[month]["event_chapters"].append(chapter_id)

    # Gacha Banners (MomBanner type 1)
    gacha_seen = defaultdict(set)
    for b in banners:
        if b.get("DestinationDomainType") != MOM_BANNER_DOMAIN_GACHA:
            continue
        gacha_id = b["DestinationDomainId"]
        start = b.get("StartDatetime", 0)
        month = ms_to_month(start)

        if is_unreleased(start):
            unreleased["gacha_ids"].append(gacha_id)
        elif gacha_id not in gacha_seen[month]:
            bundles[month]["gacha_ids"].append(gacha_id)
            gacha_seen[month].add(gacha_id)

    # Login Bonuses
    for lb in login_bonuses:
        lb_id = lb["LoginBonusId"]
        start = lb.get("StartDatetime", 0)
        month = ms_to_month(start)

        if is_unreleased(start):
            unreleased["login_bonuses"].append(lb_id)
        else:
            bundles[month]["login_bonuses"].append(lb_id)

    # Side Stories (linked via event chapters)
    for ss in ss_limit:
        ss_id = ss["SideStoryQuestLimitContentId"]
        chapter_id = ss["EventQuestChapterId"]
        month = chapter_month.get(chapter_id, "unknown")
        bundles[month]["side_stories"].append(ss_id)

    # --- Build output ---
    # Add labels to each monthly bundle
    output_bundles = {}
    for month in sorted(bundles.keys()):
        if month in ("unknown", "overflow", "1970-01"):
            continue
        b = bundles[month]
        output_bundles[month] = {
            "label": month,
            "event_chapters": b["event_chapters"],
            "gacha_ids": b["gacha_ids"],
            "login_bonuses": b["login_bonuses"],
            "side_stories": b["side_stories"],
        }

    result = {
        "bundles": output_bundles,
        "permanent": permanent,
        "unreleased": unreleased,
    }

    # --- Write output ---
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # --- Summary ---
    total_events = sum(len(b["event_chapters"]) for b in output_bundles.values())
    total_gacha = sum(len(b["gacha_ids"]) for b in output_bundles.values())
    total_login = sum(len(b["login_bonuses"]) for b in output_bundles.values())
    total_ss = sum(len(b["side_stories"]) for b in output_bundles.values())

    print(f"\nBundle index generated: {args.output}")
    print(f"  Monthly bundles: {len(output_bundles)}")
    print(f"  Events: {total_events} ({len(permanent['event_chapters'])} permanent, {len(unreleased['event_chapters'])} unreleased)")
    print(f"  Gacha banners: {total_gacha} ({len(unreleased['gacha_ids'])} unreleased)")
    print(f"  Login bonuses: {total_login} ({len(unreleased['login_bonuses'])} unreleased)")
    print(f"  Side stories: {total_ss}")
    if output_bundles:
        print(f"  Date range: {sorted(output_bundles.keys())[0]} -> {sorted(output_bundles.keys())[-1]}")
    else:
        print(f"  Date range: (none — no content found, check --dump-dir)")


if __name__ == "__main__":
    main()
