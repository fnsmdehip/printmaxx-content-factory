#!/usr/bin/env python3

from __future__ import annotations
"""
Engagement Bait Content Converter
Source: ALPHA_GOV_003 - "169 ENGAGEMENT_BAIT alpha entries sitting unused. Each can generate 3+ social posts."

Reads all ENGAGEMENT_BAIT alpha entries and converts each into 3+ ready-to-post social posts.
169 entries x 3 posts = 507+ content pieces from existing research. Zero cost.

Usage:
    python3 engagement_bait_converter.py                    # Convert all EB entries
    python3 engagement_bait_converter.py --limit 20          # Convert first 20
    python3 engagement_bait_converter.py --niche faith       # Filter by niche
    python3 engagement_bait_converter.py --output-csv        # Output as Buffer CSV
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ALPHA_FILE_1 = BASE_DIR / "LEDGER" / "ALPHA_STAGING.csv"
ALPHA_FILE_2 = BASE_DIR / "02_TRACKING" / "alpha" / "ALPHA_STAGING.csv"
OUTPUT_DIR = BASE_DIR / "AUTOMATIONS" / "content_posting"
OUTPUT_CSV = OUTPUT_DIR / "engagement_bait_posts.csv"
LOG_FILE = BASE_DIR / "AUTOMATIONS" / "logs" / "eb_converter.log"

# Post transformation patterns
PATTERNS = {
    "hot_take": {
        "format": "hot take: {insight}\n\nmost people won't agree but it's true.",
        "platform": "twitter",
    },
    "contrarian": {
        "format": "unpopular opinion:\n\n{insight}\n\nchange my mind.",
        "platform": "twitter",
    },
    "thread_hook": {
        "format": "{insight}\n\nhere's what nobody tells you about this:\n\n(thread)",
        "platform": "twitter",
    },
    "question": {
        "format": "real question:\n\n{insight}\n\nwhat's your take?",
        "platform": "twitter",
    },
    "confession": {
        "format": "i used to think {old_belief}.\n\nthen i learned {insight}.\n\neverything changed.",
        "platform": "twitter",
    },
    "callout": {
        "format": "stop {bad_thing}.\n\n{insight}\n\nit's that simple.",
        "platform": "twitter",
    },
    "linkedin_insight": {
        "format": "Something I wish I knew earlier:\n\n{insight}\n\nThe people winning right now figured this out 6 months ago.\n\nWhat would you add?",
        "platform": "linkedin",
    },
    "linkedin_story": {
        "format": "I made a mistake.\n\n{insight}\n\nTook me way too long to figure this out.\n\nDon't make the same error. Save this.",
        "platform": "linkedin",
    },
    "ig_carousel_title": {
        "format": "SLIDE 1: {short_hook}\n\nSLIDE 2-4: {insight}\n\nSLIDE 5: Follow @PRINTMAXXER for more\n\n{short_hook} #buildinpublic #indiehacker #solopreneur",
        "platform": "instagram",
    },
}


def load_engagement_bait_entries():
    """Load all ENGAGEMENT_BAIT entries from both alpha files."""
    entries = []

    for alpha_file in [ALPHA_FILE_1, ALPHA_FILE_2]:
        if not alpha_file.exists():
            continue

        with open(alpha_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                status = row.get('status', '').strip().upper()
                if 'ENGAGEMENT_BAIT' in status or 'REPURPOSE' in status:
                    entries.append(row)

    # Deduplicate by alpha_id
    seen = set()
    unique = []
    for e in entries:
        aid = e.get('alpha_id', '')
        if aid and aid not in seen:
            seen.add(aid)
            unique.append(e)

    return unique


def extract_insight(entry):
    """Extract the core insight from an EB entry."""
    # Try different fields
    for field in ['tactic', 'description', 'title', 'extracted_method']:
        value = entry.get(field, '').strip()
        if value and len(value) > 10:
            return value

    return entry.get('reviewer_notes', '')


def transform_to_posts(entry, max_posts=3):
    """Transform a single EB entry into multiple social posts."""
    posts = []
    insight = extract_insight(entry)

    if not insight or len(insight) < 10:
        return posts

    # Clean the insight
    insight = insight.strip()
    insight_lower = insight.lower()

    # Generate short hook
    words = insight.split()
    short_hook = ' '.join(words[:8]) if len(words) > 8 else insight

    # Select best patterns based on content
    selected_patterns = []

    if any(word in insight_lower for word in ['wrong', 'mistake', 'stop', 'don\'t']):
        selected_patterns.extend(["callout", "confession", "linkedin_story"])
    elif any(word in insight_lower for word in ['how', 'framework', 'system', 'method']):
        selected_patterns.extend(["thread_hook", "linkedin_insight", "ig_carousel_title"])
    elif '?' in insight:
        selected_patterns.extend(["question", "hot_take", "linkedin_insight"])
    else:
        selected_patterns.extend(["hot_take", "contrarian", "linkedin_insight"])

    for pattern_name in selected_patterns[:max_posts]:
        pattern = PATTERNS[pattern_name]
        try:
            # Simple template filling
            post_text = pattern["format"].format(
                insight=insight[:250],
                short_hook=short_hook[:80],
                old_belief=f"the opposite of this",
                bad_thing="ignoring this",
            )

            # Enforce character limits
            platform = pattern["platform"]
            if platform == "twitter" and len(post_text) > 280:
                post_text = post_text[:277] + "..."
            elif platform == "linkedin" and len(post_text) > 3000:
                post_text = post_text[:2997] + "..."

            posts.append({
                "alpha_id": entry.get('alpha_id', 'UNKNOWN'),
                "source": entry.get('source', ''),
                "platform": platform,
                "pattern": pattern_name,
                "content": post_text,
                "generated_at": datetime.now().isoformat(),
            })
        except Exception:
            continue

    return posts


def convert_all(limit=None, niche_filter=None):
    """Convert all EB entries to posts."""
    print(f"\n{'='*60}")
    print("ENGAGEMENT BAIT CONTENT CONVERTER")
    print(f"{'='*60}")

    entries = load_engagement_bait_entries()
    print(f"Loaded {len(entries)} engagement bait entries")

    if niche_filter:
        entries = [e for e in entries if niche_filter.lower() in e.get('applies_to_niches', '').lower() or e.get('applies_to_niches', '').upper() == 'ALL']
        print(f"Filtered to {len(entries)} entries for niche: {niche_filter}")

    if limit:
        entries = entries[:limit]
        print(f"Limited to {limit} entries")

    all_posts = []
    skipped = 0

    for entry in entries:
        posts = transform_to_posts(entry, max_posts=3)
        if posts:
            all_posts.extend(posts)
        else:
            skipped += 1

    print(f"\nGenerated {len(all_posts)} posts from {len(entries) - skipped} entries")
    print(f"Skipped {skipped} entries (insufficient content)")

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Buffer-compatible CSV
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["alpha_id", "platform", "pattern", "content", "source", "generated_at"])
        writer.writeheader()
        writer.writerows(all_posts)
    print(f"\nCSV written to: {OUTPUT_CSV}")

    # Platform-specific CSVs for Buffer upload
    platforms = set(p["platform"] for p in all_posts)
    for platform in platforms:
        platform_posts = [p for p in all_posts if p["platform"] == platform]
        platform_file = OUTPUT_DIR / f"eb_{platform}_posts.csv"
        with open(platform_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["content"])
            for p in platform_posts:
                writer.writerow([p["content"]])
        print(f"  {platform}: {len(platform_posts)} posts -> {platform_file}")

    # Summary stats
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total posts generated: {len(all_posts)}")
    for platform in platforms:
        count = sum(1 for p in all_posts if p["platform"] == platform)
        print(f"  {platform}: {count}")
    print(f"\nAt 5 posts/day, this is {len(all_posts) // 5} days of content.")
    print(f"At 3 posts/day, this is {len(all_posts) // 3} days of content.")
    print(f"{'='*60}\n")

    # Log
    os.makedirs(LOG_FILE.parent, exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} | Entries: {len(entries)} | Posts: {len(all_posts)} | Skipped: {skipped}\n")

    return all_posts


def main():
    parser = argparse.ArgumentParser(description="Engagement Bait Content Converter")
    parser.add_argument("--limit", type=int, help="Max entries to convert")
    parser.add_argument("--niche", type=str, help="Filter by niche (faith, fitness, AI, etc)")
    parser.add_argument("--output-csv", action="store_true", help="Output as Buffer-ready CSV")
    args = parser.parse_args()

    convert_all(limit=args.limit, niche_filter=args.niche)


if __name__ == "__main__":
    main()
