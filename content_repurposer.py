#!/usr/bin/env python3

from __future__ import annotations
"""
Content Repurposer for PRINTMAXX Meme Page Automation

Scrapes trending content from source accounts, rewrites captions via Claude API,
schedules with natural timing, tracks duplicates, detects winners.

Usage:
    python3 AUTOMATIONS/content_repurposer.py --sources          # Show source accounts
    python3 AUTOMATIONS/content_repurposer.py --scrape            # Scrape trending content
    python3 AUTOMATIONS/content_repurposer.py --rewrite           # Rewrite captions via Claude
    python3 AUTOMATIONS/content_repurposer.py --schedule          # Schedule posts with natural timing
    python3 AUTOMATIONS/content_repurposer.py --winners           # Show top performing content
    python3 AUTOMATIONS/content_repurposer.py --dry-run           # Preview all actions without executing
"""

import argparse
import csv
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# --- Project path validation ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
AUTOMATIONS_DIR = PROJECT_ROOT / "AUTOMATIONS"
LEDGER_DIR = PROJECT_ROOT / "LEDGER"
OPS_DIR = PROJECT_ROOT / "OPS"
OUTPUT_DIR = PROJECT_ROOT / "output" / "repurposed"
DB_FILE = AUTOMATIONS_DIR / "content_repurposer.db"


def safe_path(target: Path) -> Path:
    """Verify path is within project root."""
    resolved = Path(target).resolve()
    if not str(resolved).startswith(str(PROJECT_ROOT)):
        raise ValueError(f"BLOCKED: {resolved} is outside project root {PROJECT_ROOT}")
    return resolved


# Ensure directories exist
for d in [OUTPUT_DIR, OUTPUT_DIR / "scraped", OUTPUT_DIR / "rewritten", OUTPUT_DIR / "scheduled"]:
    safe_path(d).mkdir(parents=True, exist_ok=True)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Source accounts to monitor (Twitter handles without @)
SOURCE_ACCOUNTS = {
    "meme_entertainment": [
        "memezar", "reactjpg", "dloading", "NoContextBrits",
        "InternetH0F", "cursedimages_", "BestMemes",
    ],
    "engagement_bait": [
        "TheFigen_", "AskReddit_top", "redikiod",
    ],
    "tech_memes": [
        "ProgrammerHumor", "iamdevloper", "CodeMemes_",
    ],
}

# Reddit sources (JSON API, no auth needed)
REDDIT_SOURCES = [
    {"subreddit": "memes", "sort": "hot", "limit": 25},
    {"subreddit": "me_irl", "sort": "hot", "limit": 15},
    {"subreddit": "dankmemes", "sort": "hot", "limit": 15},
    {"subreddit": "shitposting", "sort": "hot", "limit": 10},
    {"subreddit": "funny", "sort": "rising", "limit": 10},
]

# Anti-shadowban settings
POSTING_CONFIG = {
    "min_interval_minutes": 45,
    "max_interval_minutes": 180,
    "max_posts_per_day": 15,
    "active_hours_start": 9,   # 9 AM
    "active_hours_end": 23,    # 11 PM
    "rest_day_interval": 14,   # Take a break every 14 days
    "min_caption_length": 10,
    "max_caption_length": 280,
}

# Claude API config (for caption rewriting)
CLAUDE_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-6-20250514"


# ============================================================================
# DATABASE (duplicate tracking + engagement)
# ============================================================================

def init_db():
    """Initialize SQLite database for tracking."""
    conn = sqlite3.connect(str(DB_FILE))
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT UNIQUE,
            source_account TEXT,
            source_platform TEXT,
            original_caption TEXT,
            rewritten_caption TEXT,
            content_hash TEXT,
            media_type TEXT,
            scraped_at TEXT,
            posted_at TEXT,
            post_url TEXT,
            likes INTEGER DEFAULT 0,
            retweets INTEGER DEFAULT 0,
            replies INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            engagement_score REAL DEFAULT 0,
            status TEXT DEFAULT 'scraped'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS posting_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            posts_count INTEGER DEFAULT 0,
            last_post_time TEXT
        )
    """)
    conn.commit()
    return conn


def content_exists(conn, source_url: str) -> bool:
    """Check if content was already scraped."""
    c = conn.cursor()
    c.execute("SELECT 1 FROM content WHERE source_url = ?", (source_url,))
    return c.fetchone() is not None


def save_content(conn, data: dict):
    """Save scraped content to database."""
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO content
        (source_url, source_account, source_platform, original_caption, content_hash, media_type, scraped_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'scraped')
    """, (
        data["source_url"], data["source_account"], data["source_platform"],
        data["original_caption"], data["content_hash"], data["media_type"],
        datetime.now().isoformat()
    ))
    conn.commit()


def get_posts_today(conn) -> int:
    """Get number of posts made today."""
    today = datetime.now().strftime("%Y-%m-%d")
    c = conn.cursor()
    c.execute("SELECT posts_count FROM posting_log WHERE date = ?", (today,))
    row = c.fetchone()
    return row[0] if row else 0


def increment_post_count(conn):
    """Increment today's post count."""
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()
    c = conn.cursor()
    c.execute("SELECT posts_count FROM posting_log WHERE date = ?", (today,))
    row = c.fetchone()
    if row:
        c.execute("UPDATE posting_log SET posts_count = posts_count + 1, last_post_time = ? WHERE date = ?",
                  (now, today))
    else:
        c.execute("INSERT INTO posting_log (date, posts_count, last_post_time) VALUES (?, 1, ?)",
                  (today, now))
    conn.commit()


# ============================================================================
# SCRAPING
# ============================================================================

def scrape_reddit(subreddit: str, sort: str = "hot", limit: int = 25) -> list:
    """Scrape Reddit via JSON API (no auth needed)."""
    import urllib.request

    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json?limit={limit}"
    headers = {"User-Agent": "PRINTMAXX-ContentBot/1.0"}

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"  [ERROR] Failed to scrape r/{subreddit}: {e}")
        return []

    results = []
    for post in data.get("data", {}).get("children", []):
        d = post.get("data", {})
        if d.get("over_18", False):
            continue  # Skip NSFW
        if d.get("is_self", False) and not d.get("selftext"):
            continue  # Skip empty self posts

        source_url = f"https://reddit.com{d.get('permalink', '')}"
        caption = d.get("title", "")
        media_type = "image" if d.get("url", "").endswith((".jpg", ".png", ".gif", ".webp")) else "text"
        if d.get("is_video"):
            media_type = "video"

        content_hash = hashlib.md5(caption.encode()).hexdigest()

        results.append({
            "source_url": source_url,
            "source_account": f"r/{subreddit}",
            "source_platform": "reddit",
            "original_caption": caption,
            "content_hash": content_hash,
            "media_type": media_type,
            "score": d.get("score", 0),
            "num_comments": d.get("num_comments", 0),
            "media_url": d.get("url", ""),
        })

    return results


def scrape_all_sources() -> list:
    """Scrape all configured sources."""
    all_content = []

    print("\n--- Scraping Reddit Sources ---")
    for source in REDDIT_SOURCES:
        print(f"  Scraping r/{source['subreddit']} ({source['sort']})...")
        posts = scrape_reddit(source["subreddit"], source["sort"], source["limit"])
        all_content.extend(posts)
        print(f"    Found {len(posts)} posts")
        time.sleep(2)  # Rate limiting

    print(f"\nTotal scraped: {len(all_content)} posts")
    return all_content


# ============================================================================
# CAPTION REWRITING (via Claude API)
# ============================================================================

def rewrite_caption(original: str, style: str = "meme_page") -> str:
    """Rewrite caption using Claude API to make it unique."""
    if not CLAUDE_API_KEY:
        print("  [WARN] No ANTHROPIC_API_KEY set. Returning original caption.")
        return original

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

        style_prompts = {
            "meme_page": "You are a viral meme page operator. Rewrite this caption to be more engaging, "
                         "funny, and shareable. Keep it under 280 characters. Don't use hashtags. "
                         "Make it feel like a real person wrote it, not AI. Use lowercase casual tone.",
            "tech_humor": "Rewrite this tech meme caption to be funnier and more relatable for developers. "
                          "Keep it under 280 characters. Casual, lowercase, no hashtags.",
            "engagement_bait": "Rewrite this as engagement bait that drives replies and quote tweets. "
                               "Ask a question, make a hot take, or create debate. Under 280 chars.",
        }

        prompt = style_prompts.get(style, style_prompts["meme_page"])

        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": f"{prompt}\n\nOriginal caption: {original}\n\nRewritten caption (just the caption, nothing else):"
            }]
        )
        rewritten = response.content[0].text.strip().strip('"\'')
        return rewritten[:280]

    except Exception as e:
        print(f"  [ERROR] Claude API rewrite failed: {e}")
        return original


def batch_rewrite(conn, limit: int = 10):
    """Rewrite captions for unprocessed scraped content."""
    c = conn.cursor()
    c.execute("""
        SELECT id, original_caption, source_platform FROM content
        WHERE status = 'scraped' AND rewritten_caption IS NULL
        ORDER BY scraped_at DESC LIMIT ?
    """, (limit,))
    rows = c.fetchall()

    if not rows:
        print("No content needs rewriting.")
        return

    print(f"\nRewriting {len(rows)} captions...")
    for row_id, caption, platform in rows:
        print(f"  [{row_id}] Original: {caption[:60]}...")
        rewritten = rewrite_caption(caption)
        print(f"         Rewritten: {rewritten[:60]}...")
        c.execute("UPDATE content SET rewritten_caption = ?, status = 'rewritten' WHERE id = ?",
                  (rewritten, row_id))
        conn.commit()
        time.sleep(1)  # Rate limit API calls


# ============================================================================
# SCHEDULING
# ============================================================================

def generate_posting_schedule(count: int) -> list:
    """Generate natural-looking posting times for today."""
    config = POSTING_CONFIG
    now = datetime.now()
    start_hour = max(config["active_hours_start"], now.hour + 1)
    end_hour = config["active_hours_end"]

    if start_hour >= end_hour:
        print("  Too late in the day to schedule more posts.")
        return []

    available_minutes = (end_hour - start_hour) * 60
    if count > config["max_posts_per_day"]:
        count = config["max_posts_per_day"]

    times = []
    for _ in range(count):
        hour = random.randint(start_hour, end_hour - 1)
        minute = random.randint(0, 59)
        scheduled = now.replace(hour=hour, minute=minute, second=0)
        if scheduled > now:
            times.append(scheduled)

    times.sort()

    # Ensure minimum interval between posts
    filtered = []
    last_time = None
    for t in times:
        if last_time is None or (t - last_time).total_seconds() >= config["min_interval_minutes"] * 60:
            filtered.append(t)
            last_time = t

    return filtered[:count]


def schedule_posts(conn, dry_run: bool = False):
    """Schedule rewritten content for posting."""
    posts_today = get_posts_today(conn)
    remaining = POSTING_CONFIG["max_posts_per_day"] - posts_today
    if remaining <= 0:
        print(f"Already hit max posts today ({posts_today}). No more scheduling.")
        return

    c = conn.cursor()
    c.execute("""
        SELECT id, rewritten_caption, media_type FROM content
        WHERE status = 'rewritten' ORDER BY scraped_at DESC LIMIT ?
    """, (remaining,))
    rows = c.fetchall()

    if not rows:
        print("No rewritten content ready to schedule.")
        return

    schedule = generate_posting_schedule(len(rows))
    if not schedule:
        return

    print(f"\nScheduling {len(schedule)} posts (dry_run={dry_run}):")
    for i, (row_id, caption, media_type) in enumerate(rows[:len(schedule)]):
        scheduled_time = schedule[i] if i < len(schedule) else None
        if scheduled_time:
            print(f"  [{scheduled_time.strftime('%H:%M')}] ({media_type}) {caption[:60]}...")
            if not dry_run:
                c.execute("UPDATE content SET status = 'scheduled', posted_at = ? WHERE id = ?",
                          (scheduled_time.isoformat(), row_id))
                conn.commit()

    # Save schedule CSV
    schedule_file = OUTPUT_DIR / "scheduled" / f"schedule_{datetime.now().strftime('%Y%m%d')}.csv"
    safe_path(schedule_file)
    with open(schedule_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "caption", "media_type", "status"])
        for i, (row_id, caption, media_type) in enumerate(rows[:len(schedule)]):
            if i < len(schedule):
                writer.writerow([schedule[i].strftime("%H:%M"), caption, media_type,
                                 "dry_run" if dry_run else "scheduled"])
    print(f"\nSchedule saved to: {schedule_file}")


# ============================================================================
# WINNER DETECTION
# ============================================================================

def show_winners(conn, limit: int = 10):
    """Show top performing repurposed content."""
    c = conn.cursor()
    c.execute("""
        SELECT rewritten_caption, likes, retweets, replies, impressions,
               engagement_score, posted_at, source_account
        FROM content
        WHERE status IN ('posted', 'winner') AND engagement_score > 0
        ORDER BY engagement_score DESC LIMIT ?
    """, (limit,))
    rows = c.fetchall()

    if not rows:
        print("No engagement data yet. Post some content first, then check --winners after 24h.")
        return

    print(f"\n--- TOP {limit} WINNERS ---")
    for i, (caption, likes, rts, replies, imps, score, posted, source) in enumerate(rows, 1):
        print(f"\n  #{i} (Score: {score:.1f})")
        print(f"  Caption: {caption[:80]}...")
        print(f"  Likes: {likes} | RTs: {rts} | Replies: {replies} | Impressions: {imps}")
        print(f"  Source: {source} | Posted: {posted}")


# ============================================================================
# STATUS
# ============================================================================

def show_status(conn):
    """Show overall repurposer status."""
    c = conn.cursor()

    stats = {}
    for status in ["scraped", "rewritten", "scheduled", "posted", "winner"]:
        c.execute("SELECT COUNT(*) FROM content WHERE status = ?", (status,))
        stats[status] = c.fetchone()[0]

    posts_today = get_posts_today(conn)
    remaining = POSTING_CONFIG["max_posts_per_day"] - posts_today

    print("\n--- CONTENT REPURPOSER STATUS ---")
    print(f"  Scraped (unprocessed): {stats['scraped']}")
    print(f"  Rewritten (ready):     {stats['rewritten']}")
    print(f"  Scheduled:             {stats['scheduled']}")
    print(f"  Posted:                {stats['posted']}")
    print(f"  Winners:               {stats['winner']}")
    print(f"  Posts today:           {posts_today}/{POSTING_CONFIG['max_posts_per_day']}")
    print(f"  Remaining today:       {remaining}")
    print(f"  Database:              {DB_FILE}")
    print(f"  Claude API key:        {'SET' if CLAUDE_API_KEY else 'NOT SET'}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="PRINTMAXX Content Repurposer")
    parser.add_argument("--sources", action="store_true", help="Show configured source accounts")
    parser.add_argument("--scrape", action="store_true", help="Scrape trending content from sources")
    parser.add_argument("--rewrite", action="store_true", help="Rewrite captions via Claude API")
    parser.add_argument("--schedule", action="store_true", help="Schedule posts with natural timing")
    parser.add_argument("--winners", action="store_true", help="Show top performing content")
    parser.add_argument("--status", action="store_true", help="Show system status")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without executing")
    parser.add_argument("--rewrite-limit", type=int, default=10, help="Max captions to rewrite per run")
    parser.add_argument("--winner-limit", type=int, default=10, help="Number of winners to show")
    args = parser.parse_args()

    if not any([args.sources, args.scrape, args.rewrite, args.schedule, args.winners, args.status]):
        parser.print_help()
        return

    conn = init_db()

    if args.sources:
        print("\n--- CONFIGURED SOURCES ---")
        for category, accounts in SOURCE_ACCOUNTS.items():
            print(f"\n  {category}:")
            for acc in accounts:
                print(f"    @{acc}")
        print(f"\n  Reddit:")
        for src in REDDIT_SOURCES:
            print(f"    r/{src['subreddit']} ({src['sort']}, limit={src['limit']})")

    if args.scrape:
        print("\nScraping sources..." + (" (DRY RUN)" if args.dry_run else ""))
        all_content = scrape_all_sources()
        new_count = 0
        for item in all_content:
            if not content_exists(conn, item["source_url"]):
                if not args.dry_run:
                    save_content(conn, item)
                new_count += 1
                print(f"  NEW: {item['original_caption'][:60]}...")
            else:
                print(f"  SKIP (duplicate): {item['original_caption'][:40]}...")
        print(f"\n{new_count} new items {'would be' if args.dry_run else ''} saved to database.")

    if args.rewrite:
        if args.dry_run:
            print("\n[DRY RUN] Would rewrite up to {args.rewrite_limit} captions via Claude API.")
        else:
            batch_rewrite(conn, limit=args.rewrite_limit)

    if args.schedule:
        schedule_posts(conn, dry_run=args.dry_run)

    if args.winners:
        show_winners(conn, limit=args.winner_limit)

    if args.status:
        show_status(conn)

    conn.close()


if __name__ == "__main__":
    main()
