"""
palestine_news.py
Fetches Palestine-focused news from English-language RSS feeds, categorizes
stories, and writes them to docs/palestine_news.json — capped at 20 per
category, max age 7 days, oldest entries replaced first.
No external APIs are used. All sources publish in English.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser
import feedparser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "palestine_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# RSS feeds — all free, English-language, Palestine-focused, no APIs
FEEDS = [
    # WAFA — Official Palestinian News Agency (English service)
    {"source": "WAFA", "url": "https://english.wafa.ps/rss"},
    # Palestine Chronicle — independent non-profit English-language newspaper
    {"source": "Palestine Chronicle", "url": "https://www.palestinechronicle.com/feed/"},
    {"source": "Palestine Chronicle", "url": "https://www.palestinechronicle.com/category/news/feed/"},
    # Al Jazeera — Palestine section (English)
    {"source": "Al Jazeera", "url": "https://www.aljazeera.com/where/palestine/feed"},
    {"source": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    # Middle East Eye — UK-based English outlet, strong Palestine coverage
    {"source": "Middle East Eye", "url": "https://www.middleeasteye.net/rss"},
    {"source": "Middle East Eye", "url": "https://www.middleeasteye.net/tags/palestine/rss"},
    # +972 Magazine — independent English outlet covering Israel-Palestine
    {"source": "+972 Magazine", "url": "https://www.972mag.com/feed/"},
]

# ---------------------------------------------------------------------------
# Category keyword mapping (Palestine-contextualised)
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacy", "diplomatic", "foreign policy", "embassy", "ambassador",
        "treaty", "bilateral", "multilateral", "united nations", "un security",
        "foreign minister", "foreign affairs", "summit", "sanctions",
        "international relations", "geopolitical", "arab league", "oic",
        "trade deal", "g20", "accord", "alliance", "envoy", "consul",
        "two-state", "peace talks", "ceasefire deal", "oslo", "quartet",
        "recognition", "state recognition", "mahmoud abbas", "pa president",
        "palestinian authority", "plo", "hamas political", "negotiations",
        "normalization", "abraham accords", "arab states", "us policy",
        "biden", "trump", "blinken", "un general assembly", "icc",
        "international court", "icj", "un resolution", "veto",
    ],
    "Military": [
        "military", "army", "idf", "israeli forces", "airstrike", "strike",
        "bombing", "shelling", "gunfire", "troops", "soldier", "weapons",
        "missile", "rocket", "armed forces", "war", "combat", "operation",
        "conflict", "explosion", "killed", "wounded", "casualties",
        "hamas", "islamic jihad", "resistance", "armed group", "brigade",
        "battalion", "naval", "tank", "drone", "sniper", "martyr",
        "shahid", "ground operation", "incursion", "raid", "arrest",
        "detention", "checkpoint", "siege", "blockade", "west bank raid",
        "settler violence", "settler attack", "occupation forces",
        "border crossing", "rafah crossing", "kerem shalom",
    ],
    "Energy": [
        "energy", "electricity", "power", "fuel", "gas", "oil",
        "solar", "renewable", "generator", "blackout", "power cut",
        "power outage", "electricity cut", "fuel shortage", "diesel",
        "gaza power plant", "energy crisis", "water supply", "desalination",
        "infrastructure", "reconstruction", "utilities", "aid convoy",
        "humanitarian corridor", "kerem shalom fuel", "power station",
        "climate", "emissions", "environment",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "unemployment",
        "jobs", "poverty", "aid", "humanitarian aid", "funding",
        "budget", "finance", "tax", "investment", "business",
        "trade", "exports", "imports", "wages", "cost of living",
        "world bank", "imf", "donor", "unrwa funding", "sanctions relief",
        "reconstruction fund", "marshall plan", "economic blockade",
        "siege economy", "currency", "shekel", "palestinian economy",
        "west bank economy", "gaza economy", "banking", "financial",
        "remittance", "foreign aid", "us aid", "eu aid",
    ],
    "Local Events": [
        "local", "community", "hospital", "school", "university",
        "clinic", "refugee camp", "camp", "jenin", "nablus", "hebron",
        "ramallah", "bethlehem", "tulkarm", "jericho", "qalqilya",
        "khan younis", "rafah", "deir al-balah", "jabalia", "beit lahiya",
        "gaza city", "north gaza", "southern gaza", "central gaza",
        "west bank", "east jerusalem", "occupied", "settler", "settlement",
        "demolition", "home demolition", "displacement", "evacuation",
        "internally displaced", "protester", "march", "demonstration",
        "civil society", "ngo", "children", "women", "family",
        "famine", "hunger", "starvation", "food insecurity", "malnutrition",
        "health", "medical", "doctor", "nurse", "al-shifa", "al-aqsa hospital",
        "european hospital", "education", "cultural", "heritage",
    ],
}


def classify(title: str, description: str):
    """Return the best-matching category for a story, or None if no match."""
    text = (title + " " + (description or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if re.search(r'\b' + re.escape(kw) + r'\b', text):
                scores[cat] += 1
    best_cat = max(scores, key=scores.get)
    return best_cat if scores[best_cat] > 0 else None


def strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_date(entry):
    """Parse a feed entry's published date into a UTC-aware datetime."""
    raw = entry.get("published") or entry.get("updated") or entry.get("created")
    if not raw:
        struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if struct:
            return datetime(*struct[:6], tzinfo=timezone.utc)
        return None
    try:
        dt = dateparser.parse(raw)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) if dt else None
    except Exception:
        return None


def fetch_feed(feed_cfg: dict) -> list:
    """Fetch a single RSS feed and return a list of story dicts."""
    source = feed_cfg["source"]
    url = feed_cfg["url"]
    stories = []
    try:
        parsed = feedparser.parse(url)
        if parsed.bozo and not parsed.entries:
            log.warning("Bozo feed (%s): %s", source, url)
            return stories
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        for entry in parsed.entries:
            pub_date = parse_date(entry)
            if pub_date and pub_date < cutoff:
                continue
            title = strip_html(entry.get("title", "")).strip()
            desc = strip_html(entry.get("summary", "")).strip()
            if not title:
                continue
            category = classify(title, desc)
            if not category:
                continue
            story = {
                "title": title,
                "source": source,
                "url": entry.get("link", ""),
                "published_date": pub_date.isoformat() if pub_date else None,
                "category": category,
            }
            stories.append(story)
    except Exception as exc:
        log.error("Failed to fetch %s (%s): %s", source, url, exc)
    return stories


def load_existing() -> dict:
    """Load the current JSON file, grouped by category."""
    if not os.path.exists(OUTPUT_FILE):
        return {cat: [] for cat in CATEGORIES}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {cat: [] for cat in CATEGORIES}

    grouped = {cat: [] for cat in CATEGORIES}
    stories = data.get("stories", data) if isinstance(data, dict) else data
    if isinstance(stories, list):
        for story in stories:
            cat = story.get("category")
            if cat in grouped:
                grouped[cat].append(story)
    return grouped


def merge(existing: dict, fresh: list) -> dict:
    """
    Merge fresh stories into the existing pool.
    - De-duplicate by URL.
    - Discard stories older than MAX_AGE_DAYS.
    - Replace oldest entries first when over MAX_PER_CATEGORY.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    existing_urls = set()
    for stories in existing.values():
        for s in stories:
            if s.get("url"):
                existing_urls.add(s["url"])

    for story in fresh:
        cat = story.get("category")
        if cat not in existing:
            continue
        if story["url"] in existing_urls:
            continue
        existing[cat].append(story)
        existing_urls.add(story["url"])

    for cat in CATEGORIES:
        pool = existing[cat]
        # Drop expired stories
        pool = [
            s for s in pool
            if s.get("published_date") and
               dateparser.parse(s["published_date"]).astimezone(timezone.utc) >= cutoff
        ]
        # Sort newest-first, cap at limit (oldest replaced first)
        pool.sort(key=lambda s: s.get("published_date") or "", reverse=True)
        existing[cat] = pool[:MAX_PER_CATEGORY]

    return existing


def write_output(grouped: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    flat = []
    for stories in grouped.values():
        flat.extend(stories)
    output = {
        "country": "Palestine",
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "story_count": len(flat),
        "stories": flat,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)
    log.info("Wrote %d stories to %s", len(flat), OUTPUT_FILE)


def main():
    log.info("Loading existing data ...")
    existing = load_existing()

    log.info("Fetching %d RSS feeds ...", len(FEEDS))
    fresh = []
    for cfg in FEEDS:
        results = fetch_feed(cfg)
        log.info("  %s — %d stories from %s", cfg["source"], len(results), cfg["url"])
        fresh.extend(results)
        time.sleep(0.5)  # polite crawl delay

    log.info("Merging %d fresh stories ...", len(fresh))
    merged = merge(existing, fresh)

    counts = {cat: len(merged[cat]) for cat in CATEGORIES}
    log.info("Category totals: %s", counts)

    write_output(merged)


if __name__ == "__main__":
    main()
