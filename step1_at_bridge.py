"""
TennisTrade v2 — Step 1: Build API-Tennis ↔ Sackmann Player Bridge
===================================================================
Pulls all unique player keys from API-Tennis fixtures across all
ATP/WTA/Challenger tournaments, fetches full name + birthday for each
player via get_players endpoint, then matches to Sackmann player IDs
using:
  1. Full name exact match
  2. Birthday + last name match (fallback)
  3. Last name + first initial (fallback)

Stores results in at_player_bridge table.
Exports unmatched/uncertain players to CSV for manual review.

Usage:
    python step1_at_bridge.py                 # Full run
    python step1_at_bridge.py --dry-run       # Preview without saving
    python step1_at_bridge.py --export-csv    # Export manual review CSV
    python step1_at_bridge.py --stats         # Show bridge table stats
    python step1_at_bridge.py --resume        # Skip already-processed keys

Tables created:
    at_player_bridge (at_player_key, at_name, at_full_name, at_bday,
                      at_country, sackmann_id, confidence, verified)
"""

import os
import re
import sys
import csv
import time
import argparse
import unicodedata
import urllib.request
import json
from datetime import date
from typing import Optional, Tuple, List, Dict

from db import get_connection, placeholder


# ── Config ────────────────────────────────────────────────────────────────────

API_KEY  = os.environ.get("API_TENNIS_KEY", "8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b")
BASE_URL = "https://api.api-tennis.com/tennis/"

KEEP_TYPES = {"Atp Singles", "Wta Singles", "Challenger Men Singles", "Challenger Women Singles"}

SKIP_TOURNAMENTS = {
    "ATP Davis Cup - Group III", "ATP Davis Cup - Group IV", "ATP Davis Cup - Group V",
    "ATP Davis Cup - World Group", "ATP Davis Cup - World Group I", "ATP Davis Cup - World Group II",
    "ATP ATP Cup", "ATP Hopman Cup", "ATP Laver Cup", "ATP United Cup",
    "Olympic Games", "Asian Games", "Next Gen Finals - Jeddah", "Grand Slam Cup", "Zhuhai"
}

API_DELAY = 0.25  # seconds between API calls


# ── API helpers ───────────────────────────────────────────────────────────────

def api_fetch(params: dict, timeout: int = 15) -> dict:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?APIkey={API_KEY}&{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TennisTrade/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"error": str(e)}


# ── Name helpers ──────────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Normalize name for matching — strip accents, lowercase, normalize spaces."""
    if not s:
        return ""
    s = re.sub(r'\s*[\[(][^\])]*[\])]', '', s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("-", " ").replace("'", "").strip().lower()
    return re.sub(r'\s+', ' ', s)


def convert_bday(at_bday: str) -> Optional[str]:
    """Convert AT birthday DD.MM.YYYY → YYYYMMDD (Sackmann format).
    Returns None for placeholder date 01.01.1970 (AT uses this when unknown).
    """
    if not at_bday or "." not in at_bday:
        return None
    if at_bday.strip() == "01.01.1970":
        return None  # AT placeholder for unknown birthday
    parts = at_bday.strip().split(".")
    if len(parts) != 3:
        return None
    day, month, year = parts
    if len(year) != 4:
        return None
    return f"{year}{month.zfill(2)}{day.zfill(2)}"


def split_full_name(full_name: str) -> Tuple[str, str]:
    """Split 'Carlos Alcaraz' → ('Carlos', 'Alcaraz')."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


def extract_last_from_abbreviated(name: str) -> str:
    """Extract last name from 'C. Alcaraz' → 'Alcaraz'."""
    if "." in name:
        parts = name.split(".")
        return parts[-1].strip().split()[-1] if parts[-1].strip() else ""
    return name.strip().split()[-1] if name.strip() else ""


# ── Sackmann matching ─────────────────────────────────────────────────────────

def match_by_full_name(conn, db_type: str, full_name: str) -> Tuple[Optional[int], str]:
    """Try exact full name match against Sackmann."""
    if not full_name:
        return None, "miss"
    ph = placeholder(db_type)
    cur = conn.cursor()
    first, last = split_full_name(full_name)
    nf, nl = normalize(first), normalize(last)

    # Exact normalized match
    cur.execute(f"""
        SELECT player_id FROM tennis_players
        WHERE LOWER(REPLACE(last_name, '-', ' ')) = {ph}
          AND LOWER(REPLACE(first_name, '-', ' ')) = {ph}
        LIMIT 1
    """, (nl, nf))
    row = cur.fetchone()
    if row:
        return row[0], "exact"

    # Last + first initial
    if nf:
        cur.execute(f"""
            SELECT player_id, first_name FROM tennis_players
            WHERE LOWER(REPLACE(last_name, '-', ' ')) = {ph}
              AND LOWER(LEFT(first_name, 1)) = {ph}
            LIMIT 5
        """, (nl, nf[0]))
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], "high"
        if len(rows) > 1:
            # Pick closest first name length
            best = min(rows, key=lambda x: abs(len(normalize(x[1])) - len(nf)))
            return best[0], "medium"

    return None, "miss"


def match_by_birthday(conn, db_type: str, sack_bday: str, at_name: str) -> Tuple[Optional[int], str]:
    """Match via birthday + every word in name as potential last name.
    Handles South American reversed names like 'Pablo Varillas Juan'.
    """
    if not sack_bday:
        return None, "miss"
    ph = placeholder(db_type)
    cur = conn.cursor()

    # Try every word in full name as potential last name
    # This handles reversed South American names e.g. "Pablo Varillas Juan" -> Varillas
    words = [w for w in at_name.split() if len(w) > 2 and not w.endswith(".")]
    
    matched_id = None
    for word in words:
        nl = normalize(word)
        cur.execute(f"""
            SELECT player_id FROM tennis_players
            WHERE birth_date = {ph}
              AND LOWER(REPLACE(last_name, '-', ' ')) = {ph}
            LIMIT 2
        """, (sack_bday, nl))
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], "exact"  # birthday + last name = very reliable
        if len(rows) > 1:
            matched_id = rows[0][0]  # ambiguous — keep trying other words

    if matched_id:
        return matched_id, "medium"

    return None, "miss"


def find_sackmann_match(conn, db_type: str, at_player: dict) -> Tuple[Optional[int], str]:
    """
    Full matching pipeline for one AT player.
    Priority: full_name exact → birthday + last → full_name initial → miss
    """
    full_name = at_player.get("player_full_name") or ""
    at_name   = at_player.get("player_name") or ""
    at_bday   = at_player.get("player_bday") or ""
    sack_bday = convert_bday(at_bday)

    # 1. Full name exact match (most reliable when available)
    if full_name:
        pid, conf = match_by_full_name(conn, db_type, full_name)
        if conf in ("exact", "high"):
            return pid, conf

    # 2. Birthday + last name (very reliable)
    if sack_bday:
        pid, conf = match_by_birthday(conn, db_type, sack_bday, full_name or at_name)
        if conf in ("exact", "high"):
            return pid, conf

    # 3. Full name medium confidence
    if full_name:
        pid, conf = match_by_full_name(conn, db_type, full_name)
        if pid:
            return pid, conf

    # 4. Birthday medium
    if sack_bday:
        pid, conf = match_by_birthday(conn, db_type, sack_bday, full_name or at_name)
        if pid:
            return pid, conf

    return None, "miss"


# ── DB setup ──────────────────────────────────────────────────────────────────

def ensure_bridge_table(conn, db_type: str):
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS at_player_bridge (
            at_player_key  INTEGER PRIMARY KEY,
            at_name        TEXT,
            at_full_name   TEXT,
            at_bday        TEXT,
            at_country     TEXT,
            sackmann_id    INTEGER,
            confidence     TEXT,
            verified       BOOLEAN DEFAULT FALSE,
            created_at     TEXT
        )
    """)


def get_existing_keys(conn, db_type: str) -> set:
    """Get all AT player keys already in bridge table."""
    cur = conn.cursor()
    cur.execute("SELECT at_player_key FROM at_player_bridge")
    return {row[0] for row in cur.fetchall()}


def insert_bridge(conn, db_type: str, player: dict, sackmann_id: Optional[int], confidence: str):
    ph = placeholder(db_type)
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO at_player_bridge
            (at_player_key, at_name, at_full_name, at_bday, at_country,
             sackmann_id, confidence, verified, created_at)
        VALUES ({",".join([ph]*9)})
        ON CONFLICT (at_player_key) DO UPDATE SET
            at_full_name=EXCLUDED.at_full_name,
            at_bday=EXCLUDED.at_bday,
            sackmann_id=EXCLUDED.sackmann_id,
            confidence=EXCLUDED.confidence
    """, (
        player["player_key"],
        player.get("player_name"),
        player.get("player_full_name"),
        player.get("player_bday"),
        player.get("player_country"),
        sackmann_id,
        confidence,
        False,
        date.today().isoformat()
    ))


# ── Main pipeline ─────────────────────────────────────────────────────────────

def collect_player_keys(test=False) -> Dict[int, str]:
    """
    Step 1: Get all unique player keys from all relevant tournaments.
    Returns {player_key: abbreviated_name}
    """
    print("Fetching tournament list...", end="", flush=True)
    data = api_fetch({"method": "get_tournaments"})
    tournaments = data.get("result", [])
    if isinstance(tournaments, dict):
        tournaments = list(tournaments.values())

    relevant = [
        t for t in tournaments
        if t.get("event_type_type") in KEEP_TYPES
        and t.get("tournament_name") not in SKIP_TOURNAMENTS
    ]
    if test:
        relevant = relevant[:5]
        print("[TEST MODE] Limited to 5 tournaments")
    print(f" {len(relevant)} tournaments")

    player_keys = {}  # key → name
    errors = 0

    for i, t in enumerate(relevant):
        if i % 100 == 0:
            print(f"  Collecting players: {i}/{len(relevant)} tournaments, "
                  f"{len(player_keys)} unique players so far", end="\r", flush=True)

        data = api_fetch({"method": "get_fixtures", "tournament_key": t["tournament_key"]})
        matches = data.get("result", [])
        if isinstance(matches, dict):
            matches = list(matches.values())

        for m in matches:
            pk1 = m.get("first_player_key")
            pk2 = m.get("second_player_key")
            n1  = m.get("event_first_player", "")
            n2  = m.get("event_second_player", "")
            if pk1 and pk1 not in player_keys:
                player_keys[int(pk1)] = n1
            if pk2 and pk2 not in player_keys:
                player_keys[int(pk2)] = n2

        time.sleep(API_DELAY)

    print(f"\n  Total unique player keys: {len(player_keys)}")
    return player_keys


def fetch_player_profiles(player_keys: Dict[int, str]) -> List[dict]:
    """
    Step 2: Fetch full profile (full name + birthday) for each player key.
    """
    profiles = []
    total = len(player_keys)
    found = missing = 0

    print(f"Fetching player profiles (0/{total})...", end="\r", flush=True)

    for i, (pk, abbrev_name) in enumerate(player_keys.items()):
        if i % 50 == 0:
            print(f"  Fetching profiles: {i}/{total} | found:{found} missing:{missing}",
                  end="\r", flush=True)

        data = api_fetch({"method": "get_players", "player_key": str(pk)})
        result = data.get("result", [])

        if result and isinstance(result, list):
            p = result[0]
            profiles.append(p)
            found += 1
        else:
            # Profile not found — create minimal entry from match data
            profiles.append({
                "player_key": pk,
                "player_name": abbrev_name,
                "player_full_name": None,
                "player_bday": None,
                "player_country": None,
            })
            missing += 1

        time.sleep(API_DELAY)

    print(f"\n  Profiles found: {found} | Missing: {missing}")
    return profiles


def run_bridge(conn, db_type: str, profiles: List[dict],
               dry_run: bool, resume_keys: set) -> dict:
    """
    Step 3: Match each AT player to Sackmann and store bridge.
    """
    stats = {"exact": 0, "high": 0, "medium": 0, "miss": 0, "skipped": 0}
    manual_review = []  # players needing manual review

    total = len(profiles)
    for i, p in enumerate(profiles):
        pk = p.get("player_key")
        if not pk:
            continue

        if pk in resume_keys:
            stats["skipped"] += 1
            continue

        if i % 100 == 0:
            print(f"  Matching: {i}/{total} | "
                  f"exact:{stats['exact']} high:{stats['high']} "
                  f"medium:{stats['medium']} miss:{stats['miss']}",
                  end="\r", flush=True)

        sackmann_id, confidence = find_sackmann_match(conn, db_type, p)

        stats[confidence] = stats.get(confidence, 0) + 1

        if confidence in ("medium", "miss"):
            manual_review.append({
                "at_player_key": pk,
                "at_name": p.get("player_name"),
                "at_full_name": p.get("player_full_name"),
                "at_bday": p.get("player_bday"),
                "at_country": p.get("player_country"),
                "sackmann_id": sackmann_id,
                "confidence": confidence,
            })

        if not dry_run:
            insert_bridge(conn, db_type, p, sackmann_id, confidence)

    print(f"\n  Bridge complete: {total} players processed")
    return stats, manual_review


def export_manual_review(manual_review: list, filepath: str = "manual_review.csv"):
    """Export uncertain matches to CSV for manual review."""
    if not manual_review:
        print("No manual review needed.")
        return

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "at_player_key", "at_name", "at_full_name", "at_bday",
            "at_country", "sackmann_id", "confidence"
        ])
        writer.writeheader()
        writer.writerows(manual_review)
    print(f"Manual review CSV exported: {filepath} ({len(manual_review)} players)")


def show_stats(conn, db_type: str):
    """Show current bridge table statistics."""
    cur = conn.cursor()
    cur.execute("""
        SELECT confidence, COUNT(*) as cnt
        FROM at_player_bridge
        GROUP BY confidence
        ORDER BY cnt DESC
    """)
    rows = cur.fetchall()
    print("\nBridge table stats:")
    total = 0
    for conf, cnt in rows:
        pct = cnt / max(sum(r[1] for r in rows), 1) * 100
        print(f"  {conf:10} {cnt:5} ({pct:.1f}%)")
        total += cnt
    print(f"  {'TOTAL':10} {total:5}")

    cur.execute("""
        SELECT COUNT(*) FROM at_player_bridge
        WHERE sackmann_id IS NOT NULL
    """)
    matched = cur.fetchone()[0]
    print(f"\n  Matched to Sackmann: {matched}/{total} ({matched/max(total,1)*100:.1f}%)")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisTrade v2 — Build AT Player Bridge")
    parser.add_argument("--dry-run",    action="store_true", help="Preview without saving")
    parser.add_argument("--export-csv", action="store_true", help="Export manual review CSV")
    parser.add_argument("--stats",      action="store_true", help="Show bridge table stats")
    parser.add_argument("--resume",     action="store_true", help="Skip already-processed keys")
    parser.add_argument("--test",       action="store_true", help="Test on 5 tournaments only")
    args = parser.parse_args()

    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}")

    if args.stats:
        show_stats(conn, db_type)
        conn.close()
        return

    if not args.dry_run:
        ensure_bridge_table(conn, db_type)

    resume_keys = set()
    if args.resume:
        resume_keys = get_existing_keys(conn, db_type)
        print(f"Resuming — skipping {len(resume_keys)} already-processed keys")

    # Step 1: Collect all unique player keys
    print("\n── Step 1: Collect player keys ──────────────────────────")
    player_keys = collect_player_keys(test=args.test)

    # Step 2: Fetch player profiles
    print("\n── Step 2: Fetch player profiles ────────────────────────")
    profiles = fetch_player_profiles(player_keys)

    # Step 3: Match to Sackmann
    print("\n── Step 3: Match to Sackmann ────────────────────────────")
    stats, manual_review = run_bridge(conn, db_type, profiles, args.dry_run, resume_keys)

    # Summary
    total = sum(v for k, v in stats.items() if k != "skipped")
    auto_matched = stats["exact"] + stats["high"]
    print(f"\n{'='*60}")
    print(f"Results:")
    print(f"  Exact match:    {stats['exact']:5} ({stats['exact']/max(total,1)*100:.1f}%)")
    print(f"  High conf:      {stats['high']:5} ({stats['high']/max(total,1)*100:.1f}%)")
    print(f"  Medium conf:    {stats['medium']:5} — needs manual review")
    print(f"  Not found:      {stats['miss']:5} — new players (post-Sackmann)")
    print(f"  Skipped:        {stats['skipped']:5}")
    print(f"  Auto-matched:   {auto_matched}/{total} ({auto_matched/max(total,1)*100:.1f}%)")

    if args.export_csv or manual_review:
        export_manual_review(manual_review)

    if args.dry_run:
        print("\n[DRY RUN] Nothing saved to DB.")
    else:
        print(f"\n✓ at_player_bridge table updated")

    conn.close()


if __name__ == "__main__":
    main()
