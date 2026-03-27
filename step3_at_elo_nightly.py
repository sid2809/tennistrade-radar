"""
TennisTrade v2 — Step 3 Nightly: Daily Elo Update
==================================================
Runs every night at 11 PM IST after matches complete.
Processes all at_matches where elo_processed = FALSE.

Key decisions:
- No CURRENT_DATE filter — processes full backlog after outages
- Staleness applied at READ TIME in step10, NOT here
- Same adaptive K-factor as step3_at_elo.py
- Updates at_elo_current in place (ON CONFLICT DO UPDATE)

Usage:
    python step3_at_elo_nightly.py          # Process all unprocessed matches
    python step3_at_elo_nightly.py --dry-run # Show what would be processed
    python step3_at_elo_nightly.py --stats   # Show current state
"""

import os
import argparse
from datetime import datetime
from typing import Dict

from db import get_connection, placeholder

# ── Config ────────────────────────────────────────────────────────────────────

SURFACE_MAP = {
    "Hard": "hard", "Clay": "clay", "Grass": "grass",
    "hard": "hard", "clay": "clay", "grass": "grass",
    "Carpet": "hard",
}

GRAND_SLAM_NAMES = {
    "australian open", "french open", "roland garros", "wimbledon", "us open"
}


# ── Elo computation ───────────────────────────────────────────────────────────

def compute_k(match_count: int, is_gs: bool = False) -> float:
    """K = max(16, 250/(n+5)), capped at 48. Grand Slams get 1.5x."""
    k = max(16.0, 250.0 / (match_count + 5))
    if is_gs:
        k *= 1.5
    return min(k, 48.0)


def expected(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))


def is_grand_slam(name: str) -> bool:
    return (name or "").strip().lower() in GRAND_SLAM_NAMES


# ── DB helpers ────────────────────────────────────────────────────────────────

def load_unprocessed_matches(conn) -> list:
    """
    Load all matches where elo_processed = FALSE.
    No CURRENT_DATE filter — handles backlog after Railway outages.
    # Staleness applied at prediction time in step10, not here.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT event_key, event_date, p1_key, p2_key, winner_key,
               surface, tournament_name, tour
        FROM at_matches
        WHERE elo_processed = FALSE
          AND winner_key IS NOT NULL
          AND p1_key IS NOT NULL
          AND p2_key IS NOT NULL
        ORDER BY event_date, event_key
    """)
    return cur.fetchall()


def load_elo_for_players(conn, player_keys: set) -> Dict[int, dict]:
    """Load current Elo for a set of player keys."""
    if not player_keys:
        return {}
    cur = conn.cursor()
    keys_list = list(player_keys)
    placeholders = ",".join(["%s"] * len(keys_list))
    cur.execute(f"""
        SELECT at_player_key, tour,
               elo_overall, elo_hard, elo_clay, elo_grass,
               hard_count, clay_count, grass_count, match_count,
               last_match_date
        FROM at_elo_current
        WHERE at_player_key IN ({placeholders})
    """, keys_list)
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def get_surface_elo(row: dict, surf: str) -> float:
    s = SURFACE_MAP.get(surf, "")
    overall = float(row.get("elo_overall") or 1500.0)
    if s == "hard":  return float(row.get("elo_hard") or overall)
    if s == "clay":  return float(row.get("elo_clay") or overall)
    if s == "grass": return float(row.get("elo_grass") or overall)
    return overall


def update_player_elo(conn, db_type, at_key, row, result, opp_elo,
                      surf, tourney, date):
    """Compute new Elo and write to at_elo_current."""
    ph = placeholder(db_type)
    cur = conn.cursor()

    overall = float(row.get("elo_overall") or 1500.0)
    hard    = float(row.get("elo_hard")    or overall)
    clay    = float(row.get("elo_clay")    or overall)
    grass   = float(row.get("elo_grass")   or overall)
    hard_n  = int(row.get("hard_count")    or 0)
    clay_n  = int(row.get("clay_count")    or 0)
    grass_n = int(row.get("grass_count")   or 0)
    total   = int(row.get("match_count")   or 0)
    tour    = row.get("tour", "unknown")

    k   = compute_k(total, is_grand_slam(tourney))
    exp = expected(get_surface_elo(row, surf), opp_elo)
    d   = k * (result - exp)

    overall += d
    s = SURFACE_MAP.get(surf, "")
    if s == "hard":
        hard   += d; hard_n  += 1
    elif s == "clay":
        clay   += d; clay_n  += 1
    elif s == "grass":
        grass  += d; grass_n += 1
    total += 1

    last_date = date if date else row.get("last_match_date", "")

    cur.execute(f"""
        INSERT INTO at_elo_current
            (at_player_key, sackmann_id, tour,
             elo_overall, elo_hard, elo_clay, elo_grass,
             hard_count, clay_count, grass_count,
             match_count, last_match_date, updated_at)
        VALUES ({",".join([ph]*13)})
        ON CONFLICT (at_player_key) DO UPDATE SET
            elo_overall=EXCLUDED.elo_overall,
            elo_hard=EXCLUDED.elo_hard,
            elo_clay=EXCLUDED.elo_clay,
            elo_grass=EXCLUDED.elo_grass,
            hard_count=EXCLUDED.hard_count,
            clay_count=EXCLUDED.clay_count,
            grass_count=EXCLUDED.grass_count,
            match_count=EXCLUDED.match_count,
            last_match_date=EXCLUDED.last_match_date,
            updated_at=EXCLUDED.updated_at
    """, (
        at_key, row.get("sackmann_id"), tour,
        round(overall, 2), round(hard, 2), round(clay, 2), round(grass, 2),
        hard_n, clay_n, grass_n,
        total, last_date, datetime.utcnow().isoformat()
    ))

    # Return updated row for in-memory use within same batch
    return {
        **row,
        "elo_overall": overall, "elo_hard": hard,
        "elo_clay": clay, "elo_grass": grass,
        "hard_count": hard_n, "clay_count": clay_n, "grass_count": grass_n,
        "match_count": total, "last_match_date": last_date,
    }


def mark_processed(conn, db_type, event_keys: list):
    """Mark matches as elo_processed = TRUE."""
    if not event_keys:
        return
    ph = placeholder(db_type)
    placeholders = ",".join([ph] * len(event_keys))
    conn.cursor().execute(
        f"UPDATE at_matches SET elo_processed = TRUE WHERE event_key IN ({placeholders})",
        event_keys
    )


def show_stats(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM at_matches WHERE elo_processed = FALSE")
    pending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM at_matches WHERE elo_processed = TRUE")
    done = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM at_elo_current")
    players = cur.fetchone()[0]
    cur.execute("""
        SELECT ROUND(AVG(elo_overall)::numeric,0),
               MAX(elo_overall), MIN(elo_overall)
        FROM at_elo_current
    """)
    r = cur.fetchone()
    print(f"  at_matches processed:   {done:,}")
    print(f"  at_matches pending:     {pending:,}")
    print(f"  at_elo_current players: {players:,}")
    print(f"  Elo avg/max/min:        {r[0]:.0f} / {r[1]:.0f} / {r[2]:.0f}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stats",   action="store_true")
    args = parser.parse_args()

    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}", flush=True)
    print(f"[{datetime.utcnow().isoformat()}] Nightly Elo update starting...", flush=True)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    # Load unprocessed matches
    rows = load_unprocessed_matches(conn)
    print(f"  {len(rows):,} unprocessed matches", flush=True)

    if not rows:
        print("  Nothing to process. All matches up to date.", flush=True)
        conn.close()
        return

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {len(rows):,} matches.")
        # Show sample
        print("Sample (first 5):")
        for r in rows[:5]:
            print(f"  event_key={r[0]} date={r[1]} p1={r[2]} p2={r[3]} winner={r[4]}")
        conn.close()
        return

    # Collect all player keys needed
    all_keys = set()
    for row in rows:
        _, _, p1k, p2k, wk, _, _, _ = row
        all_keys.update([p1k, p2k])

    # Load their current Elo
    elo_cache = load_elo_for_players(conn, all_keys)
    print(f"  {len(elo_cache):,} players loaded from at_elo_current", flush=True)

    # Default row for new players
    def default_row(at_key):
        return {
            "at_player_key": at_key, "sackmann_id": None, "tour": "unknown",
            "elo_overall": 1500.0, "elo_hard": 1500.0,
            "elo_clay": 1500.0, "elo_grass": 1500.0,
            "hard_count": 0, "clay_count": 0, "grass_count": 0,
            "match_count": 0, "last_match_date": "",
        }

    processed = 0
    to_mark = []
    batch_size = 500

    for i, row in enumerate(rows):
        ek, date, p1k, p2k, wk, surf, tourney, tour = row
        surf = surf or "Hard"

        p1_row = elo_cache.get(p1k, default_row(p1k))
        p2_row = elo_cache.get(p2k, default_row(p2k))

        p1_surf_elo = get_surface_elo(p1_row, surf)
        p2_surf_elo = get_surface_elo(p2_row, surf)

        if wk == p1k:
            p1_row = update_player_elo(conn, db_type, p1k, p1_row, 1.0,
                                       p2_surf_elo, surf, tourney or "", date or "")
            p2_row = update_player_elo(conn, db_type, p2k, p2_row, 0.0,
                                       p1_surf_elo, surf, tourney or "", date or "")
        else:
            p1_row = update_player_elo(conn, db_type, p1k, p1_row, 0.0,
                                       p2_surf_elo, surf, tourney or "", date or "")
            p2_row = update_player_elo(conn, db_type, p2k, p2_row, 1.0,
                                       p1_surf_elo, surf, tourney or "", date or "")

        elo_cache[p1k] = p1_row
        elo_cache[p2k] = p2_row

        to_mark.append(ek)
        processed += 1

        # Commit in batches
        if len(to_mark) >= batch_size:
            mark_processed(conn, db_type, to_mark)
            to_mark = []
            print(f"  {processed:,}/{len(rows):,} processed...", flush=True)

    # Final batch
    if to_mark:
        mark_processed(conn, db_type, to_mark)

    print(f"\n✓ Nightly Elo update complete", flush=True)
    print(f"  Processed: {processed:,} matches", flush=True)
    print(f"  Players updated: {len(elo_cache):,}", flush=True)
    print(f"  Timestamp: {datetime.utcnow().isoformat()}", flush=True)

    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
