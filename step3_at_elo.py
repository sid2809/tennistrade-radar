"""
TennisTrade v2 — Step 3: Compute Hybrid Elo Ratings
=====================================================
Computes Elo ratings using hybrid dataset:
  Phase 1: ALL Sackmann matches (2000-2020) — keyed by sackmann_id
  Phase 2: AT matches from 2021 onwards — keyed by at_player_key
  Bridge:  Sackmann Elo carried forward for players in at_player_bridge

Uses adaptive K-factor: K = max(16, 250 / (matches_played + 5))
Grand Slam: 1.5x K multiplier.
Surface Elo tracked with match counts for adaptive blending in step10.

Key decisions (from master plan):
- Phase 1 processes ALL Sackmann matches (not just bridged players)
- Sackmann→AT handoff: bridged players carry their Sackmann Elo into Phase 2
- New AT players start at calibrated tour debut Elo (not 1500)
- Staleness discount applied at READ TIME in step10, NOT here
- AT matches filtered to 2021+ to avoid double-counting Sackmann overlap
- Nightly updates via step3_at_elo_nightly.py (separate script)

Usage:
    python step3_at_elo.py              # Full recompute
    python step3_at_elo.py --dry-run    # Show stats without writing
    python step3_at_elo.py --stats      # Show current at_elo_current counts
"""

import os
import argparse
from datetime import datetime
from typing import Dict

from db import get_connection, placeholder

# ── Config ────────────────────────────────────────────────────────────────────

# Calibrated starting Elo by tour (not 1500 default)
TOUR_DEBUT_ELO = {
    "atp":               1540.0,
    "wta":               1530.0,
    "challenger_men":    1500.0,
    "challenger_women":  1480.0,
    "unknown":           1500.0,
}

SURFACE_MAP = {
    "Hard": "hard", "Clay": "clay", "Grass": "grass",
    "hard": "hard", "clay": "clay", "grass": "grass",
    "Carpet": "hard",  # treat carpet as hard
}

# Sackmann stores tour as "M" (men/ATP) or "W" (women/WTA) in tennis_players
# Map to our internal tour keys for calibrated debut Elo lookup
SACKMANN_TOUR_MAP = {
    "m": "atp", "w": "wta",
    "atp": "atp", "wta": "wta",
    "f": "challenger_men",  # futures
}

GRAND_SLAM_NAMES = {
    "australian open", "french open", "roland garros", "wimbledon", "us open"
}

# AT matches from 2021 onwards (avoid double-counting Sackmann overlap 2021-2024)
AT_START_DATE = "2021-01-01"


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


# ── Player Elo state ──────────────────────────────────────────────────────────

class PlayerElo:
    def __init__(self, debut: float = 1500.0):
        self.overall = debut
        self.hard    = debut
        self.clay    = debut
        self.grass   = debut
        self.total   = 0
        self.hard_n  = 0
        self.clay_n  = 0
        self.grass_n = 0
        self.last_date = ""

    def surface_elo(self, surf: str) -> float:
        s = SURFACE_MAP.get(surf, "")
        if s == "hard":  return self.hard
        if s == "clay":  return self.clay
        if s == "grass": return self.grass
        return self.overall

    def update(self, result: float, opp_elo: float, surf: str,
               tourney: str, date: str):
        k   = compute_k(self.total, is_grand_slam(tourney))
        exp = expected(self.surface_elo(surf), opp_elo)
        d   = k * (result - exp)
        self.overall += d
        s = SURFACE_MAP.get(surf, "")
        if s == "hard":
            self.hard   += d; self.hard_n  += 1
        elif s == "clay":
            self.clay   += d; self.clay_n  += 1
        elif s == "grass":
            self.grass  += d; self.grass_n += 1
        self.total += 1
        if date and date > self.last_date:
            self.last_date = date


# ── DB helpers ────────────────────────────────────────────────────────────────

def ensure_table(conn, db_type):
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS at_elo_current (
            at_player_key   INT PRIMARY KEY,
            sackmann_id     INT,
            tour            TEXT,
            elo_overall     REAL,
            elo_hard        REAL,
            elo_clay        REAL,
            elo_grass       REAL,
            hard_count      INT DEFAULT 0,
            clay_count      INT DEFAULT 0,
            grass_count     INT DEFAULT 0,
            match_count     INT DEFAULT 0,
            last_match_date TEXT,
            updated_at      TEXT
        )
    """)


def load_bridge(conn):
    """Returns {at_player_key: {sackmann_id, name, tour}}"""
    cur = conn.cursor()
    cur.execute("""
        SELECT at_player_key, sackmann_id,
               COALESCE(at_full_name, at_name) as name
        FROM at_player_bridge
    """)
    bridge = {}
    for row in cur.fetchall():
        bridge[int(row[0])] = {"sackmann_id": row[1], "name": row[2]}
    return bridge


def load_sackmann_players(conn):
    """Returns {sackmann_id: tour_str}"""
    cur = conn.cursor()
    cur.execute("SELECT player_id, tour FROM tennis_players")
    return {row[0]: (row[1] or "").lower() for row in cur.fetchall()}


def load_sackmann_matches(conn):
    """Returns sorted list of Sackmann matches up to 2020-12-31."""
    cur = conn.cursor()
    # Filter to pre-2021 to avoid overlap with AT matches
    cur.execute("""
        SELECT tourney_date, winner_id, loser_id, surface, tourney_name
        FROM tennis_matches
        WHERE winner_id IS NOT NULL
          AND loser_id IS NOT NULL
          AND tourney_date < 20210101
        ORDER BY tourney_date, match_id
    """)
    return cur.fetchall()


def load_at_matches(conn):
    """Returns sorted list of AT matches from 2021 onwards."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT event_date, p1_key, p2_key, winner_key,
               surface, tournament_name, tour
        FROM at_matches
        WHERE winner_key IS NOT NULL
          AND p1_key IS NOT NULL
          AND p2_key IS NOT NULL
          AND event_date >= '{AT_START_DATE}'
        ORDER BY event_date, event_key
    """)
    return cur.fetchall()


def save_ratings(conn, db_type, ratings, bridge, at_tour_map, dry_run):
    if dry_run:
        return
    ph  = placeholder(db_type)
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    ok  = 0
    for at_key, elo in ratings.items():
        sack_id = bridge.get(at_key, {}).get("sackmann_id")
        tour    = at_tour_map.get(at_key, "unknown")
        try:
            cur.execute(f"""
                INSERT INTO at_elo_current
                    (at_player_key, sackmann_id, tour,
                     elo_overall, elo_hard, elo_clay, elo_grass,
                     hard_count, clay_count, grass_count,
                     match_count, last_match_date, updated_at)
                VALUES ({",".join([ph]*13)})
                ON CONFLICT (at_player_key) DO UPDATE SET
                    sackmann_id=EXCLUDED.sackmann_id,
                    tour=EXCLUDED.tour,
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
            """, (at_key, sack_id, tour,
                  round(elo.overall,2), round(elo.hard,2),
                  round(elo.clay,2),    round(elo.grass,2),
                  elo.hard_n, elo.clay_n, elo.grass_n,
                  elo.total,  elo.last_date, now))
            ok += 1
        except Exception as e:
            print(f"  save error at_key={at_key}: {e}", flush=True)
    print(f"  Saved {ok:,} ratings to at_elo_current", flush=True)


def show_stats(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM at_elo_current")
        print(f"  at_elo_current: {cur.fetchone()[0]:,} players")
        cur.execute("""
            SELECT tour, COUNT(*), ROUND(AVG(elo_overall)::numeric,0)
            FROM at_elo_current GROUP BY tour ORDER BY COUNT(*) DESC
        """)
        print("By tour:")
        for r in cur.fetchall():
            print(f"  {(r[0] or 'unknown'):25} {r[1]:6} players | avg Elo: {r[2]}")
        cur.execute("""
            SELECT MIN(elo_overall), MAX(elo_overall),
                   ROUND(AVG(elo_overall)::numeric,0)
            FROM at_elo_current
        """)
        r = cur.fetchone()
        print(f"Elo range: {r[0]:.0f} min | {r[2]:.0f} avg | {r[1]:.0f} max")
    except Exception as e:
        print(f"  Error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stats",   action="store_true")
    args = parser.parse_args()

    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}", flush=True)

    if args.stats:
        show_stats(conn); conn.close(); return

    if not args.dry_run:
        ensure_table(conn, db_type)

    # ── Load bridge & player info ───────────────────────────────────────────────
    print("Loading bridge and player data...", flush=True)
    bridge         = load_bridge(conn)            # {at_key: {sackmann_id, name}}
    sack_players   = load_sackmann_players(conn)  # {sackmann_id: tour}

    # Reverse bridge: sackmann_id → at_player_key
    sack_to_at = {info["sackmann_id"]: at_key
                  for at_key, info in bridge.items()
                  if info.get("sackmann_id")}

    print(f"  {len(bridge):,} AT players | {len(sack_to_at):,} bridged to Sackmann",
          flush=True)

    # ── Phase 1: Sackmann (2000-2020) ─────────────────────────────────────────
    print("\nPhase 1: Sackmann matches (2000-2020)...", flush=True)
    sack_rows = load_sackmann_matches(conn)
    print(f"  {len(sack_rows):,} matches", flush=True)

    # Phase 1 uses sackmann_id as key (separate dict)
    sack_elo: Dict[int, PlayerElo] = {}

    def get_sack(sid: int) -> PlayerElo:
        if sid not in sack_elo:
            raw_tour = sack_players.get(sid, "")
            tour = SACKMANN_TOUR_MAP.get(raw_tour, "unknown")
            sack_elo[sid] = PlayerElo(TOUR_DEBUT_ELO.get(tour, 1500.0))
        return sack_elo[sid]

    processed = 0
    for i, row in enumerate(sack_rows):
        date_int, w_sid, l_sid, surf, tourney = row
        date_str = str(date_int)
        if len(date_str) == 8:
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

        w = get_sack(w_sid)
        l = get_sack(l_sid)
        w_se = w.surface_elo(surf or "Hard")
        l_se = l.surface_elo(surf or "Hard")
        w.update(1.0, l_se, surf or "Hard", tourney or "", date_str)
        l.update(0.0, w_se, surf or "Hard", tourney or "", date_str)
        processed += 1

        if (i+1) % 50000 == 0:
            print(f"  {i+1:,}/{len(sack_rows):,} | {len(sack_elo):,} players",
                  flush=True)

    print(f"  Done: {processed:,} processed | {len(sack_elo):,} players rated",
          flush=True)

    # ── Phase 2: AT matches (2021+) ───────────────────────────────────────────
    print("\nPhase 2: AT matches (2021+)...", flush=True)
    at_rows = load_at_matches(conn)
    print(f"  {len(at_rows):,} matches (from {AT_START_DATE})", flush=True)

    # Phase 2 ratings keyed by at_player_key
    at_ratings:  Dict[int, PlayerElo] = {}
    at_tour_map: Dict[int, str]       = {}

    def get_at(at_key: int, tour_str: str) -> PlayerElo:
        if at_key not in at_ratings:
            # Check if this player has Sackmann history via bridge
            sack_id = bridge.get(at_key, {}).get("sackmann_id")
            if sack_id and sack_id in sack_elo:
                # Carry forward Sackmann Elo
                src = sack_elo[sack_id]
                p   = PlayerElo(src.overall)
                p.hard    = src.hard
                p.clay    = src.clay
                p.grass   = src.grass
                p.hard_n  = src.hard_n
                p.clay_n  = src.clay_n
                p.grass_n = src.grass_n
                p.total   = src.total
                p.last_date = src.last_date
            else:
                # New player — calibrated debut Elo
                debut = TOUR_DEBUT_ELO.get(tour_str, 1500.0)
                p = PlayerElo(debut)
            at_ratings[at_key]  = p
            at_tour_map[at_key] = tour_str
        return at_ratings[at_key]

    processed = skipped = 0
    for i, row in enumerate(at_rows):
        date, p1k, p2k, wk, surf, tourney, tour = row
        if not p1k or not p2k or not wk:
            skipped += 1; continue

        tour_str = "unknown"
        t = (tour or "").lower()
        if "wta" in t:        tour_str = "wta"
        elif "atp" in t:      tour_str = "atp"
        elif "women" in t:    tour_str = "challenger_women"
        elif "challenger" in t: tour_str = "challenger_men"

        p1 = get_at(p1k, tour_str)
        p2 = get_at(p2k, tour_str)
        p1s = p1.surface_elo(surf or "Hard")
        p2s = p2.surface_elo(surf or "Hard")

        if wk == p1k:
            p1.update(1.0, p2s, surf or "Hard", tourney or "", date or "")
            p2.update(0.0, p1s, surf or "Hard", tourney or "", date or "")
        else:
            p1.update(0.0, p2s, surf or "Hard", tourney or "", date or "")
            p2.update(1.0, p1s, surf or "Hard", tourney or "", date or "")
        processed += 1

        if (i+1) % 20000 == 0:
            print(f"  {i+1:,}/{len(at_rows):,} | {len(at_ratings):,} players",
                  flush=True)

    print(f"  Done: {processed:,} processed | {skipped:,} skipped | "
          f"{len(at_ratings):,} players rated", flush=True)

    # ── Summary ────────────────────────────────────────────────────────────────
    elos = [r.overall for r in at_ratings.values()]
    if elos:
        bridged_count = sum(1 for ak in at_ratings
                            if bridge.get(ak, {}).get("sackmann_id") and
                            bridge[ak]["sackmann_id"] in sack_elo)
        print(f"\nSummary:")
        print(f"  Total AT players rated:   {len(elos):,}")
        print(f"  With Sackmann history:    {bridged_count:,}")
        print(f"  New players (AT-only):    {len(elos)-bridged_count:,}")
        print(f"  Elo min/avg/max:          "
              f"{min(elos):.0f} / {sum(elos)/len(elos):.0f} / {max(elos):.0f}")

    top10 = sorted(at_ratings.items(), key=lambda x: x[1].overall, reverse=True)[:10]
    print("\nTop 10 by Elo:")
    for ak, elo in top10:
        name = bridge.get(ak, {}).get("name", f"AT:{ak}")
        print(f"  {(name or '?'):35} {elo.overall:.0f} ({elo.total} matches)")

    # ── Save ───────────────────────────────────────────────────────────────────
    print(f"\nSaving {len(at_ratings):,} ratings...", flush=True)
    save_ratings(conn, db_type, at_ratings, bridge, at_tour_map, args.dry_run)

    if args.dry_run:
        print("\n[DRY RUN] Nothing saved.")
    else:
        print("\n✓ at_elo_current populated")
        show_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
