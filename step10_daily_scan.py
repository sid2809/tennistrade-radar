"""
TennisTrade v2 — Step 10: Daily Pre-Match Scanner
=================================================
Runs every morning (via Railway cron or manually) to:

1. Fetch today's ATP/WTA/Challenger matches from API-Tennis
2. Fetch pre-match odds
3. Look up v2 Elo ratings by AT player key (no name matching)
4. Apply overround normalization, staleness discount, adaptive surface blend
5. Find value bets (model prob vs normalized implied prob)
6. Auto-log all signals as OPEN paper trades with model_version + event_key

Key v2 changes vs v1:
- Reads at_elo_current (not tennis_elo_current)
- Player lookup by AT key (not fuzzy name matching)
- Overround normalization: implied = raw / sum(raw)
- Staleness discount: Elo regresses toward tour mean after inactivity
- Adaptive surface blend: weight = min(surface_matches/50, 1.0)
- Unrated player filter replaces ITF tour filter
- paper_trades gets event_key + model_version for auto-settlement

Usage:
    python step10_daily_scan.py                  # Scan today
    python step10_daily_scan.py --date 2026-03-27
    python step10_daily_scan.py --dry-run
    python step10_daily_scan.py --report
"""

import os
import json
import time
import argparse
import urllib.request
from datetime import datetime, date
from typing import Dict, List, Tuple

from db import get_connection, placeholder

# ── Config ────────────────────────────────────────────────────────────────────

MODEL_VERSION     = "v2_elo_hybrid"
DEFAULT_STAKE     = 500
EDGE_THRESHOLD    = 15.0
MAX_TRADES_PER_DAY = 10
API_KEY  = os.environ.get("API_TENNIS_KEY", "8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b")
BASE_URL = "https://api.api-tennis.com/tennis/"

TOUR_FILTER = {"Atp Singles", "Wta Singles", "Challenger Men Singles", "Challenger Women Singles"}

# Staleness: Elo regresses toward tour mean when player is inactive
# Applied at READ TIME here — NOT stored in at_elo_current
TOUR_MEAN_ELO = {
    "atp":               1580.0,
    "wta":               1560.0,
    "challenger_men":    1500.0,
    "challenger_women":  1480.0,
    "unknown":           1500.0,
}
STALENESS_HALF_LIFE_DAYS = 180  # after 180 days inactive, Elo halfway to tour mean

SURFACE_RAMP = 50  # matches needed for full surface Elo weight


# ── API helpers ───────────────────────────────────────────────────────────────

def api_fetch(params: dict, timeout: int = 30) -> dict:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?APIkey={API_KEY}&{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TennisTrade/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"  API error: {e}")
        return {}


def fetch_fixtures(date_str: str) -> List[dict]:
    """Fetch today's singles fixtures with AT player keys."""
    data = api_fetch({"method": "get_fixtures", "date_start": date_str, "date_stop": date_str})
    if not data.get("success") or not data.get("result"):
        return []

    result = data["result"]
    if isinstance(result, dict):
        result = list(result.values())

    matches = []
    for e in result:
        tour = e.get("event_type_type", "")
        if tour not in TOUR_FILTER:
            continue
        if "double" in tour.lower():
            continue
        # Skip if no player keys
        p1k = e.get("first_player_key")
        p2k = e.get("second_player_key")
        if not p1k or not p2k:
            continue

        matches.append({
            "event_key":   str(e.get("event_key", "")),
            "player1":     e.get("event_first_player", ""),
            "player2":     e.get("event_second_player", ""),
            "p1_at_key":   int(p1k),
            "p2_at_key":   int(p2k),
            "tournament":  e.get("tournament_name", ""),
            "round":       e.get("tournament_round", ""),
            "time_utc":    e.get("event_time", ""),
            "tour":        tour,
            "surface":     detect_surface(e.get("tournament_name", "")),
        })

    return matches


def fetch_odds(date_str: str) -> Dict[str, dict]:
    """Fetch pre-match odds keyed by event_key."""
    data = api_fetch({"method": "get_odds", "date_start": date_str, "date_stop": date_str})
    if not data.get("success") or not data.get("result"):
        return {}

    result = data["result"]
    odds_map = {}

    if isinstance(result, dict):
        for event_key, markets in result.items():
            if not isinstance(markets, dict):
                continue
            ha   = markets.get("Home/Away", {})
            home = ha.get("Home", {})
            away = ha.get("Away", {})

            def best(d):
                for k in ["Pncl", "bet365", "Betfair", "1xBet"]:
                    if d.get(k):
                        try: return float(d[k])
                        except: pass
                vals = [v for v in d.values() if v]
                try: return float(vals[0]) if vals else None
                except: return None

            o1, o2 = best(home), best(away)
            if o1 and o2:
                odds_map[str(event_key)] = {"odds_p1": o1, "odds_p2": o2}

    return odds_map


def detect_surface(tournament: str) -> str:
    t = tournament.lower()
    for kw in ["roland garros", "french open", "madrid", "rome", "monte carlo",
               "barcelona", "buenos aires", "rio", "hamburg", "clay"]:
        if kw in t: return "Clay"
    for kw in ["wimbledon", "halle", "queen", "eastbourne", "newport",
               "mallorca", "grass"]:
        if kw in t: return "Grass"
    return "Hard"


# ── Elo helpers ───────────────────────────────────────────────────────────────

def apply_staleness(elo: float, tour: str, days_inactive: int) -> float:
    """
    Staleness discount: applied at READ TIME (not stored in DB).
    Elo regresses toward tour mean as player is inactive.
    # Staleness applied at prediction time in step10, not in step3_at_elo_nightly
    """
    if days_inactive <= 14:
        return elo
    mean = TOUR_MEAN_ELO.get(tour, 1500.0)
    decay = 2 ** (-days_inactive / STALENESS_HALF_LIFE_DAYS)
    return elo * decay + mean * (1 - decay)


def adaptive_surface_elo(row: dict, surface: str) -> float:
    """
    Adaptive blend: weight = min(surface_matches/50, 1.0)
    0 surface matches → 100% overall
    50+ matches       → 100% surface
    """
    overall = float(row.get("elo_overall") or 1500.0)
    surf_map = {"Hard": ("elo_hard", "hard_count"),
                "Clay": ("elo_clay", "clay_count"),
                "Grass": ("elo_grass", "grass_count")}
    if surface not in surf_map:
        return overall
    elo_col, count_col = surf_map[surface]
    surf_elo = float(row.get(elo_col) or overall)
    surf_n   = int(row.get(count_col) or 0)
    weight   = min(surf_n / SURFACE_RAMP, 1.0)
    return weight * surf_elo + (1 - weight) * overall


def normalize_implied(odds_p1: float, odds_p2: float) -> Tuple[float, float, float]:
    """
    Remove bookmaker overround.
    Returns (implied_p1, implied_p2, overround)
    """
    raw_p1 = 1.0 / odds_p1
    raw_p2 = 1.0 / odds_p2
    overround = raw_p1 + raw_p2
    return raw_p1 / overround, raw_p2 / overround, overround


def elo_win_prob(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))


def days_since(date_str: str) -> int:
    """Days since last_match_date."""
    if not date_str:
        return 999
    try:
        last = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.utcnow() - last).days
    except:
        return 999


def edge_threshold_for_tour(tour: str) -> float:
    if "Challenger" in tour:
        return 20.0
    return 15.0


# ── DB helpers ────────────────────────────────────────────────────────────────

def load_elo_index(conn) -> Dict[int, dict]:
    """Load at_elo_current keyed by at_player_key."""
    cur = conn.cursor()
    cur.execute("""
        SELECT at_player_key, tour,
               elo_overall, elo_hard, elo_clay, elo_grass,
               hard_count, clay_count, grass_count,
               match_count, last_match_date
        FROM at_elo_current
    """)
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def ensure_daily_odds_table(conn, db_type):
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS tennis_daily_odds (
            scan_date    TEXT NOT NULL,
            event_key    TEXT NOT NULL,
            player1      TEXT,
            player2      TEXT,
            tournament   TEXT,
            tour         TEXT,
            surface      TEXT,
            round        TEXT,
            time_utc     TEXT,
            p1_elo       REAL,
            p2_elo       REAL,
            model_p1     REAL,
            model_p2     REAL,
            odds_p1      REAL,
            odds_p2      REAL,
            implied_p1   REAL,
            edge_p1      REAL,
            edge_p2      REAL,
            overround    REAL,
            p1_conf      TEXT,
            p2_conf      TEXT,
            PRIMARY KEY (scan_date, event_key)
        )
    """)
    # Add overround column if table existed before v2
    try:
        conn.cursor().execute("ALTER TABLE tennis_daily_odds ADD COLUMN overround REAL")
    except Exception:
        pass  # already exists


def save_daily_odds(conn, db_type, scan_date, rows):
    if not rows:
        return
    ph = placeholder(db_type)
    cur = conn.cursor()
    for r in rows:
        cur.execute(f"""
            INSERT INTO tennis_daily_odds
            (scan_date, event_key, player1, player2, tournament, tour, surface, round,
             time_utc, p1_elo, p2_elo, model_p1, model_p2,
             odds_p1, odds_p2, implied_p1, edge_p1, edge_p2, overround, p1_conf, p2_conf)
            VALUES ({",".join([ph]*21)})
            ON CONFLICT (scan_date, event_key) DO UPDATE SET
                odds_p1=EXCLUDED.odds_p1, odds_p2=EXCLUDED.odds_p2,
                model_p1=EXCLUDED.model_p1, edge_p1=EXCLUDED.edge_p1,
                edge_p2=EXCLUDED.edge_p2, overround=EXCLUDED.overround
        """, (
            scan_date, r["event_key"], r["player1"], r["player2"],
            r["tournament"], r["tour"], r["surface"], r["round"],
            r["time_utc"], r["p1_elo"], r["p2_elo"],
            r["model_p1"], r["model_p2"],
            r["odds_p1"], r["odds_p2"], r.get("implied_p1"),
            r["edge_p1"], r["edge_p2"], r.get("overround"),
            r["p1_conf"], r["p2_conf"],
        ))


def ensure_paper_trades_columns(conn, db_type):
    """Add v2 columns to paper_trades if missing."""
    cur = conn.cursor()
    for col, defn in [
        ("event_key",    "TEXT"),
        ("settled_at",   "TIMESTAMP"),
        ("settled_by",   "TEXT DEFAULT 'manual'"),
        ("model_version","TEXT DEFAULT 'v1_elo_sackmann'"),
    ]:
        try:
            cur.execute(f"ALTER TABLE paper_trades ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists


def log_paper_trade(conn, db_type, trade):
    ph = placeholder(db_type)
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO paper_trades
        (trade_id, strategy, player1, player2, tournament, surface, tour,
         entry_side, entry_player, entry_odds, entry_stake, entry_liability,
         entry_time, entry_reason, status, confidence, event_key, model_version)
        VALUES ({",".join([ph]*18)})
        ON CONFLICT (trade_id) DO NOTHING
    """, (
        trade["trade_id"], trade["strategy"],
        trade["player1"], trade["player2"],
        trade["tournament"], trade["surface"], trade["tour"],
        "BACK", trade["bet_on"],
        trade["bet_odds"], trade["stake"], trade["stake"],
        trade["entry_time"],
        f"Auto | Edge: {trade['edge']:.1f}% | Model: {trade['model_prob']:.1f}%",
        "OPEN", trade["edge"],
        trade["event_key"], MODEL_VERSION,
    ))


def count_open_trades_today(conn, db_type, scan_date) -> int:
    ph = placeholder(db_type)
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM paper_trades WHERE status='OPEN' AND entry_time::date = {ph}",
        (scan_date,)
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_scan(conn, db_type, scan_date, stake, threshold, dry_run, verbose):
    print(f"\n{'='*60}")
    print(f"TennisTrade v2 — Daily Pre-Match Scan")
    print(f"{'='*60}")
    print(f"  Date:          {scan_date}")
    print(f"  Model:         {MODEL_VERSION}")
    print(f"  Threshold:     ≥{threshold}% edge")
    print(f"  Stake:         ₹{stake:,.0f}")
    print(f"  Dry run:       {dry_run}")
    print()

    # 1. Load v2 Elo index (by AT player key — no name matching needed)
    print("Loading v2 Elo index (at_elo_current)...", end="", flush=True)
    t0 = time.time()
    elo_index = load_elo_index(conn)
    print(f" {len(elo_index):,} players ({time.time()-t0:.1f}s)")

    if not elo_index:
        print("  ERROR: at_elo_current is empty. Run step3_at_elo.py first.")
        return {"matches": 0, "value_bets": 0, "logged": 0}

    # 2. Fetch fixtures
    print("Fetching fixtures...", end="", flush=True)
    fixtures = fetch_fixtures(scan_date)
    print(f" {len(fixtures)} matches")

    if not fixtures:
        print("  No matches found.")
        return {"matches": 0, "with_odds": 0, "value_bets": 0, "logged": 0}

    # 3. Fetch odds
    print("Fetching odds...", end="", flush=True)
    odds_map = fetch_odds(scan_date)
    print(f" {len(odds_map)} matches with odds")

    # 4. Enrich matches
    print("\nEnriching matches:")
    enriched = []
    unrated_skipped = 0

    for m in fixtures:
        ek  = m["event_key"]
        p1k = m["p1_at_key"]
        p2k = m["p2_at_key"]

        p1_row = elo_index.get(p1k)
        p2_row = elo_index.get(p2k)

        # Unrated player filter — drop if either player not in at_elo_current
        if not p1_row or not p2_row:
            unrated_skipped += 1
            if verbose:
                miss = m["player1"] if not p1_row else m["player2"]
                print(f"  SKIP (unrated) {m['player1']} vs {m['player2']} — {miss} not in index")
            continue

        # Staleness discount — applied at read time
        surf = m["surface"]
        tour = p1_row.get("tour", "unknown")

        p1_days = days_since(p1_row.get("last_match_date", ""))
        p2_days = days_since(p2_row.get("last_match_date", ""))

        p1_raw_elo = adaptive_surface_elo(p1_row, surf)
        p2_raw_elo = adaptive_surface_elo(p2_row, surf)

        p1_elo = apply_staleness(p1_raw_elo, p1_row.get("tour", "unknown"), p1_days)
        p2_elo = apply_staleness(p2_raw_elo, p2_row.get("tour", "unknown"), p2_days)

        model_p1 = elo_win_prob(p1_elo, p2_elo)
        model_p2 = 1.0 - model_p1

        # Odds + overround normalization
        odds = odds_map.get(ek, {})
        odds_p1 = odds.get("odds_p1")
        odds_p2 = odds.get("odds_p2")

        edge_p1 = edge_p2 = implied_p1 = implied_p2 = overround = None

        if odds_p1 and odds_p2:
            implied_p1, implied_p2, overround = normalize_implied(odds_p1, odds_p2)
            edge_p1 = round((model_p1 - implied_p1) * 100, 1)
            edge_p2 = round((model_p2 - implied_p2) * 100, 1)

            # Flag suspicious overround
            if overround > 1.15 and verbose:
                print(f"  WARN high overround {overround:.3f}: {m['player1']} vs {m['player2']}")

        p1_conf = "rated" if p1_row else "miss"
        p2_conf = "rated" if p2_row else "miss"

        row = {
            **m,
            "p1_elo": round(p1_elo, 1), "p2_elo": round(p2_elo, 1),
            "p1_conf": p1_conf, "p2_conf": p2_conf,
            "p1_count": p1_row.get("match_count", 0),
            "p2_count": p2_row.get("match_count", 0),
            "model_p1": round(model_p1 * 100, 1),
            "model_p2": round(model_p2 * 100, 1),
            "odds_p1": odds_p1, "odds_p2": odds_p2,
            "implied_p1": round(implied_p1 * 100, 1) if implied_p1 else None,
            "implied_p2": round(implied_p2 * 100, 1) if implied_p2 else None,
            "edge_p1": edge_p1, "edge_p2": edge_p2,
            "overround": round(overround, 4) if overround else None,
            "p1_days_inactive": p1_days,
            "p2_days_inactive": p2_days,
        }
        enriched.append(row)

        if verbose:
            e1 = f"+{edge_p1:.1f}%" if edge_p1 else "—"
            e2 = f"+{edge_p2:.1f}%" if edge_p2 else "—"
            stale1 = f" [{p1_days}d]" if p1_days > 30 else ""
            stale2 = f" [{p2_days}d]" if p2_days > 30 else ""
            print(f"  {m['player1'][:20]:<20}({p1_elo:.0f}{stale1}) vs "
                  f"{m['player2'][:20]:<20}({p2_elo:.0f}{stale2}) "
                  f"| edge: {e1}/{e2}")

    if unrated_skipped:
        print(f"  Skipped {unrated_skipped} matches with unrated players")

    # 5. Save daily odds
    if not dry_run:
        ensure_daily_odds_table(conn, db_type)
        save_daily_odds(conn, db_type, scan_date, enriched)
        print(f"\n✓ Saved {len(enriched)} matches to tennis_daily_odds")

    # 6. Find value bets
    value_bets = []
    for r in enriched:
        if not r["edge_p1"] and not r["edge_p2"]:
            continue  # no odds

        e1 = r["edge_p1"] or 0
        e2 = r["edge_p2"] or 0
        best_edge = max(e1, e2)
        effective_threshold = max(threshold, edge_threshold_for_tour(r["tour"]))

        if best_edge >= effective_threshold:
            bet_on    = r["player1"] if e1 > e2 else r["player2"]
            bet_odds  = r["odds_p1"] if e1 > e2 else r["odds_p2"]
            model_prob = r["model_p1"] if e1 > e2 else r["model_p2"]
            value_bets.append({
                **r,
                "bet_on": bet_on, "bet_odds": bet_odds,
                "edge": best_edge, "model_prob": model_prob,
                "stake": stake,
            })

    value_bets.sort(key=lambda x: x["edge"], reverse=True)

    print(f"\n{'─'*60}")
    print(f"Value bets found: {len(value_bets)}")
    for v in value_bets:
        stale_note = ""
        if v["p1_days_inactive"] > 90 or v["p2_days_inactive"] > 90:
            stale_note = " ⚠ inactive player"
        over_note = f" [overround {v['overround']:.3f}]" if v.get("overround") and v["overround"] > 1.10 else ""
        print(f"  [{v['tour'][:15]:15}] {v['player1']} vs {v['player2']}")
        print(f"           Bet: {v['bet_on']} @ {v['bet_odds']:.2f} | "
              f"Edge: +{v['edge']:.1f}% | Model: {v['model_prob']:.1f}%"
              f"{stale_note}{over_note}")

    # 7. Log paper trades
    logged = 0
    if not dry_run and value_bets:
        ensure_paper_trades_columns(conn, db_type)
        already = count_open_trades_today(conn, db_type, scan_date)
        remaining = MAX_TRADES_PER_DAY - already
        to_log = value_bets[:remaining]

        print(f"\nLogging paper trades ({already} already open today, cap={MAX_TRADES_PER_DAY}):")
        for v in to_log:
            trade_id = f"AUTO_{scan_date.replace('-','')}_{v['event_key']}"
            trade = {
                **v,
                "trade_id": trade_id,
                "entry_time": datetime.utcnow().isoformat(),
                "strategy": "T6",
            }
            try:
                log_paper_trade(conn, db_type, trade)
                logged += 1
                print(f"  ✓ {v['bet_on']} @ {v['bet_odds']:.2f} "
                      f"(edge +{v['edge']:.1f}%, ₹{stake:,.0f})")
            except Exception as e:
                print(f"  ✗ Failed: {e}")

        print(f"\n✓ {logged} paper trades logged ({MODEL_VERSION})")
    elif dry_run:
        print("\n[DRY RUN] No trades logged.")

    with_odds = sum(1 for r in enriched if r.get("odds_p1"))
    return {
        "matches": len(fixtures),
        "enriched": len(enriched),
        "with_odds": with_odds,
        "value_bets": len(value_bets),
        "logged": logged,
    }


def show_report(conn, db_type, scan_date):
    cur = conn.cursor()
    ph = placeholder(db_type)
    cur.execute(f"""
        SELECT trade_id, player1, player2, entry_player, entry_odds,
               entry_stake, confidence, status, pnl, model_version, entry_time
        FROM paper_trades
        WHERE entry_time::date = {ph}
        ORDER BY entry_time DESC
    """, (scan_date,))
    rows = cur.fetchall()
    if not rows:
        print(f"No paper trades for {scan_date}")
        return
    print(f"\nPaper trades for {scan_date} ({len(rows)} total):")
    print(f"{'Match':<35} {'Bet':<20} {'Odds':>5} {'Edge':>6} {'Model':>10} {'Status':>6} {'P&L':>8}")
    print("─" * 100)
    total_pnl = 0
    for r in rows:
        tid, p1, p2, bet_on, odds, stake, edge, status, pnl, mv, etime = r
        match = f"{p1} vs {p2}"[:34]
        pnl_str = f"₹{pnl:+,.0f}" if pnl is not None else "open"
        if pnl: total_pnl += pnl
        print(f"{match:<35} {bet_on:<20} {odds:>5.2f} "
              f"{(edge or 0):>5.1f}% {(mv or '?'):>10} {status:>6} {pnl_str:>8}")
    print("─" * 100)
    print(f"{'Total P&L':>92} ₹{total_pnl:+,.0f}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",      default=date.today().isoformat())
    parser.add_argument("--stake",     type=float, default=DEFAULT_STAKE)
    parser.add_argument("--threshold", type=float, default=EDGE_THRESHOLD)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--verbose",   action="store_true")
    parser.add_argument("--report",    action="store_true")
    args = parser.parse_args()

    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}")

    if args.report:
        show_report(conn, db_type, args.date)
        conn.close()
        return

    result = run_scan(
        conn, db_type,
        scan_date=args.date,
        stake=args.stake,
        threshold=args.threshold,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    print(f"\nSummary: {result['matches']} fixtures | "
          f"{result.get('enriched',0)} rated | "
          f"{result['with_odds']} with odds | "
          f"{result['value_bets']} value bets | "
          f"{result['logged']} logged")

    conn.close()


if __name__ == "__main__":
    main()
