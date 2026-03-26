"""
TennisTrade — Step 10: Daily Pre-Match Scanner
===============================================
Runs every morning (via Railway cron or manually) to:

1. Fetch today's ATP/WTA/Challenger matches from API-Tennis
2. Fetch pre-match odds
3. Look up Elo ratings for all players
4. Find value bets (model prob vs bookmaker implied prob)
5. Auto-log all signals ≥ EDGE_THRESHOLD% as OPEN paper trades
6. Store today's odds in tennis_daily_odds table for the dashboard

Usage:
    python step10_daily_scan.py                  # Scan today
    python step10_daily_scan.py --date 2026-03-27  # Scan specific date
    python step10_daily_scan.py --dry-run         # Print signals, don't save
    python step10_daily_scan.py --stake 1000      # Override default stake
    python step10_daily_scan.py --threshold 20    # Only log ≥20% edge
    python step10_daily_scan.py --report          # Show today's paper trades

Schedule on Railway:
    Add a cron job: 0 2 * * *  (2:30 AM UTC = 8:00 AM IST)
    Command: python step10_daily_scan.py
"""

import os
import sys
import json
import time
import argparse
import unicodedata
import urllib.request
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

from db import get_connection, placeholder

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_STAKE     = 500       # ₹ per trade
EDGE_THRESHOLD    = 15.0      # Minimum % edge to log a paper trade
MAX_TRADES_PER_DAY = 10       # Cap to avoid logging 50 trades in one scan
API_KEY = os.environ.get("API_TENNIS_KEY", "8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b")
BASE_URL = "https://api.api-tennis.com/tennis/"

TOUR_FILTER = {"ATP", "WTA", "Challenger"}  # Skip ITF


# ── API-Tennis helpers ────────────────────────────────────────────────────────

def api_fetch(params: dict, timeout: int = 20) -> dict:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?APIkey={API_KEY}&{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TennisTrade/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  API error: {e}")
        return {}


def detect_tour(event_type: str) -> str:
    t = (event_type or "").lower()
    if "wta" in t: return "WTA"
    if "atp" in t: return "ATP"
    if "challenger" in t: return "Challenger"
    if "itf" in t or "m15" in t or "m25" in t or "w15" in t or "w25" in t: return "ITF"
    return "Other"


def detect_surface(tournament: str) -> str:
    t = tournament.lower()
    clay = ["roland garros", "french open", "madrid", "rome", "monte carlo",
            "barcelona", "buenos aires", "rio", "hamburg", "bastad", "umag",
            "gstaad", "kitzbuhel", "clay"]
    grass = ["wimbledon", "halle", "queen", "eastbourne", "newport",
             "mallorca", "stuttgart grass", "grass"]
    for kw in clay:
        if kw in t: return "Clay"
    for kw in grass:
        if kw in t: return "Grass"
    return "Hard"


def fetch_fixtures(date_str: str) -> List[dict]:
    """Fetch today's fixtures (singles only, ATP/WTA/Challenger)."""
    data = api_fetch({"method": "get_fixtures", "date_start": date_str, "date_stop": date_str})
    if not data.get("success") or not data.get("result"):
        return []

    result = data["result"]
    if isinstance(result, dict):
        result = list(result.values())

    matches = []
    for e in result:
        tour = detect_tour(e.get("event_type_type", ""))
        if tour not in TOUR_FILTER:
            continue
        t = (e.get("event_type_type", "") or "").lower()
        if "double" in t:
            continue

        matches.append({
            "event_key": str(e.get("event_key", "")),
            "player1": e.get("event_first_player", ""),
            "player2": e.get("event_second_player", ""),
            "tournament": e.get("tournament_name", ""),
            "round": e.get("tournament_round", ""),
            "time_utc": e.get("event_time", ""),
            "tour": tour,
            "surface": detect_surface(e.get("tournament_name", "")),
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
            ha = markets.get("Home/Away", {})
            home = ha.get("Home", {})
            away = ha.get("Away", {})
            
            # Priority: Pinnacle > Bet365 > Betfair > first available
            def best_odds(d):
                for k in ["Pncl", "bet365", "Betfair", "1xBet"]:
                    if d.get(k):
                        try: return float(d[k])
                        except: pass
                vals = [v for v in d.values() if v]
                try: return float(vals[0]) if vals else None
                except: return None

            o1 = best_odds(home)
            o2 = best_odds(away)
            if o1 and o2:
                odds_map[str(event_key)] = {"odds_p1": o1, "odds_p2": o2}

    return odds_map


# ── Name matching ─────────────────────────────────────────────────────────────

def normalize_name(raw: str) -> str:
    """Strip accents, lowercase, normalize hyphens."""
    if not raw:
        return ""
    s = raw
    # Strip seeding brackets
    import re
    s = re.sub(r'\s*[\[(][^\])]*[\])]', '', s)
    # NFD normalize to strip accents
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # Comma-flipped names
    m = re.match(r'^([^,]+),\s*(.+)$', s)
    if m:
        s = f"{m.group(2)} {m.group(1)}"
    s = s.replace("-", " ").strip().lower()
    s = re.sub(r'\s+', ' ', s)
    return s


def build_name_index(player_rows: list) -> dict:
    """Build {normalized_name: row} and {last_name: [rows]} index."""
    by_full = {}
    by_last = {}
    for row in player_rows:
        norm = normalize_name(row["player_name"])
        by_full[norm] = row
        parts = norm.split()
        last = parts[-1] if parts else ""
        if last:
            by_last.setdefault(last, []).append((norm, row))
    return {"full": by_full, "last": by_last}


def levenshtein(a: str, b: str) -> int:
    m, n = len(a), len(b)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            temp = dp[j]
            dp[j] = prev if a[i-1] == b[j-1] else 1 + min(dp[j], dp[j-1], prev)
            prev = temp
    return dp[n]


def find_player(name: str, index: dict) -> Tuple[Optional[dict], str]:
    """Return (player_row, confidence) or (None, 'miss')."""
    norm = normalize_name(name)
    if not norm:
        return None, "miss"

    # Exact
    if norm in index["full"]:
        return index["full"][norm], "exact"

    parts = norm.split()
    first = parts[0] if parts else ""
    last  = parts[-1] if parts else ""

    # Last + first initial
    candidates = index["last"].get(last, [])
    initial_matches = [
        (n, r) for n, r in candidates
        if n.split() and n.split()[0][:1] == first[:1]
    ]
    if len(initial_matches) == 1:
        return initial_matches[0][1], "high"
    if len(initial_matches) > 1:
        best = min(initial_matches, key=lambda x: levenshtein(norm, x[0]))
        d = levenshtein(norm, best[0])
        return best[1], "high" if d <= 2 else "medium"

    # Last only (unique)
    if len(candidates) == 1:
        return candidates[0][1], "medium"

    # Levenshtein
    best_row, best_dist = None, 3
    for n, row in index["full"].items():
        d = levenshtein(norm, n)
        if d < best_dist:
            best_dist, best_row = d, row
    if best_row:
        return best_row, "fuzzy"

    return None, "miss"


# ── Elo helpers ───────────────────────────────────────────────────────────────

def elo_win_prob(elo_a: float, elo_b: float) -> float:
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))


def blended_elo(row: dict, surface: str) -> float:
    surf_map = {"Hard": "elo_hard", "Clay": "elo_clay", "Grass": "elo_grass"}
    overall = float(row.get("elo_overall") or 1500)
    surf_col = surf_map.get(surface)
    if surf_col and row.get(surf_col):
        return round((overall + float(row[surf_col])) / 2, 1)
    return overall


# ── DB helpers ────────────────────────────────────────────────────────────────

def ensure_daily_odds_table(conn, db_type: str):
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
            p1_conf      TEXT,
            p2_conf      TEXT,
            PRIMARY KEY (scan_date, event_key)
        )
    """)


def save_daily_odds(conn, db_type: str, scan_date: str, rows: list):
    """Upsert today's enriched match data."""
    if not rows:
        return
    cur = conn.cursor()
    ph = placeholder(db_type)
    for r in rows:
        cur.execute(f"""
            INSERT INTO tennis_daily_odds
            (scan_date, event_key, player1, player2, tournament, tour, surface, round,
             time_utc, p1_elo, p2_elo, model_p1, model_p2,
             odds_p1, odds_p2, implied_p1, edge_p1, edge_p2, p1_conf, p2_conf)
            VALUES ({','.join([ph]*20)})
            ON CONFLICT (scan_date, event_key) DO UPDATE SET
                odds_p1=EXCLUDED.odds_p1, odds_p2=EXCLUDED.odds_p2,
                model_p1=EXCLUDED.model_p1, edge_p1=EXCLUDED.edge_p1,
                edge_p2=EXCLUDED.edge_p2
        """, (
            scan_date, r["event_key"], r["player1"], r["player2"],
            r["tournament"], r["tour"], r["surface"], r["round"],
            r["time_utc"], r["p1_elo"], r["p2_elo"],
            r["model_p1"], r["model_p2"],
            r["odds_p1"], r["odds_p2"], r["implied_p1"],
            r["edge_p1"], r["edge_p2"], r["p1_conf"], r["p2_conf"],
        ))


def log_paper_trade(conn, db_type: str, trade: dict):
    """Insert a paper trade into paper_trades table."""
    cur = conn.cursor()
    ph = placeholder(db_type)
    cur.execute(f"""
        INSERT INTO paper_trades
        (trade_id, strategy, player1, player2, tournament, surface, tour,
         entry_side, entry_player, entry_odds, entry_stake, entry_liability,
         entry_time, entry_reason, status, confidence)
        VALUES ({','.join([ph]*16)})
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
    ))


def count_open_trades_today(conn, db_type: str, scan_date: str) -> int:
    ph = placeholder(db_type)
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM paper_trades WHERE status='OPEN' AND entry_time::date = {ph}",
        (scan_date,)
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_scan(conn, db_type: str, scan_date: str, stake: float,
             threshold: float, dry_run: bool, verbose: bool) -> dict:

    print(f"\n{'='*60}")
    print(f"TennisTrade — Daily Pre-Match Scan")
    print(f"{'='*60}")
    print(f"  Date:      {scan_date}")
    print(f"  Threshold: ≥{threshold}% edge")
    print(f"  Stake:     ₹{stake:,.0f}")
    print(f"  Dry run:   {dry_run}")
    print()

    # 1. Load player index from DB
    print("Loading player Elo index...", end="", flush=True)
    t0 = time.time()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.player_id,
               (p.first_name || ' ' || p.last_name) AS player_name,
               e.elo_overall, e.elo_hard, e.elo_clay, e.elo_grass
        FROM tennis_players p
        JOIN tennis_elo_current e ON e.player_id = p.player_id
        WHERE e.elo_overall IS NOT NULL
    """)
    cols = [d[0] for d in cur.description]
    player_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    name_index = build_name_index(player_rows)
    print(f" {len(player_rows):,} players ({time.time()-t0:.1f}s)")

    # 2. Fetch fixtures
    print("Fetching fixtures...", end="", flush=True)
    fixtures = fetch_fixtures(scan_date)
    print(f" {len(fixtures)} matches")

    if not fixtures:
        print("  No matches found. Exiting.")
        return {"matches": 0, "value_bets": 0, "logged": 0}

    # 3. Fetch odds
    print("Fetching odds...", end="", flush=True)
    odds_map = fetch_odds(scan_date)
    print(f" {len(odds_map)} matches with odds")

    # 4. Enrich + compute edge
    print("\nEnriching matches:")
    enriched = []
    for m in fixtures:
        ek = m["event_key"]
        p1_row, p1_conf = find_player(m["player1"], name_index)
        p2_row, p2_conf = find_player(m["player2"], name_index)

        p1_elo = blended_elo(p1_row, m["surface"]) if p1_row else 1500.0
        p2_elo = blended_elo(p2_row, m["surface"]) if p2_row else 1500.0

        model_p1 = elo_win_prob(p1_elo, p2_elo)
        model_p2 = 1 - model_p1

        odds = odds_map.get(ek, {})
        odds_p1 = odds.get("odds_p1")
        odds_p2 = odds.get("odds_p2")
        implied_p1 = (1 / odds_p1) if odds_p1 else None
        implied_p2 = (1 / odds_p2) if odds_p2 else None
        edge_p1 = round((model_p1 - implied_p1) * 100, 1) if implied_p1 else None
        edge_p2 = round((model_p2 - implied_p2) * 100, 1) if implied_p2 else None

        row = {
            **m,
            "p1_elo": p1_elo, "p2_elo": p2_elo,
            "p1_conf": p1_conf, "p2_conf": p2_conf,
            "model_p1": round(model_p1 * 100, 1),
            "model_p2": round(model_p2 * 100, 1),
            "odds_p1": odds_p1, "odds_p2": odds_p2,
            "implied_p1": round(implied_p1 * 100, 1) if implied_p1 else None,
            "edge_p1": edge_p1, "edge_p2": edge_p2,
        }
        enriched.append(row)

        if verbose:
            e1 = f"+{edge_p1:.1f}%" if edge_p1 else "—"
            e2 = f"+{edge_p2:.1f}%" if edge_p2 else "—"
            print(f"  {m['player1'][:20]:<20} ({p1_elo:.0f},{p1_conf[:1]}) vs "
                  f"{m['player2'][:20]:<20} ({p2_elo:.0f},{p2_conf[:1]}) "
                  f"| edge: {e1}/{e2}")

    # 5. Save daily odds to DB
    if not dry_run:
        ensure_daily_odds_table(conn, db_type)
        save_daily_odds(conn, db_type, scan_date, enriched)
        print(f"\n✓ Saved {len(enriched)} matches to tennis_daily_odds")

    # 6. Find value bets
    value_bets = []
    for r in enriched:
        # Skip unrated players
        if r["p1_conf"] == "miss" or r["p2_conf"] == "miss":
            continue
        if r["p1_elo"] == 1500.0 and r["p2_elo"] == 1500.0:
            continue

        e1 = r["edge_p1"] or 0
        e2 = r["edge_p2"] or 0
        best_edge = max(e1, e2)

        if best_edge >= threshold:
            bet_on  = r["player1"] if e1 > e2 else r["player2"]
            bet_odds = r["odds_p1"] if e1 > e2 else r["odds_p2"]
            model_prob = r["model_p1"] if e1 > e2 else r["model_p2"]

            value_bets.append({
                **r,
                "bet_on": bet_on,
                "bet_odds": bet_odds,
                "edge": best_edge,
                "model_prob": model_prob,
                "stake": stake,
            })

    # Sort by edge desc
    value_bets.sort(key=lambda x: x["edge"], reverse=True)

    print(f"\n{'─'*60}")
    print(f"Value bets found: {len(value_bets)}")
    for v in value_bets:
        print(f"  [{v['tour']:10}] {v['player1']} vs {v['player2']}")
        print(f"           Bet: {v['bet_on']} @ {v['bet_odds']:.2f} | "
              f"Edge: +{v['edge']:.1f}% | Model: {v['model_prob']:.1f}%")

    # 7. Log paper trades
    logged = 0
    if not dry_run and value_bets:
        # Check how many already logged today
        already = count_open_trades_today(conn, db_type, scan_date)
        remaining = MAX_TRADES_PER_DAY - already
        to_log = value_bets[:remaining]

        print(f"\nLogging paper trades ({already} already open today, cap={MAX_TRADES_PER_DAY}):")
        for v in to_log:
            trade_id = f"AUTO_{scan_date.replace('-','')}_{v['event_key']}"
            entry_time = datetime.utcnow().isoformat()
            trade = {**v, "trade_id": trade_id, "entry_time": entry_time, "strategy": "T6"}
            try:
                log_paper_trade(conn, db_type, trade)
                logged += 1
                print(f"  ✓ Logged: {v['bet_on']} @ {v['bet_odds']:.2f} "
                      f"(edge +{v['edge']:.1f}%, ₹{stake:,.0f})")
            except Exception as e:
                print(f"  ✗ Failed to log {v['bet_on']}: {e}")

        print(f"\n✓ {logged} paper trades logged")
    elif dry_run:
        print("\n[DRY RUN] No trades logged.")

    return {
        "matches": len(fixtures),
        "with_odds": len(odds_map),
        "value_bets": len(value_bets),
        "logged": logged,
    }


def show_report(conn, db_type: str, scan_date: str):
    """Show today's paper trades."""
    cur = conn.cursor()
    ph = placeholder(db_type)
    cur.execute(f"""
        SELECT trade_id, player1, player2, entry_player, entry_odds,
               entry_stake, confidence, status, pnl, entry_time
        FROM paper_trades
        WHERE entry_time::date = {ph}
        ORDER BY entry_time DESC
    """, (scan_date,))
    rows = cur.fetchall()

    if not rows:
        print(f"No paper trades for {scan_date}")
        return

    print(f"\nPaper trades for {scan_date} ({len(rows)} total):")
    print(f"{'ID':<25} {'Match':<35} {'Bet':<20} {'Odds':>5} {'Edge':>6} {'Status':>6} {'P&L':>8}")
    print("─" * 110)
    total_pnl = 0
    for r in rows:
        tid, p1, p2, bet_on, odds, stake, edge, status, pnl, etime = r
        match = f"{p1} vs {p2}"[:34]
        pnl_str = f"₹{pnl:+,.0f}" if pnl is not None else "open"
        if pnl:
            total_pnl += pnl
        print(f"{tid:<25} {match:<35} {bet_on:<20} {odds:>5.2f} "
              f"{edge:>5.1f}% {status:>6} {pnl_str:>8}")
    print("─" * 110)
    print(f"{'Total P&L':>95} ₹{total_pnl:+,.0f}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisTrade Daily Scanner")
    parser.add_argument("--date", type=str,
                        default=date.today().isoformat(),
                        help="Date to scan (YYYY-MM-DD, default: today)")
    parser.add_argument("--stake", type=float, default=DEFAULT_STAKE,
                        help=f"Stake per trade in ₹ (default: {DEFAULT_STAKE})")
    parser.add_argument("--threshold", type=float, default=EDGE_THRESHOLD,
                        help=f"Minimum edge %% (default: {EDGE_THRESHOLD})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print signals without saving")
    parser.add_argument("--verbose", action="store_true",
                        help="Print all match enrichments")
    parser.add_argument("--report", action="store_true",
                        help="Show today's paper trades and exit")
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

    print(f"\nSummary: {result['matches']} matches | "
          f"{result['with_odds']} with odds | "
          f"{result['value_bets']} value bets | "
          f"{result['logged']} logged")

    conn.close()


if __name__ == "__main__":
    main()
