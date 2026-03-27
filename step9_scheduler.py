"""
TennisTrade v2 — Scheduler
===========================
Runs alongside step9_wire.py as a separate process.
Handles scheduled tasks:

1. Daily scan at 8 AM IST (2:30 AM UTC) — step10_daily_scan.py
2. Nightly Elo at 11 PM IST (5:30 PM UTC) — step3_at_elo_nightly.py
3. Auto-settlement every 60s — checks completed matches, settles open trades

This is intentionally a separate process from step9_wire.py so that:
- step9 can be restarted independently
- Scheduler failures don't kill the live radar
- Each task logs independently

Usage:
    python step9_scheduler.py          # Run scheduler (blocking)
    python step9_scheduler.py --test   # Run all tasks once immediately and exit
"""

import os
import sys
import time
import subprocess
import argparse
import urllib.request
import json
from datetime import datetime, timezone, timedelta
from typing import List

from db import get_connection, placeholder

API_KEY  = os.environ.get("API_TENNIS_KEY", "8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b")
BASE_URL = "https://api.api-tennis.com/tennis/"

# IST = UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

# Task run times (IST hour, minute)
DAILY_SCAN_TIME  = (8,  0)   # 8:00 AM IST
NIGHTLY_ELO_TIME = (23, 0)   # 11:00 PM IST

# Settlement check interval
SETTLE_INTERVAL_SECS = 60


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"[{now}] {msg}", flush=True)


# ── Task runner ───────────────────────────────────────────────────────────────

def run_task(script: str, args: list = []):
    """Run a python script as subprocess, stream output."""
    cmd = [sys.executable, script] + args
    log(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=False, timeout=600)
        if result.returncode == 0:
            log(f"✓ {script} completed successfully")
        else:
            log(f"✗ {script} exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        log(f"✗ {script} timed out after 10 minutes")
    except Exception as e:
        log(f"✗ {script} failed: {e}")


# ── Auto-settlement ───────────────────────────────────────────────────────────

def api_fetch(params: dict) -> dict:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?APIkey={API_KEY}&{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TennisTrade/2.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        log(f"API error: {e}")
        return {}


def get_open_trades(conn, db_type) -> list:
    """Get all OPEN paper trades with event_key."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT trade_id, event_key, entry_player, entry_odds, entry_stake
            FROM paper_trades
            WHERE status = 'OPEN'
              AND event_key IS NOT NULL
        """)
        return cur.fetchall()
    except Exception as e:
        log(f"Error loading open trades: {e}")
        return []


def get_connection_safe():
    """Get DB connection, reconnecting if needed."""
    try:
        conn, db_type = get_connection()
        return conn, db_type
    except Exception as e:
        log(f"DB connection failed: {e}")
        return None, None


def check_and_settle(conn, db_type):
    """
    Check open paper trades against today's completed fixtures.
    Uses get_fixtures (not get_livescore) because finished matches
    disappear from livescore immediately but stay in fixtures.
    """
    trades = get_open_trades(conn, db_type)
    if not trades:
        return

    # Use today's date in IST
    today = datetime.now(IST).strftime("%Y-%m-%d")

    # Fetch today's fixtures (includes Finished matches)
    data = api_fetch({"method": "get_fixtures",
                      "date_start": today, "date_stop": today})
    result = data.get("result", {})
    if isinstance(result, dict):
        result = list(result.values())

    # Build map of event_key → finished match info
    finished_map = {}
    for e in (result or []):
        if e.get("event_status") != "Finished":
            continue
        ek = str(e.get("event_key", ""))
        if ek:
            finished_map[ek] = {
                "winner": e.get("event_winner", ""),
                "p1": e.get("event_first_player", ""),
                "p2": e.get("event_second_player", ""),
            }

    if not finished_map:
        return  # No finished matches yet today

    ph = placeholder(db_type)
    cur = conn.cursor()
    settled = 0

    for trade in trades:
        trade_id, event_key, entry_player, entry_odds, entry_stake = trade
        if not event_key:
            continue

        match = finished_map.get(str(event_key))
        if not match:
            continue  # Match not finished yet

        # Resolve winner name
        winner_raw = match.get("winner", "")
        if winner_raw == "First Player":
            winner_name = match.get("p1", "")
        elif winner_raw == "Second Player":
            winner_name = match.get("p2", "")
        else:
            winner_name = winner_raw

        if not winner_name:
            continue

        # Determine outcome — compare abbreviated names from same API source
        won = (entry_player.strip().lower() == winner_name.strip().lower())
        pnl = round(entry_stake * (entry_odds - 1), 2) if won else -entry_stake
        status_val = "WON" if won else "LOST"

        try:
            cur.execute(f"""
                UPDATE paper_trades
                SET status = {ph}, pnl = {ph},
                    settled_at = NOW(), settled_by = 'auto'
                WHERE trade_id = {ph}
            """, (status_val, pnl, trade_id))
            settled += 1
            log(f"Auto-settled {trade_id}: {status_val} | PnL: ₹{pnl:+,.0f}")
        except Exception as e:
            log(f"Error settling {trade_id}: {e}")

    if settled:
        log(f"Settled {settled} trade(s)")


# ── Schedule tracking ─────────────────────────────────────────────────────────

class TaskScheduler:
    def __init__(self):
        self.last_daily_scan  = None  # date string YYYY-MM-DD
        self.last_nightly_elo = None  # date string YYYY-MM-DD

    def now_ist(self) -> datetime:
        return datetime.now(IST)

    def should_run_daily_scan(self) -> bool:
        now = self.now_ist()
        today = now.strftime("%Y-%m-%d")
        if self.last_daily_scan == today:
            return False
        h, m = now.hour, now.minute
        return (h == DAILY_SCAN_TIME[0] and m >= DAILY_SCAN_TIME[1])

    def should_run_nightly_elo(self) -> bool:
        now = self.now_ist()
        today = now.strftime("%Y-%m-%d")
        if self.last_nightly_elo == today:
            return False
        h, m = now.hour, now.minute
        return (h == NIGHTLY_ELO_TIME[0] and m >= NIGHTLY_ELO_TIME[1])

    def mark_daily_scan(self):
        self.last_daily_scan = self.now_ist().strftime("%Y-%m-%d")

    def mark_nightly_elo(self):
        self.last_nightly_elo = self.now_ist().strftime("%Y-%m-%d")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true",
                        help="Run all tasks once immediately and exit")
    args = parser.parse_args()

    log("TennisTrade v2 Scheduler starting...")
    log(f"  Daily scan:   {DAILY_SCAN_TIME[0]:02d}:{DAILY_SCAN_TIME[1]:02d} IST")
    log(f"  Nightly Elo:  {NIGHTLY_ELO_TIME[0]:02d}:{NIGHTLY_ELO_TIME[1]:02d} IST")
    log(f"  Settlement:   every {SETTLE_INTERVAL_SECS}s")

    conn, db_type = get_connection()
    log(f"  Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}")

    if args.test:
        log("TEST MODE — running all tasks once")
        check_and_settle(conn, db_type)
        run_task("step10_daily_scan.py", ["--dry-run"])
        run_task("step3_at_elo_nightly.py", ["--dry-run"])
        conn.close()
        return

    scheduler = TaskScheduler()
    last_settle = time.time()

    log("Scheduler running. Ctrl+C to stop.")

    while True:
        try:
            # Reconnect if connection dropped
            if conn is None:
                log("Reconnecting to DB...")
                conn, db_type = get_connection()

            now = time.time()

            # Auto-settlement every 60s
            if now - last_settle >= SETTLE_INTERVAL_SECS:
                check_and_settle(conn, db_type)
                last_settle = now

            # Daily scan check
            if scheduler.should_run_daily_scan():
                log("Triggering daily scan (8 AM IST)...")
                run_task("step10_daily_scan.py")
                scheduler.mark_daily_scan()

            # Nightly Elo check
            if scheduler.should_run_nightly_elo():
                log("Triggering nightly Elo update (11 PM IST)...")
                run_task("step3_at_elo_nightly.py")
                scheduler.mark_nightly_elo()

            time.sleep(30)  # Check every 30 seconds

        except KeyboardInterrupt:
            log("Scheduler stopped.")
            break
        except Exception as e:
            log(f"Scheduler error: {e}")
            conn = None  # Force reconnect next cycle
            time.sleep(60)

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
