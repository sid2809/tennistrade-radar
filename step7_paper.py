"""
TennisTrade — Step 7: Paper Trading Engine
==========================================
Auto-executes every signal the radar generates in a virtual portfolio.
Uses real live odds from API-Tennis but zero real money.

What it does:
  - Every signal from the scanner → auto-opened as a virtual position
  - Monitors live odds for exit conditions → auto-closed with virtual P&L
  - Maintains complete trade journal in PostgreSQL
  - Generates daily/weekly/strategy-level reports
  - After 200+ paper trades, you know EXACTLY which strategies work

Integrates with the radar (step6_radar.py):
  radar = Radar(fetcher, bot, engine)
  paper = PaperTrader(engine, db_conn)
  radar.paper_trader = paper  # Radar auto-feeds signals to paper trader

Usage:
    # Standalone report (after paper trading has run)
    python step7_paper.py --report
    python step7_paper.py --report --strategy T3
    python step7_paper.py --report --period weekly
    
    # Reset paper trading journal
    python step7_paper.py --reset
    
    # Export to CSV
    python step7_paper.py --export trades.csv
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict
from collections import defaultdict

from db import get_connection, placeholder, batch_insert
from step5_trading import (
    GreenUpCalculator, Side, Signal, Position,
)


# ── Paper Trade Record ───────────────────────────────────────────────────────

@dataclass
class PaperTrade:
    """A complete paper trade from entry to exit."""
    trade_id: str
    strategy: str          # T1, T2, T3, T4, T5
    
    # Match info
    match_id: str
    player1: str
    player2: str
    tournament: str
    surface: str
    tour: str              # ATP / WTA
    
    # Entry
    entry_side: str        # BACK or LAY
    entry_player: str      # Who we backed/laid
    entry_odds: float
    entry_stake: float
    entry_liability: float  # For lays: stake * (odds - 1). For backs: stake
    entry_time: str
    entry_score: str       # Match score at entry e.g. "0-0 | 3-1"
    entry_reason: str      # Signal reason text
    
    # Exit (filled when closed)
    exit_odds: Optional[float] = None
    exit_stake: Optional[float] = None  # Lay/back amount to green up
    exit_time: Optional[str] = None
    exit_score: Optional[str] = None
    exit_reason: Optional[str] = None
    exit_type: Optional[str] = None  # GREEN_UP, CUT_LOSS, MATCH_ENDED, TIMEOUT
    
    # P&L
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None  # % return on liability/stake
    
    # Status
    status: str = "OPEN"   # OPEN, CLOSED
    
    # Confidence from signal
    confidence: float = 0.0


# ── Paper Trading Engine ─────────────────────────────────────────────────────

class PaperTrader:
    """
    Receives signals from the radar scanner, auto-executes them as paper
    trades, monitors for exits, and records everything.
    """
    
    def __init__(self, bankroll: float = 50000, 
                 max_open: int = 3,
                 max_daily_loss_pct: float = 0.05,
                 max_stake_pct: float = 0.03,
                 db_conn=None, db_type="pg"):
        
        self.initial_bankroll = bankroll
        self.bankroll = bankroll
        self.max_open = max_open
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_stake_pct = max_stake_pct
        
        self.db_conn = db_conn
        self.db_type = db_type
        
        self.open_trades: Dict[str, PaperTrade] = {}  # match_id -> trade
        self.closed_trades: List[PaperTrade] = []
        self.daily_pnl: float = 0
        self.today: str = datetime.now().strftime("%Y-%m-%d")
        
        self.calc = GreenUpCalculator()
        self.trade_counter = 0
        
        # Create DB tables if connected
        if self.db_conn:
            self._create_tables()
            self._load_state()
    
    # ── Signal Processing ────────────────────────────────────────────────
    
    def on_signal(self, signal: Signal, match_state) -> Optional[PaperTrade]:
        """
        Called by the radar when a strategy signal fires.
        Auto-opens a paper trade if risk limits allow.
        
        Returns the PaperTrade if opened, None if blocked.
        """
        # Reset daily counter if new day
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.today:
            self.today = today
            self.daily_pnl = 0
        
        # Risk checks
        if len(self.open_trades) >= self.max_open:
            return None
        
        if self.daily_pnl <= -(self.bankroll * self.max_daily_loss_pct):
            return None
        
        if match_state.match_id in self.open_trades:
            return None  # Already have a position in this match
        
        # Calculate stake
        stake_pct = min(signal.stake_pct, self.max_stake_pct)
        stake = round(self.bankroll * stake_pct, 0)
        stake = max(10, stake)
        
        if signal.action == Side.LAY:
            liability = stake * (signal.odds_target - 1)
        else:
            liability = stake
        
        # Create paper trade
        self.trade_counter += 1
        trade_id = f"PT_{self.today}_{self.trade_counter:04d}"
        
        score_str = f"{match_state.sets_score} | {match_state.games_score}"
        
        trade = PaperTrade(
            trade_id=trade_id,
            strategy=signal.strategy,
            match_id=match_state.match_id,
            player1=match_state.player1.name,
            player2=match_state.player2.name,
            tournament=match_state.tournament,
            surface=match_state.surface,
            tour=match_state.tour,
            entry_side=signal.action.value,
            entry_player=signal.player,
            entry_odds=signal.odds_target,
            entry_stake=stake,
            entry_liability=round(liability, 2),
            entry_time=datetime.now().isoformat(),
            entry_score=score_str,
            entry_reason=signal.reason,
            confidence=signal.confidence,
        )
        
        self.open_trades[match_state.match_id] = trade
        self._save_trade(trade)
        
        return trade
    
    def on_exit(self, match_id: str, current_odds: float,
                exit_type: str, reason: str, 
                match_score: str = "") -> Optional[PaperTrade]:
        """
        Called by the radar when exit conditions trigger.
        Auto-closes the paper trade and calculates P&L.
        """
        if match_id not in self.open_trades:
            return None
        
        trade = self.open_trades[match_id]
        
        # Calculate green-up P&L
        if trade.entry_side == "BACK":
            green = self.calc.green_up(
                trade.entry_odds, trade.entry_stake, current_odds
            )
            pnl = green.get("profit_equal", 0)
            exit_stake = green.get("lay_stake", 0)
        else:  # LAY
            green = self.calc.reverse_green_up(
                trade.entry_odds, trade.entry_stake, current_odds
            )
            pnl = green.get("profit_equal", 0)
            exit_stake = green.get("back_stake", 0)
        
        # Fill exit fields
        trade.exit_odds = current_odds
        trade.exit_stake = round(exit_stake, 2)
        trade.exit_time = datetime.now().isoformat()
        trade.exit_score = match_score
        trade.exit_reason = reason
        trade.exit_type = exit_type
        trade.pnl = round(pnl, 2)
        trade.pnl_pct = round(
            (pnl / trade.entry_liability * 100) if trade.entry_liability > 0 else 0, 1
        )
        trade.status = "CLOSED"
        
        # Update bankroll
        self.bankroll += pnl
        self.daily_pnl += pnl
        
        # Move to closed
        del self.open_trades[match_id]
        self.closed_trades.append(trade)
        self._save_trade(trade)
        
        return trade
    
    def force_close_stale(self, max_age_hours: int = 6):
        """Close any trades that have been open too long (match probably ended)."""
        now = datetime.now()
        stale = []
        
        for match_id, trade in self.open_trades.items():
            entry_time = datetime.fromisoformat(trade.entry_time)
            if (now - entry_time).total_seconds() > max_age_hours * 3600:
                stale.append(match_id)
        
        for match_id in stale:
            trade = self.open_trades[match_id]
            # Close at entry odds (flat — no P&L since we don't know final odds)
            self.on_exit(
                match_id, trade.entry_odds,
                "TIMEOUT", f"Trade open for {max_age_hours}+ hours — force closed",
                match_score="unknown"
            )
    
    # ── Reporting ────────────────────────────────────────────────────────
    
    def report(self, strategy: str = None, period: str = "all") -> str:
        """Generate a comprehensive performance report."""
        trades = self.closed_trades[:]
        
        # Load from DB if available
        if self.db_conn and not trades:
            trades = self._load_closed_trades()
        
        # Filter by strategy
        if strategy:
            trades = [t for t in trades if t.strategy == strategy]
        
        # Filter by period
        if period == "today":
            today = datetime.now().strftime("%Y-%m-%d")
            trades = [t for t in trades if t.entry_time and t.entry_time.startswith(today)]
        elif period == "weekly":
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            trades = [t for t in trades if t.entry_time and t.entry_time >= week_ago]
        elif period == "monthly":
            month_ago = (datetime.now() - timedelta(days=30)).isoformat()
            trades = [t for t in trades if t.entry_time and t.entry_time >= month_ago]
        
        if not trades:
            return "No closed trades found for the specified filter."
        
        # ── Compute metrics ──
        total = len(trades)
        wins = sum(1 for t in trades if t.pnl and t.pnl > 0)
        losses = sum(1 for t in trades if t.pnl and t.pnl < 0)
        flat = total - wins - losses
        
        total_pnl = sum(t.pnl or 0 for t in trades)
        total_staked = sum(t.entry_liability or 0 for t in trades)
        
        win_rate = (wins / total * 100) if total > 0 else 0
        roi = (total_pnl / total_staked * 100) if total_staked > 0 else 0
        
        avg_win = 0
        avg_loss = 0
        if wins > 0:
            avg_win = sum(t.pnl for t in trades if t.pnl and t.pnl > 0) / wins
        if losses > 0:
            avg_loss = sum(t.pnl for t in trades if t.pnl and t.pnl < 0) / losses
        
        # Max drawdown
        running = 0
        peak = 0
        max_dd = 0
        for t in sorted(trades, key=lambda x: x.entry_time or ""):
            running += (t.pnl or 0)
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd
        
        # Best and worst trades
        best = max(trades, key=lambda t: t.pnl or 0)
        worst = min(trades, key=lambda t: t.pnl or 0)
        
        # ── By strategy ──
        by_strat = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0, "staked": 0})
        for t in trades:
            s = t.strategy
            by_strat[s]["trades"] += 1
            if t.pnl and t.pnl > 0:
                by_strat[s]["wins"] += 1
            by_strat[s]["pnl"] += (t.pnl or 0)
            by_strat[s]["staked"] += (t.entry_liability or 0)
        
        # ── By surface ──
        by_surface = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0})
        for t in trades:
            s = t.surface or "Unknown"
            by_surface[s]["trades"] += 1
            if t.pnl and t.pnl > 0:
                by_surface[s]["wins"] += 1
            by_surface[s]["pnl"] += (t.pnl or 0)
        
        # ── By tour ──
        by_tour = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0})
        for t in trades:
            tour = t.tour or "Unknown"
            by_tour[tour]["trades"] += 1
            if t.pnl and t.pnl > 0:
                by_tour[tour]["wins"] += 1
            by_tour[tour]["pnl"] += (t.pnl or 0)
        
        # ── By exit type ──
        by_exit = defaultdict(int)
        for t in trades:
            by_exit[t.exit_type or "unknown"] += 1
        
        # ── Daily P&L ──
        daily = defaultdict(float)
        for t in trades:
            if t.exit_time:
                day = t.exit_time[:10]
                daily[day] += (t.pnl or 0)
        
        # ── Build report ──
        lines = [
            "=" * 62,
            "PAPER TRADING REPORT",
            "=" * 62,
            f"  Period: {period}" + (f" | Strategy: {strategy}" if strategy else ""),
            f"  Bankroll: ₹{self.initial_bankroll:,.0f} → ₹{self.bankroll:,.0f} "
            f"({(self.bankroll/self.initial_bankroll - 1)*100:+.1f}%)",
            "",
            f"  Total trades: {total}",
            f"  Wins: {wins} | Losses: {losses} | Flat: {flat}",
            f"  Win rate: {win_rate:.1f}%",
            f"  Total P&L: ₹{total_pnl:+,.0f}",
            f"  Total risked: ₹{total_staked:,.0f}",
            f"  ROI on risk: {roi:+.1f}%",
            f"  Avg win: ₹{avg_win:+,.0f} | Avg loss: ₹{avg_loss:+,.0f}",
            f"  Max drawdown: ₹{max_dd:,.0f}",
            "",
            f"  Best trade: ₹{best.pnl:+,.0f} ({best.strategy} — "
            f"{best.player1} vs {best.player2})",
            f"  Worst trade: ₹{worst.pnl:+,.0f} ({worst.strategy} — "
            f"{worst.player1} vs {worst.player2})",
        ]
        
        # Strategy breakdown
        lines.append("")
        lines.append("  By strategy:")
        for s in sorted(by_strat.keys()):
            d = by_strat[s]
            wr = (d["wins"] / d["trades"] * 100) if d["trades"] > 0 else 0
            sr = (d["pnl"] / d["staked"] * 100) if d["staked"] > 0 else 0
            lines.append(
                f"    {s}: {d['trades']} trades, {wr:.0f}% WR, "
                f"₹{d['pnl']:+,.0f} ({sr:+.1f}% ROI)"
            )
        
        # Surface breakdown
        lines.append("")
        lines.append("  By surface:")
        for s in sorted(by_surface.keys()):
            d = by_surface[s]
            wr = (d["wins"] / d["trades"] * 100) if d["trades"] > 0 else 0
            lines.append(f"    {s}: {d['trades']} trades, {wr:.0f}% WR, ₹{d['pnl']:+,.0f}")
        
        # Tour breakdown
        lines.append("")
        lines.append("  By tour:")
        for tour in sorted(by_tour.keys()):
            d = by_tour[tour]
            wr = (d["wins"] / d["trades"] * 100) if d["trades"] > 0 else 0
            lines.append(f"    {tour}: {d['trades']} trades, {wr:.0f}% WR, ₹{d['pnl']:+,.0f}")
        
        # Exit types
        lines.append("")
        lines.append("  By exit type:")
        for etype, count in sorted(by_exit.items(), key=lambda x: -x[1]):
            lines.append(f"    {etype}: {count}")
        
        # Daily P&L (last 10 days)
        if daily:
            lines.append("")
            lines.append("  Daily P&L (recent):")
            for day in sorted(daily.keys())[-10:]:
                pnl = daily[day]
                bar = "+" * int(abs(pnl) / 50) if pnl > 0 else "-" * int(abs(pnl) / 50)
                lines.append(f"    {day}: ₹{pnl:+,.0f} {bar}")
        
        # Verdict
        lines.append("")
        lines.append("  " + "─" * 50)
        if total < 50:
            lines.append(f"  ⏳ Need more trades ({total}/50 minimum for significance)")
        elif roi > 5:
            lines.append(f"  ✓ PROFITABLE — {roi:+.1f}% ROI. Consider going live with real money.")
        elif roi > 0:
            lines.append(f"  ~ MARGINAL — {roi:+.1f}% ROI. Tighten filters or drop weak strategies.")
        else:
            lines.append(f"  ✗ UNPROFITABLE — {roi:+.1f}% ROI. Do NOT go live. Review model.")
        
        # Specific strategy recommendations
        if total >= 50:
            lines.append("")
            lines.append("  Recommendations:")
            for s in sorted(by_strat.keys()):
                d = by_strat[s]
                if d["trades"] >= 10:
                    sr = (d["pnl"] / d["staked"] * 100) if d["staked"] > 0 else 0
                    if sr > 5:
                        lines.append(f"    ✓ {s}: Keep — {sr:+.1f}% ROI is strong")
                    elif sr > 0:
                        lines.append(f"    ~ {s}: Marginal — consider tightening filters")
                    else:
                        lines.append(f"    ✗ {s}: DROP — {sr:+.1f}% ROI after {d['trades']} trades")
        
        lines.append("=" * 62)
        return "\n".join(lines)
    
    def trade_log(self, last_n: int = 20) -> str:
        """Print last N trades as a table."""
        trades = self.closed_trades[-last_n:]
        if not trades:
            return "No closed trades yet."
        
        lines = [
            f"{'ID':<18} {'Strat':<4} {'Side':<5} {'Entry':>6} {'Exit':>6} "
            f"{'P&L':>8} {'%':>6} {'Type':<10} {'Match':<25}",
            "─" * 100,
        ]
        
        for t in trades:
            match_short = f"{t.player1[:10]} v {t.player2[:10]}"
            pnl_str = f"₹{t.pnl:+,.0f}" if t.pnl else "—"
            pct_str = f"{t.pnl_pct:+.0f}%" if t.pnl_pct else "—"
            lines.append(
                f"{t.trade_id:<18} {t.strategy:<4} {t.entry_side:<5} "
                f"{t.entry_odds:>6.2f} {(t.exit_odds or 0):>6.2f} "
                f"{pnl_str:>8} {pct_str:>6} {(t.exit_type or 'OPEN'):<10} "
                f"{match_short:<25}"
            )
        
        return "\n".join(lines)
    
    # ── Database Persistence ─────────────────────────────────────────────
    
    def _create_tables(self):
        """Create paper trades table."""
        if not self.db_conn:
            return
        
        cur = self.db_conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                trade_id        TEXT PRIMARY KEY,
                strategy        TEXT,
                match_id        TEXT,
                player1         TEXT,
                player2         TEXT,
                tournament      TEXT,
                surface         TEXT,
                tour            TEXT,
                entry_side      TEXT,
                entry_player    TEXT,
                entry_odds      REAL,
                entry_stake     REAL,
                entry_liability REAL,
                entry_time      TEXT,
                entry_score     TEXT,
                entry_reason    TEXT,
                exit_odds       REAL,
                exit_stake      REAL,
                exit_time       TEXT,
                exit_score      TEXT,
                exit_reason     TEXT,
                exit_type       TEXT,
                pnl             REAL,
                pnl_pct         REAL,
                status          TEXT,
                confidence      REAL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_state (
                key     TEXT PRIMARY KEY,
                value   TEXT
            )
        """)
        
        self.db_conn.commit()
    
    def _save_trade(self, trade: PaperTrade):
        """Save or update a trade in the database."""
        if not self.db_conn:
            return
        
        cur = self.db_conn.cursor()
        ph = placeholder(self.db_type)
        
        if self.db_type == "pg":
            cur.execute(f"""
                INSERT INTO paper_trades (
                    trade_id, strategy, match_id, player1, player2,
                    tournament, surface, tour, entry_side, entry_player,
                    entry_odds, entry_stake, entry_liability, entry_time,
                    entry_score, entry_reason, exit_odds, exit_stake,
                    exit_time, exit_score, exit_reason, exit_type,
                    pnl, pnl_pct, status, confidence
                ) VALUES (
                    {ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},
                    {ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},
                    {ph},{ph},{ph},{ph},{ph},{ph}
                ) ON CONFLICT (trade_id) DO UPDATE SET
                    exit_odds = EXCLUDED.exit_odds,
                    exit_stake = EXCLUDED.exit_stake,
                    exit_time = EXCLUDED.exit_time,
                    exit_score = EXCLUDED.exit_score,
                    exit_reason = EXCLUDED.exit_reason,
                    exit_type = EXCLUDED.exit_type,
                    pnl = EXCLUDED.pnl,
                    pnl_pct = EXCLUDED.pnl_pct,
                    status = EXCLUDED.status
            """, (
                trade.trade_id, trade.strategy, trade.match_id,
                trade.player1, trade.player2, trade.tournament,
                trade.surface, trade.tour, trade.entry_side,
                trade.entry_player, trade.entry_odds, trade.entry_stake,
                trade.entry_liability, trade.entry_time, trade.entry_score,
                trade.entry_reason, trade.exit_odds, trade.exit_stake,
                trade.exit_time, trade.exit_score, trade.exit_reason,
                trade.exit_type, trade.pnl, trade.pnl_pct,
                trade.status, trade.confidence,
            ))
        else:
            cur.execute(f"""
                INSERT OR REPLACE INTO paper_trades (
                    trade_id, strategy, match_id, player1, player2,
                    tournament, surface, tour, entry_side, entry_player,
                    entry_odds, entry_stake, entry_liability, entry_time,
                    entry_score, entry_reason, exit_odds, exit_stake,
                    exit_time, exit_score, exit_reason, exit_type,
                    pnl, pnl_pct, status, confidence
                ) VALUES (
                    {ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},
                    {ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},
                    {ph},{ph},{ph},{ph},{ph},{ph}
                )
            """, (
                trade.trade_id, trade.strategy, trade.match_id,
                trade.player1, trade.player2, trade.tournament,
                trade.surface, trade.tour, trade.entry_side,
                trade.entry_player, trade.entry_odds, trade.entry_stake,
                trade.entry_liability, trade.entry_time, trade.entry_score,
                trade.entry_reason, trade.exit_odds, trade.exit_stake,
                trade.exit_time, trade.exit_score, trade.exit_reason,
                trade.exit_type, trade.pnl, trade.pnl_pct,
                trade.status, trade.confidence,
            ))
        
        # Save bankroll state
        cur.execute(f"""
            INSERT INTO paper_state (key, value) VALUES ({ph}, {ph})
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """ if self.db_type == "pg" else f"""
            INSERT OR REPLACE INTO paper_state (key, value) VALUES ({ph}, {ph})
        """, ("bankroll", str(self.bankroll)))
        
        self.db_conn.commit()
    
    def _load_state(self):
        """Load bankroll and open trades from DB on startup."""
        if not self.db_conn:
            return
        
        cur = self.db_conn.cursor()
        ph = placeholder(self.db_type)
        
        # Load bankroll
        cur.execute(f"SELECT value FROM paper_state WHERE key = {ph}", ("bankroll",))
        row = cur.fetchone()
        if row:
            self.bankroll = float(row[0])
        
        # Load open trades
        cur.execute(f"SELECT * FROM paper_trades WHERE status = {ph}", ("OPEN",))
        for row in cur.fetchall():
            trade = self._row_to_trade(row, cur.description)
            if trade:
                self.open_trades[trade.match_id] = trade
        
        # Load trade counter
        cur.execute("SELECT COUNT(*) FROM paper_trades")
        self.trade_counter = cur.fetchone()[0]
    
    def _load_closed_trades(self) -> List[PaperTrade]:
        """Load all closed trades from DB."""
        if not self.db_conn:
            return []
        
        cur = self.db_conn.cursor()
        ph = placeholder(self.db_type)
        cur.execute(f"SELECT * FROM paper_trades WHERE status = {ph} ORDER BY entry_time", 
                    ("CLOSED",))
        
        trades = []
        for row in cur.fetchall():
            trade = self._row_to_trade(row, cur.description)
            if trade:
                trades.append(trade)
        return trades
    
    def _row_to_trade(self, row, description) -> Optional[PaperTrade]:
        """Convert a DB row to a PaperTrade."""
        if not row:
            return None
        
        cols = [d[0] for d in description]
        data = dict(zip(cols, row))
        
        return PaperTrade(
            trade_id=data.get("trade_id", ""),
            strategy=data.get("strategy", ""),
            match_id=data.get("match_id", ""),
            player1=data.get("player1", ""),
            player2=data.get("player2", ""),
            tournament=data.get("tournament", ""),
            surface=data.get("surface", ""),
            tour=data.get("tour", ""),
            entry_side=data.get("entry_side", ""),
            entry_player=data.get("entry_player", ""),
            entry_odds=data.get("entry_odds", 0),
            entry_stake=data.get("entry_stake", 0),
            entry_liability=data.get("entry_liability", 0),
            entry_time=data.get("entry_time", ""),
            entry_score=data.get("entry_score", ""),
            entry_reason=data.get("entry_reason", ""),
            exit_odds=data.get("exit_odds"),
            exit_stake=data.get("exit_stake"),
            exit_time=data.get("exit_time"),
            exit_score=data.get("exit_score"),
            exit_reason=data.get("exit_reason"),
            exit_type=data.get("exit_type"),
            pnl=data.get("pnl"),
            pnl_pct=data.get("pnl_pct"),
            status=data.get("status", "OPEN"),
            confidence=data.get("confidence", 0),
        )
    
    def export_csv(self, filepath: str):
        """Export all trades to CSV."""
        trades = self._load_closed_trades() if self.db_conn else self.closed_trades
        
        if not trades:
            print("No trades to export.")
            return
        
        import csv
        fields = [
            "trade_id", "strategy", "match_id", "player1", "player2",
            "tournament", "surface", "tour", "entry_side", "entry_player",
            "entry_odds", "entry_stake", "entry_liability", "entry_time",
            "entry_score", "exit_odds", "exit_stake", "exit_time",
            "exit_score", "exit_type", "pnl", "pnl_pct", "confidence",
        ]
        
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for trade in trades:
                row = {k: getattr(trade, k, "") for k in fields}
                writer.writerow(row)
        
        print(f"✓ Exported {len(trades)} trades to {filepath}")


# ── Telegram Report Commands ─────────────────────────────────────────────────

def format_telegram_report(paper: PaperTrader, period: str = "today") -> str:
    """Format a report for Telegram (shorter than console)."""
    trades = paper._load_closed_trades() if paper.db_conn else paper.closed_trades
    
    if period == "today":
        today = datetime.now().strftime("%Y-%m-%d")
        trades = [t for t in trades if t.entry_time and t.entry_time.startswith(today)]
    elif period == "weekly":
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        trades = [t for t in trades if t.entry_time and t.entry_time >= week_ago]
    
    if not trades:
        return f"No paper trades for {period}."
    
    total = len(trades)
    wins = sum(1 for t in trades if t.pnl and t.pnl > 0)
    total_pnl = sum(t.pnl or 0 for t in trades)
    wr = (wins / total * 100) if total > 0 else 0
    
    by_strat = defaultdict(lambda: {"n": 0, "pnl": 0})
    for t in trades:
        by_strat[t.strategy]["n"] += 1
        by_strat[t.strategy]["pnl"] += (t.pnl or 0)
    
    strat_lines = "\n".join(
        f"  {s}: {d['n']} trades, ₹{d['pnl']:+,.0f}"
        for s, d in sorted(by_strat.items())
    )
    
    return (
        f"📊 Paper trading — {period}\n\n"
        f"Trades: {total} | Wins: {wins} ({wr:.0f}%)\n"
        f"P&L: ₹{total_pnl:+,.0f}\n"
        f"Bankroll: ₹{paper.bankroll:,.0f}\n\n"
        f"By strategy:\n{strat_lines}"
    )


# ── Main (CLI) ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisTrade Paper Trading")
    parser.add_argument("--db-url", type=str, help="PostgreSQL URL")
    parser.add_argument("--local", action="store_true", help="Use SQLite")
    parser.add_argument("--report", action="store_true", help="Show report")
    parser.add_argument("--strategy", type=str, help="Filter by strategy (T1-T5)")
    parser.add_argument("--period", type=str, default="all",
                        choices=["all", "today", "weekly", "monthly"])
    parser.add_argument("--log", action="store_true", help="Show trade log")
    parser.add_argument("--log-n", type=int, default=20, help="Trades to show")
    parser.add_argument("--export", type=str, help="Export to CSV file")
    parser.add_argument("--reset", action="store_true", help="Reset all paper trades")
    parser.add_argument("--bankroll", type=float, default=50000)
    args = parser.parse_args()
    
    conn, db_type = get_connection()
    paper = PaperTrader(bankroll=args.bankroll, db_conn=conn, db_type=db_type)
    
    if args.reset:
        cur = conn.cursor()
        cur.execute("DELETE FROM paper_trades")
        cur.execute("DELETE FROM paper_state")
        conn.commit()
        print("✓ Paper trading journal reset.")
        return
    
    if args.export:
        paper.export_csv(args.export)
        return
    
    if args.log:
        print(paper.trade_log(last_n=args.log_n))
        return
    
    if args.report:
        print(paper.report(strategy=args.strategy, period=args.period))
        return
    
    # Default: show summary
    print(paper.report(period="all"))


if __name__ == "__main__":
    main()
