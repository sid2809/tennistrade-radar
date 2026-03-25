"""
TennisTrade — Step 6: Live Match Radar
======================================
THE CORE PRODUCT.

This is not a prediction model you check before matches. This is a live
radar that runs continuously, watches EVERY ongoing tennis match, and
sends you Telegram alerts:

  🟢 ENTER: "Lay Swiatek at 1.28 — serving for set at 5-4, WTA Clay"
  🔴 EXIT:  "Green up NOW — broken! Odds at 1.55. Lay ₹722 to lock ₹278"
  ⚪ SKIP:  (silence — no edge in any live match right now)

Architecture:
  1. Poll live scores every 15 seconds (all ATP/WTA matches)
  2. Poll exchange odds for matches with active positions or potential signals
  3. Run T1-T5 strategy scanners against every match state
  4. If signal fires → Telegram alert with exact instructions
  5. Track open positions → monitor for exit/stop-loss conditions
  6. Send exit alert when target hit or stop triggered

Data flow:
  Live Scores API → Match State → Strategy Scanner → Signal?
       ↓                                                ↓
  Exchange Odds API                              Telegram Alert
       ↓                                                ↓
  Position Tracker ← ← ← ← ← ← ← ← ← ← You execute manually
       ↓
  Exit Monitor → Exit Signal → Telegram "GREEN UP NOW"

Usage:
    # Set up environment
    export DATABASE_URL="your_railway_postgresql_url"
    export TELEGRAM_BOT_TOKEN="your_bot_token"
    export TELEGRAM_CHAT_ID="your_chat_id"
    
    # Optional: live score API
    export SCORE_API_KEY="your_api_key"  # FlashScore, SportRadar, etc.
    
    # Run the radar
    python step6_radar.py
    
    # Test mode (simulates with fake data)
    python step6_radar.py --test
    
    # Dry run (scans but doesn't send Telegram)
    python step6_radar.py --dry-run
"""

import os
import sys
import json
import time
import logging
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum

from step5_trading import (
    GreenUpCalculator, TradingEngine, Position, Side, Signal,
    signal_t1_break_recovery, signal_t3_serve_for_set_lay,
    signal_t4_double_break_recovery,
)

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("radar")


# ── Match State ──────────────────────────────────────────────────────────────

class MatchStatus(Enum):
    NOT_STARTED = "not_started"
    LIVE = "live"
    FINISHED = "finished"
    SUSPENDED = "suspended"


@dataclass
class PlayerState:
    """State of one player in a live match."""
    name: str
    player_id: Optional[int] = None
    sets_won: int = 0
    games_in_current_set: int = 0
    points: str = "0"  # "0", "15", "30", "40", "AD"
    is_serving: bool = False
    
    # From database (loaded at match start)
    elo_overall: float = 1500
    elo_surface: float = 1500
    elo_blended: float = 1500
    serve_hold_pct: Optional[float] = None
    break_rate: Optional[float] = None  # How often they break opponent
    comeback_rate: Optional[float] = None  # How often they come back from 2+ breaks down


@dataclass 
class MatchState:
    """Complete state of a live match — updated every poll."""
    match_id: str
    tournament: str
    surface: str
    tour: str  # "ATP" or "WTA"
    round: str
    best_of: int  # 3 or 5
    
    player1: PlayerState
    player2: PlayerState
    
    status: MatchStatus = MatchStatus.NOT_STARTED
    
    # Odds (from exchange)
    p1_back_odds: Optional[float] = None
    p1_lay_odds: Optional[float] = None
    p2_back_odds: Optional[float] = None
    p2_lay_odds: Optional[float] = None
    pre_match_p1_odds: Optional[float] = None  # Snapshot at match start
    pre_match_p2_odds: Optional[float] = None
    
    # Derived
    total_sets_played: int = 0
    current_set_number: int = 1
    
    # History within this match
    breaks_in_current_set: int = 0
    last_break_by: Optional[str] = None  # Player name who last broke
    
    # Timestamps
    last_updated: Optional[datetime] = None
    match_start_time: Optional[datetime] = None
    
    @property
    def is_wta(self) -> bool:
        return self.tour.upper() == "WTA"
    
    @property
    def server(self) -> PlayerState:
        return self.player1 if self.player1.is_serving else self.player2
    
    @property
    def returner(self) -> PlayerState:
        return self.player2 if self.player1.is_serving else self.player1
    
    @property
    def games_score(self) -> str:
        """Current set game score, e.g. '5-4'."""
        return f"{self.player1.games_in_current_set}-{self.player2.games_in_current_set}"
    
    @property
    def sets_score(self) -> str:
        """Set score, e.g. '1-0'."""
        return f"{self.player1.sets_won}-{self.player2.sets_won}"
    
    @property
    def favorite(self) -> PlayerState:
        """Pre-match favorite (lower odds = more likely to win)."""
        if self.pre_match_p1_odds and self.pre_match_p2_odds:
            return self.player1 if self.pre_match_p1_odds < self.pre_match_p2_odds else self.player2
        if self.player1.elo_blended >= self.player2.elo_blended:
            return self.player1
        return self.player2
    
    @property
    def underdog(self) -> PlayerState:
        if self.favorite == self.player1:
            return self.player2
        return self.player1
    
    def get_odds(self, player: PlayerState) -> Optional[float]:
        """Get current back odds for a player."""
        if player == self.player1:
            return self.p1_back_odds
        return self.p2_back_odds
    
    def get_pre_match_odds(self, player: PlayerState) -> Optional[float]:
        if player == self.player1:
            return self.pre_match_p1_odds
        return self.pre_match_p2_odds
    
    @property
    def game_lead(self) -> int:
        """How many games ahead is the leader in the current set."""
        return abs(self.player1.games_in_current_set - self.player2.games_in_current_set)
    
    @property
    def set_leader(self) -> Optional[PlayerState]:
        """Who is leading in the current set (or None if tied)."""
        if self.player1.games_in_current_set > self.player2.games_in_current_set:
            return self.player1
        elif self.player2.games_in_current_set > self.player1.games_in_current_set:
            return self.player2
        return None
    
    def someone_serving_for_set(self) -> Optional[PlayerState]:
        """Returns the player serving for the set, or None."""
        p1g = self.player1.games_in_current_set
        p2g = self.player2.games_in_current_set
        
        # Player 1 serving for set: they lead 5-4 or 6-5 AND they're serving
        if self.player1.is_serving and p1g >= 5 and p1g > p2g and p1g - p2g >= 1:
            return self.player1
        if self.player2.is_serving and p2g >= 5 and p2g > p1g and p2g - p1g >= 1:
            return self.player2
        
        return None
    
    def has_double_break(self) -> Optional[Tuple[PlayerState, PlayerState]]:
        """
        Returns (dominant_player, trailing_player) if there's a double break
        situation (3+ game lead in current set). Returns None otherwise.
        """
        p1g = self.player1.games_in_current_set
        p2g = self.player2.games_in_current_set
        
        if p1g - p2g >= 3 and p1g <= 5:  # e.g., 4-1, 5-2, 4-0
            return (self.player1, self.player2)
        elif p2g - p1g >= 3 and p2g <= 5:
            return (self.player2, self.player1)
        
        return None


# ── Strategy Scanner ─────────────────────────────────────────────────────────

class StrategyScanner:
    """
    Scans a match state against all strategies and returns actionable signals.
    This is the brain — it decides WHAT to trade and WHEN.
    """
    
    def __init__(self):
        self.active_signals: Dict[str, Signal] = {}  # match_id -> last signal
        self.cooldowns: Dict[str, datetime] = {}  # match_id -> don't signal until
    
    def is_on_cooldown(self, match_id: str) -> bool:
        """Prevent signal spam — 2 minute cooldown after each signal."""
        if match_id in self.cooldowns:
            return datetime.now() < self.cooldowns[match_id]
        return False
    
    def set_cooldown(self, match_id: str, seconds: int = 120):
        self.cooldowns[match_id] = datetime.now() + timedelta(seconds=seconds)
    
    def scan(self, match: MatchState) -> List[Signal]:
        """
        Run all strategies against a match state.
        Returns list of signals (usually 0 or 1).
        """
        if match.status != MatchStatus.LIVE:
            return []
        
        if self.is_on_cooldown(match.match_id):
            return []
        
        signals = []
        
        # ── T1: Break-of-serve trading ──
        sig = self._check_t1(match)
        if sig:
            signals.append(sig)
        
        # ── T3: Serving-for-set lay ──
        sig = self._check_t3(match)
        if sig:
            signals.append(sig)
        
        # ── T4: Double-break recovery ──
        sig = self._check_t4(match)
        if sig:
            signals.append(sig)
        
        # ── T5: Pre-match value lay ──
        sig = self._check_t5(match)
        if sig:
            signals.append(sig)
        
        # Set cooldown if we generated a signal
        if signals:
            self.set_cooldown(match.match_id)
        
        return signals
    
    def _check_t1(self, m: MatchState) -> Optional[Signal]:
        """T1: Has the favorite been broken? Are odds spiked?"""
        fav = m.favorite
        fav_odds = m.get_odds(fav)
        pre_odds = m.get_pre_match_odds(fav)
        
        if not fav_odds or not pre_odds:
            return None
        
        return signal_t1_break_recovery(
            favorite_name=fav.name,
            underdog_name=m.underdog.name,
            pre_match_fav_odds=pre_odds,
            current_fav_odds=fav_odds,
            sets_score=m.sets_score,
            games_score=m.games_score,
            fav_serve_hold_pct=fav.serve_hold_pct,
            underdog_break_rate=m.underdog.break_rate,
            surface=m.surface,
            fav_elo=fav.elo_blended,
            underdog_elo=m.underdog.elo_blended,
        )
    
    def _check_t3(self, m: MatchState) -> Optional[Signal]:
        """T3: Is someone serving for the set?"""
        server_for_set = m.someone_serving_for_set()
        if not server_for_set:
            return None
        
        server_odds = m.get_odds(server_for_set)
        if not server_odds:
            return None
        
        returner = m.player2 if server_for_set == m.player1 else m.player1
        
        return signal_t3_serve_for_set_lay(
            server_name=server_for_set.name,
            returner_name=returner.name,
            server_odds=server_odds,
            server_hold_pct=server_for_set.serve_hold_pct,
            surface=m.surface,
            set_score=m.games_score,
            is_wta=m.is_wta,
        )
    
    def _check_t4(self, m: MatchState) -> Optional[Signal]:
        """T4: Is there a double break situation?"""
        result = m.has_double_break()
        if not result:
            return None
        
        dominant, trailing = result
        dom_odds = m.get_odds(dominant)
        if not dom_odds:
            return None
        
        return signal_t4_double_break_recovery(
            dominant_name=dominant.name,
            trailing_name=trailing.name,
            dominant_odds=dom_odds,
            games_score=m.games_score,
            trailing_comeback_rate=trailing.comeback_rate,
            surface=m.surface,
            is_wta=m.is_wta,
        )
    
    def _check_t5(self, m: MatchState) -> Optional[Signal]:
        """T5: Heavy favorite mispriced pre-match or early in set 1?"""
        # Only in set 1, first 4 games
        if m.current_set_number > 1:
            return None
        
        total_games = m.player1.games_in_current_set + m.player2.games_in_current_set
        if total_games > 4:
            return None
        
        fav = m.favorite
        fav_odds = m.get_odds(fav)
        
        if not fav_odds or fav_odds > 1.20:
            return None  # Not short enough for T5
        
        # Check if model thinks the market is too short
        elo_prob = 1 / (1 + 10 ** ((m.underdog.elo_blended - fav.elo_blended) / 400))
        market_prob = 1 / fav_odds
        
        edge = market_prob - elo_prob
        
        if edge < 0.05:
            return None  # Need at least 5% edge (market too short vs model)
        
        liability = 100 * (fav_odds - 1)
        
        return Signal(
            strategy="T5",
            action=Side.LAY,
            player=fav.name,
            odds_target=fav_odds,
            confidence=min(0.7, edge * 5),
            reason=(f"Heavy fav {fav.name} at {fav_odds:.2f}. Market implies "
                    f"{market_prob*100:.0f}%, model says {elo_prob*100:.0f}%. "
                    f"Edge: {edge*100:.0f}%. Early in match ({m.games_score})."),
            stop_loss=f"If favorite breaks in game 3-4, cut. Loss capped at odds structure.",
            take_profit=f"If underdog holds serve or breaks, green up immediately.",
            stake_pct=0.015,
        )


# ── Telegram Alerts ──────────────────────────────────────────────────────────

class TelegramBot:
    """Sends trading alerts via Telegram."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.enabled = bool(token and chat_id)
        
        if not self.enabled:
            log.warning("Telegram not configured — alerts will print to console only")
    
    def send(self, message: str, silent: bool = False):
        """Send a message to the configured chat."""
        # Always print to console
        print(f"\n{'─'*50}")
        print(message)
        print(f"{'─'*50}\n")
        
        if not self.enabled:
            return
        
        try:
            data = json.dumps({
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_notification": silent,
            }).encode("utf-8")
            
            req = urllib.request.Request(
                f"{self.base_url}/sendMessage",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            log.error(f"Telegram send failed: {e}")
    
    def send_entry_signal(self, signal: Signal, stake: float, match: MatchState):
        """Format and send an entry signal."""
        emoji = "🟢" if signal.action == Side.BACK else "🔴"
        action = signal.action.value
        
        if signal.action == Side.LAY:
            liability = stake * (signal.odds_target - 1)
            stake_line = f"💰 Lay stake: ₹{stake:,.0f} (liability: ₹{liability:,.0f})"
        else:
            stake_line = f"💰 Stake: ₹{stake:,.0f}"
        
        msg = (
            f"{emoji} <b>ENTER — {signal.strategy}</b>\n"
            f"\n"
            f"🎾 {match.player1.name} vs {match.player2.name}\n"
            f"🏟 {match.tournament} | {match.surface} | {'WTA' if match.is_wta else 'ATP'}\n"
            f"📊 Score: {match.sets_score} | {match.games_score}\n"
            f"\n"
            f"➡️ <b>{action} {signal.player} @ {signal.odds_target:.2f}</b>\n"
            f"{stake_line}\n"
            f"\n"
            f"📝 {signal.reason}\n"
            f"\n"
            f"🎯 Target: {signal.take_profit}\n"
            f"🛑 Stop: {signal.stop_loss}\n"
            f"⚡ Confidence: {signal.confidence*100:.0f}%"
        )
        self.send(msg)
    
    def send_exit_signal(self, position: Position, current_odds: float,
                         green_up_result: dict, reason: str):
        """Send an exit / green-up signal."""
        if green_up_result.get("status") == "profit":
            emoji = "✅"
            action = "GREEN UP"
        else:
            emoji = "⚠️"
            action = "CUT LOSS"
        
        pnl = green_up_result.get("profit_equal", 0)
        pnl_sign = "+" if pnl >= 0 else ""
        
        # Determine what to do
        first_bet = position.bets[0]
        if first_bet.side == Side.BACK:
            exit_action = f"LAY {position.player_name} @ {current_odds:.2f}"
            exit_stake = green_up_result.get("lay_stake", 0)
        else:
            exit_action = f"BACK {position.player_name} @ {current_odds:.2f}"
            exit_stake = green_up_result.get("back_stake", 0)
        
        msg = (
            f"{emoji} <b>{action} — {position.strategy}</b>\n"
            f"\n"
            f"🎾 {position.player_name} vs {position.opponent_name}\n"
            f"🏟 {position.tournament}\n"
            f"\n"
            f"➡️ <b>{exit_action}</b>\n"
            f"💰 Stake: ₹{exit_stake:,.0f}\n"
            f"📊 Locked P&L: {pnl_sign}₹{pnl:,.0f}\n"
            f"\n"
            f"📝 {reason}"
        )
        self.send(msg)
    
    def send_session_summary(self, engine: TradingEngine):
        """End-of-session summary."""
        self.send(engine.session_summary())


# ── Exit Monitor ─────────────────────────────────────────────────────────────

class ExitMonitor:
    """
    Watches open positions and determines when to exit.
    Generates exit signals based on strategy-specific rules.
    """
    
    def __init__(self):
        self.calc = GreenUpCalculator()
    
    def check_exit(self, position: Position, match: MatchState) -> Optional[dict]:
        """
        Check if an open position should be exited.
        
        Returns dict with exit instructions, or None if no exit needed.
        """
        if position.status != "OPEN" or not position.bets:
            return None
        
        first_bet = position.bets[0]
        player = match.player1 if match.player1.name == position.player_name else match.player2
        current_odds = match.get_odds(player)
        
        if not current_odds:
            return None
        
        strategy = position.strategy
        
        # ── Strategy-specific exit rules ──
        
        if strategy == "T1":
            return self._exit_t1(position, match, first_bet, current_odds)
        elif strategy == "T3":
            return self._exit_t3(position, match, first_bet, current_odds)
        elif strategy == "T4":
            return self._exit_t4(position, match, first_bet, current_odds)
        elif strategy == "T5":
            return self._exit_t5(position, match, first_bet, current_odds)
        
        return None
    
    def _exit_t1(self, pos, match, bet, current_odds):
        """
        T1 exit: We backed the favorite after they were broken.
        Exit when:
          ✅ Favorite breaks back → odds return → GREEN UP
          🛑 Opponent goes 2 breaks up → CUT LOSS
          🛑 Opponent serving for set → CUT LOSS
        """
        # Profit target: odds returned at least 60% toward pre-match
        if current_odds < bet.odds * 0.90:  # Odds dropped 10%+ from our entry
            green = self.calc.green_up(bet.odds, bet.stake, current_odds)
            if green.get("status") == "profit":
                return {
                    "action": "GREEN_UP",
                    "reason": f"Odds shortened from {bet.odds:.2f} → {current_odds:.2f}. Break back likely occurred.",
                    "green_up": green,
                    "current_odds": current_odds,
                }
        
        # Stop loss: odds drifted 30%+ beyond entry (opponent dominating)
        if current_odds > bet.odds * 1.30:
            green = self.calc.green_up(bet.odds, bet.stake, current_odds)
            return {
                "action": "CUT_LOSS",
                "reason": f"Odds drifted to {current_odds:.2f} (30%+ beyond entry {bet.odds:.2f}). Cut loss.",
                "green_up": green,
                "current_odds": current_odds,
            }
        
        return None
    
    def _exit_t3(self, pos, match, bet, current_odds):
        """
        T3 exit: We laid someone serving for the set.
        Exit when:
          ✅ They got broken → odds spiked → GREEN UP (big profit)
          🛑 They held serve (won the set) → loss is auto-capped
        """
        # Check if odds have risen (they got broken or lost the game)
        if bet.side == Side.LAY:
            if current_odds > bet.odds * 1.10:  # Odds rose 10%+
                green = self.calc.reverse_green_up(bet.odds, bet.stake, current_odds)
                if green.get("status") == "profit":
                    return {
                        "action": "GREEN_UP",
                        "reason": f"Server broken! Odds spiked from {bet.odds:.2f} → {current_odds:.2f}.",
                        "green_up": green,
                        "current_odds": current_odds,
                    }
            
            # Auto-exit if odds dropped (they held/won set)
            if current_odds < bet.odds * 0.92:
                green = self.calc.reverse_green_up(bet.odds, bet.stake, current_odds)
                return {
                    "action": "CUT_LOSS",
                    "reason": f"Server held. Odds dropped to {current_odds:.2f}. Auto-exit.",
                    "green_up": green,
                    "current_odds": current_odds,
                }
        
        return None
    
    def _exit_t4(self, pos, match, bet, current_odds):
        """
        T4 exit: We laid at extreme short odds (1.02-1.08) after double break.
        Exit when:
          ✅ ANY odds drift upward → GREEN UP (even small move = huge % return)
          🛑 If dominant player wins set → loss = original tiny liability
        """
        if bet.side == Side.LAY:
            # Even a tiny move from 1.04 → 1.08 is worth greening up
            if current_odds > bet.odds + 0.03:
                green = self.calc.reverse_green_up(bet.odds, bet.stake, current_odds)
                if green.get("status") == "profit":
                    return {
                        "action": "GREEN_UP",
                        "reason": f"Comeback started! Odds moved {bet.odds:.2f} → {current_odds:.2f}.",
                        "green_up": green,
                        "current_odds": current_odds,
                    }
        
        return None
    
    def _exit_t5(self, pos, match, bet, current_odds):
        """
        T5 exit: We laid a heavy favorite early in match.
        Exit when:
          ✅ Underdog performing well → fav odds drifting → GREEN UP
          🛑 Favorite crushing → odds dropping → CUT LOSS at 30% of liability
        """
        if bet.side == Side.LAY:
            if current_odds > bet.odds * 1.08:
                green = self.calc.reverse_green_up(bet.odds, bet.stake, current_odds)
                if green.get("status") == "profit":
                    return {
                        "action": "GREEN_UP",
                        "reason": f"Favorite drifting. Odds: {bet.odds:.2f} → {current_odds:.2f}.",
                        "green_up": green,
                        "current_odds": current_odds,
                    }
            
            if current_odds < bet.odds * 0.96:
                green = self.calc.reverse_green_up(bet.odds, bet.stake, current_odds)
                return {
                    "action": "CUT_LOSS",
                    "reason": f"Favorite steamrolling. Cut at {current_odds:.2f}.",
                    "green_up": green,
                    "current_odds": current_odds,
                }
        
        return None


# ── Live Score Fetcher (Pluggable) ───────────────────────────────────────────

class LiveScoreFetcher:
    """
    Abstract base for fetching live scores. Implement one of:
    - BetfairAPIFetcher (if platform exposes API)
    - FlashScoreFetcher (free, scraping-based)
    - SportRadarFetcher (paid API)
    - ManualFetcher (you update scores via Telegram commands)
    
    For now, this provides the interface. The actual implementation
    depends on which data source you set up.
    """
    
    def fetch_live_matches(self) -> List[MatchState]:
        """
        Returns list of all currently live tennis matches.
        Override this in your implementation.
        """
        raise NotImplementedError("Implement a live score fetcher")
    
    def fetch_odds(self, match_id: str) -> Optional[dict]:
        """
        Fetch current exchange odds for a match.
        Returns {"p1_back": float, "p1_lay": float, "p2_back": float, "p2_lay": float}
        """
        raise NotImplementedError("Implement odds fetcher")


class ManualFetcher(LiveScoreFetcher):
    """
    Manual mode: you input match states via the console or Telegram.
    Useful for testing and for when no API is available.
    """
    
    def __init__(self):
        self.matches: Dict[str, MatchState] = {}
    
    def add_match(self, match: MatchState):
        self.matches[match.match_id] = match
    
    def update_score(self, match_id: str, p1_games: int, p2_games: int,
                     p1_sets: int, p2_sets: int, server: int,
                     p1_odds: float, p2_odds: float):
        """Update a match's score and odds."""
        if match_id not in self.matches:
            return
        
        m = self.matches[match_id]
        m.player1.games_in_current_set = p1_games
        m.player2.games_in_current_set = p2_games
        m.player1.sets_won = p1_sets
        m.player2.sets_won = p2_sets
        m.player1.is_serving = (server == 1)
        m.player2.is_serving = (server == 2)
        m.p1_back_odds = p1_odds
        m.p2_back_odds = p2_odds
        m.status = MatchStatus.LIVE
        m.last_updated = datetime.now()
    
    def fetch_live_matches(self) -> List[MatchState]:
        return [m for m in self.matches.values() if m.status == MatchStatus.LIVE]


# ── Main Radar Loop ──────────────────────────────────────────────────────────

class Radar:
    """
    The main loop. Polls scores → scans strategies → sends alerts → monitors exits.
    """
    
    def __init__(self, fetcher: LiveScoreFetcher, bot: TelegramBot,
                 engine: TradingEngine, poll_interval: int = 15):
        self.fetcher = fetcher
        self.bot = bot
        self.engine = engine
        self.scanner = StrategyScanner()
        self.exit_monitor = ExitMonitor()
        self.poll_interval = poll_interval
        
        # Track which matches we've already sent entry signals for
        self.signaled_matches: Dict[str, str] = {}  # match_id -> strategy
        
        # Open positions by match
        self.positions: Dict[str, Position] = {}
    
    def run(self, dry_run: bool = False):
        """Main loop — runs until interrupted."""
        log.info("🎾 TennisTrade Radar starting...")
        log.info(f"   Poll interval: {self.poll_interval}s")
        log.info(f"   Bankroll: ₹{self.engine.bankroll:,.0f}")
        log.info(f"   Telegram: {'ON' if self.bot.enabled else 'OFF (console only)'}")
        log.info(f"   Dry run: {dry_run}")
        log.info("   Press Ctrl+C to stop\n")
        
        if not dry_run and self.bot.enabled:
            self.bot.send("🎾 TennisTrade Radar is LIVE\n"
                         f"💰 Bankroll: ₹{self.engine.bankroll:,.0f}\n"
                         f"⏱ Scanning every {self.poll_interval}s")
        
        cycle = 0
        try:
            while True:
                cycle += 1
                self._tick(cycle, dry_run)
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            log.info("\n🛑 Radar stopped")
            if self.engine.closed_positions:
                summary = self.engine.session_summary()
                log.info(summary)
                if self.bot.enabled:
                    self.bot.send_session_summary(self.engine)
    
    def _tick(self, cycle: int, dry_run: bool):
        """One scan cycle."""
        try:
            # 1. Fetch all live matches
            matches = self.fetcher.fetch_live_matches()
            
            if cycle % 20 == 1:  # Log match count periodically
                log.info(f"Monitoring {len(matches)} live matches")
            
            for match in matches:
                # 2. Check exits for open positions
                if match.match_id in self.positions:
                    pos = self.positions[match.match_id]
                    if pos.status == "OPEN":
                        exit_result = self.exit_monitor.check_exit(pos, match)
                        if exit_result:
                            self._handle_exit(pos, match, exit_result, dry_run)
                
                # 3. Scan for new entry signals
                can_trade, reason = self.engine.can_trade()
                if not can_trade:
                    continue
                
                signals = self.scanner.scan(match)
                for signal in signals:
                    self._handle_signal(signal, match, dry_run)
        
        except Exception as e:
            log.error(f"Tick error: {e}")
    
    def _handle_signal(self, signal: Signal, match: MatchState, dry_run: bool):
        """Process a new trading signal."""
        # Don't double-enter the same match
        if match.match_id in self.positions:
            return
        
        stake = self.engine.calculate_stake(signal)
        
        log.info(f"📊 SIGNAL: {signal.strategy} — {signal.action.value} "
                f"{signal.player} @ {signal.odds_target:.2f}")
        
        if not dry_run:
            self.bot.send_entry_signal(signal, stake, match)
            
            # Open position (assume you'll execute manually)
            pos = self.engine.open_position(
                match_id=match.match_id,
                player=signal.player,
                opponent=(match.player2.name if signal.player == match.player1.name 
                         else match.player1.name),
                strategy=signal.strategy,
                side=signal.action,
                odds=signal.odds_target,
                stake=stake,
                surface=match.surface,
                tournament=match.tournament,
            )
            self.positions[match.match_id] = pos
    
    def _handle_exit(self, position: Position, match: MatchState, 
                     exit_result: dict, dry_run: bool):
        """Process an exit signal."""
        green_up = exit_result["green_up"]
        pnl = green_up.get("profit_equal", 0)
        
        action = exit_result["action"]
        emoji = "✅" if action == "GREEN_UP" else "⚠️"
        
        log.info(f"{emoji} EXIT: {position.strategy} — {action} "
                f"P&L: ₹{pnl:+,.0f}")
        
        if not dry_run:
            self.bot.send_exit_signal(
                position, exit_result["current_odds"],
                green_up, exit_result["reason"]
            )
            self.engine.close_position(position, pnl, exit_result["reason"])


# ── Test / Demo ──────────────────────────────────────────────────────────────

def run_test():
    """Simulate a live trading session with fake match data."""
    
    print("=" * 60)
    print("TennisTrade — Live Radar Simulation")
    print("=" * 60)
    
    bot = TelegramBot(token="", chat_id="")  # Console only
    engine = TradingEngine(bankroll=50000)
    fetcher = ManualFetcher()
    
    # ── Set up a simulated WTA match ──
    match = MatchState(
        match_id="SIM_001",
        tournament="Madrid Open",
        surface="Clay",
        tour="WTA",
        round="QF",
        best_of=3,
        player1=PlayerState(
            name="Swiatek", elo_blended=2050,
            serve_hold_pct=0.73, break_rate=0.35,
        ),
        player2=PlayerState(
            name="Gauff", elo_blended=1950,
            serve_hold_pct=0.68, break_rate=0.30,
            comeback_rate=0.12,
        ),
        pre_match_p1_odds=1.45,
        pre_match_p2_odds=3.00,
        status=MatchStatus.LIVE,
        match_start_time=datetime.now(),
    )
    fetcher.add_match(match)
    
    radar = Radar(fetcher, bot, engine, poll_interval=1)
    scanner = radar.scanner
    exit_monitor = radar.exit_monitor
    
    print("\n── Simulating match progression ──\n")
    
    # Game 1-2: Normal holds
    print("⏱ Games 1-2: Both players hold serve")
    fetcher.update_score("SIM_001", p1_games=1, p2_games=1, 
                         p1_sets=0, p2_sets=0, server=1,
                         p1_odds=1.45, p2_odds=3.00)
    signals = scanner.scan(fetcher.matches["SIM_001"])
    print(f"  Signals: {len(signals)} (expected: 0)")
    
    # Game 3: Swiatek gets broken!
    print("\n⏱ Game 3: Swiatek BROKEN! Score 1-2, Gauff serving")
    fetcher.update_score("SIM_001", p1_games=1, p2_games=2,
                         p1_sets=0, p2_sets=0, server=2,
                         p1_odds=1.80, p2_odds=2.10)
    scanner.cooldowns.clear()  # Clear for demo
    signals = scanner.scan(fetcher.matches["SIM_001"])
    print(f"  Signals: {len(signals)}")
    for sig in signals:
        print(f"  → {sig.strategy}: {sig.action.value} {sig.player} @ {sig.odds_target:.2f}")
        print(f"    Reason: {sig.reason}")
        
        # Execute signal
        stake = engine.calculate_stake(sig)
        pos = engine.open_position(
            "SIM_001", sig.player, "Gauff", sig.strategy,
            sig.action, sig.odds_target, stake, "Clay", "Madrid Open"
        )
        radar.positions["SIM_001"] = pos
        bot.send_entry_signal(sig, stake, match)
    
    # Game 4-5: Holds, then Swiatek breaks back!
    print("\n⏱ Games 4-5: Holds, then Swiatek BREAKS BACK! Score 3-2")
    fetcher.update_score("SIM_001", p1_games=3, p2_games=2,
                         p1_sets=0, p2_sets=0, server=1,
                         p1_odds=1.42, p2_odds=3.10)
    
    if "SIM_001" in radar.positions:
        pos = radar.positions["SIM_001"]
        exit_result = exit_monitor.check_exit(pos, fetcher.matches["SIM_001"])
        if exit_result:
            print(f"  EXIT SIGNAL: {exit_result['action']}")
            print(f"  Reason: {exit_result['reason']}")
            green = exit_result["green_up"]
            print(f"  Green-up P&L: ₹{green['profit_equal']:+,.0f}")
            bot.send_exit_signal(pos, exit_result["current_odds"], green, exit_result["reason"])
            engine.close_position(pos, green["profit_equal"])
    
    # ── Simulate a T3 scenario ──
    print("\n\n── New scenario: T3 Serving-for-Set ──\n")
    
    # Reset scanner
    scanner.cooldowns.clear()
    
    match2 = MatchState(
        match_id="SIM_002",
        tournament="Roland Garros",
        surface="Clay",
        tour="WTA",
        round="R16",
        best_of=3,
        player1=PlayerState(
            name="Sabalenka", elo_blended=2000,
            serve_hold_pct=0.70,
        ),
        player2=PlayerState(
            name="Zheng", elo_blended=1850,
            serve_hold_pct=0.65,
        ),
        pre_match_p1_odds=1.35,
        pre_match_p2_odds=3.50,
        status=MatchStatus.LIVE,
    )
    fetcher.add_match(match2)
    
    print("⏱ Sabalenka serving for the set at 5-4")
    fetcher.update_score("SIM_002", p1_games=5, p2_games=4,
                         p1_sets=0, p2_sets=0, server=1,
                         p1_odds=1.28, p2_odds=4.20)
    
    signals = scanner.scan(fetcher.matches["SIM_002"])
    print(f"  Signals: {len(signals)}")
    for sig in signals:
        print(f"  → {sig.strategy}: {sig.action.value} {sig.player} @ {sig.odds_target:.2f}")
        stake = engine.calculate_stake(sig)
        pos2 = engine.open_position(
            "SIM_002", sig.player, "Zheng", sig.strategy,
            sig.action, sig.odds_target, stake, "Clay", "Roland Garros"
        )
        radar.positions["SIM_002"] = pos2
        bot.send_entry_signal(sig, stake, match2)
    
    # Sabalenka gets BROKEN!
    print("\n⏱ Sabalenka BROKEN! Score 5-5, Zheng serving")
    fetcher.update_score("SIM_002", p1_games=5, p2_games=5,
                         p1_sets=0, p2_sets=0, server=2,
                         p1_odds=1.55, p2_odds=2.60)
    
    if "SIM_002" in radar.positions:
        pos2 = radar.positions["SIM_002"]
        exit_result = exit_monitor.check_exit(pos2, fetcher.matches["SIM_002"])
        if exit_result:
            green = exit_result["green_up"]
            print(f"  EXIT: {exit_result['action']} — P&L: ₹{green['profit_equal']:+,.0f}")
            bot.send_exit_signal(pos2, exit_result["current_odds"], green, exit_result["reason"])
            engine.close_position(pos2, green["profit_equal"])
    
    # ── T4 scenario ──
    print("\n\n── New scenario: T4 Jackpot Lay ──\n")
    scanner.cooldowns.clear()
    
    match3 = MatchState(
        match_id="SIM_003",
        tournament="Australian Open",
        surface="Hard",
        tour="WTA",
        round="SF",
        best_of=3,
        player1=PlayerState(
            name="Rybakina", elo_blended=1980,
            serve_hold_pct=0.78,
        ),
        player2=PlayerState(
            name="Pegula", elo_blended=1900,
            serve_hold_pct=0.70,
            comeback_rate=0.13,
        ),
        pre_match_p1_odds=1.60,
        pre_match_p2_odds=2.50,
        status=MatchStatus.LIVE,
    )
    fetcher.add_match(match3)
    
    print("⏱ Rybakina dominates: 4-0 in set 1 (double break)")
    fetcher.update_score("SIM_003", p1_games=4, p2_games=0,
                         p1_sets=0, p2_sets=0, server=1,
                         p1_odds=1.05, p2_odds=14.0)
    
    signals = scanner.scan(fetcher.matches["SIM_003"])
    print(f"  Signals: {len(signals)}")
    for sig in signals:
        print(f"  → {sig.strategy}: {sig.action.value} {sig.player} @ {sig.odds_target:.2f}")
        print(f"    {sig.reason}")
        stake = engine.calculate_stake(sig)
        liability = stake * (sig.odds_target - 1)
        print(f"    Stake: ₹{stake:,.0f} | Liability: ₹{liability:,.0f}")
        pos3 = engine.open_position(
            "SIM_003", sig.player, "Pegula", sig.strategy,
            sig.action, sig.odds_target, stake, "Hard", "Australian Open"
        )
        radar.positions["SIM_003"] = pos3
    
    # Pegula fights back! Breaks once
    print("\n⏱ Pegula breaks back! Score 4-2, fighting!")
    fetcher.update_score("SIM_003", p1_games=4, p2_games=2,
                         p1_sets=0, p2_sets=0, server=2,
                         p1_odds=1.18, p2_odds=5.50)
    
    if "SIM_003" in radar.positions:
        pos3 = radar.positions["SIM_003"]
        exit_result = exit_monitor.check_exit(pos3, fetcher.matches["SIM_003"])
        if exit_result:
            green = exit_result["green_up"]
            print(f"  EXIT: {exit_result['action']} — P&L: ₹{green['profit_equal']:+,.0f}")
            pct = green.get('pct_return', 0)
            print(f"  Return on liability: {pct}%")
            engine.close_position(pos3, green["profit_equal"])
    
    # ── Session summary ──
    print(f"\n{engine.session_summary()}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisTrade Live Radar")
    parser.add_argument("--test", action="store_true", help="Run simulation")
    parser.add_argument("--dry-run", action="store_true", help="Scan but don't alert")
    parser.add_argument("--poll", type=int, default=15, help="Poll interval in seconds")
    parser.add_argument("--bankroll", type=float, default=50000, help="Starting bankroll")
    args = parser.parse_args()
    
    if args.test:
        run_test()
        return
    
    # Real mode
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    
    bot = TelegramBot(token, chat_id)
    engine = TradingEngine(bankroll=args.bankroll)
    
    # TODO: Replace ManualFetcher with your actual score/odds API
    # Options:
    #   - Platform's Betfair API wrapper (best — gives both scores AND odds)
    #   - SportRadar API for scores + separate odds polling
    #   - FlashScore scraper for scores + platform odds manual entry
    fetcher = ManualFetcher()
    
    radar = Radar(fetcher, bot, engine, poll_interval=args.poll)
    radar.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
