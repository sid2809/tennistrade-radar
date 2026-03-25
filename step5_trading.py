"""
TennisTrade — Step 5: Exchange Trading Engine
=============================================
Now that we know you have exchange access (Indian platform on Betfair API),
ALL 6 in-play strategies are live:

  T1: Break-of-serve trading (bread and butter)
  T2: Set winner trading
  T3: Serving-for-set lay (low risk, high frequency)
  T4: Double-break recovery play (jackpot lay — your Strategy 11)
  T5: Pre-match value lay of heavy favorites
  T6: Momentum scalping (advanced — Phase 2)

This module provides:
  - Green-up / cash-out calculator
  - Position manager (tracks open positions, calculates P&L)
  - Strategy signal generators
  - Trade journal integration

Usage:
    # Import as a library (used by dashboard and Telegram bot)
    from step5_trading import TradingEngine, GreenUpCalculator, Strategy
    
    # Standalone test mode
    python step5_trading.py --test
"""

import math
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple


# ── Green-Up Calculator ──────────────────────────────────────────────────────

class GreenUpCalculator:
    """
    The core math of exchange trading.
    
    You BACK at one price, then LAY at another. This calculator tells you:
    - Exactly how much to lay to guarantee profit regardless of outcome
    - Your guaranteed profit (or loss) if you green up now
    - Whether to green up for equal profit on both sides, or weight one side
    
    This is the single most important piece of code in the entire system.
    """
    
    @staticmethod
    def green_up(back_odds: float, back_stake: float, 
                 current_odds: float) -> dict:
        """
        Calculate the lay stake needed to green up (lock in profit/loss).
        
        Args:
            back_odds: The odds you backed at
            back_stake: How much you staked on the back bet
            current_odds: Current lay odds available
            
        Returns:
            dict with:
                lay_stake: how much to lay
                profit_if_wins: guaranteed profit if selection wins
                profit_if_loses: guaranteed profit if selection loses
                profit_equal: equal profit on both sides
                status: 'profit' or 'loss'
        """
        if current_odds <= 1 or back_odds <= 1:
            return {"error": "Invalid odds (must be > 1.0)"}
        
        # Option A: Equal profit on both sides
        # lay_stake = (back_odds * back_stake) / current_odds
        lay_stake_equal = (back_odds * back_stake) / current_odds
        
        # If selection wins:
        #   Back profit: back_stake * (back_odds - 1)
        #   Lay loss: lay_stake * (current_odds - 1)
        #   Net = back_stake * (back_odds - 1) - lay_stake * (current_odds - 1)
        
        # If selection loses:
        #   Back loss: -back_stake
        #   Lay profit: +lay_stake
        #   Net = lay_stake - back_stake
        
        profit_if_wins = back_stake * (back_odds - 1) - lay_stake_equal * (current_odds - 1)
        profit_if_loses = lay_stake_equal - back_stake
        
        # For truly equal green:
        # back_stake * (back_odds - 1) - lay_stake * (current_odds - 1) = lay_stake - back_stake
        # Solve for lay_stake:
        # back_stake * back_odds - back_stake - lay_stake * current_odds + lay_stake = lay_stake - back_stake
        # back_stake * back_odds = lay_stake * current_odds
        # lay_stake = (back_stake * back_odds) / current_odds
        
        equal_profit = profit_if_loses  # Both sides equal with this formula
        
        status = "profit" if equal_profit > 0 else "loss"
        
        return {
            "lay_stake": round(lay_stake_equal, 2),
            "profit_if_wins": round(profit_if_wins, 2),
            "profit_if_loses": round(profit_if_loses, 2),
            "profit_equal": round(equal_profit, 2),
            "status": status,
            "odds_moved": round(back_odds - current_odds, 2),
            "pct_return": round((equal_profit / back_stake) * 100, 1) if back_stake > 0 else 0,
        }
    
    @staticmethod
    def reverse_green_up(lay_odds: float, lay_stake: float,
                         current_odds: float) -> dict:
        """
        Green up when you LAID first (e.g., Strategy T3, T4, T5).
        You laid at one price, now want to back at current price to lock in.
        
        Args:
            lay_odds: The odds you laid at
            lay_stake: How much you laid (your liability = lay_stake * (lay_odds - 1))
            current_odds: Current back odds available
        """
        if current_odds <= 1 or lay_odds <= 1:
            return {"error": "Invalid odds"}
        
        # Back stake to equalize:
        # back_stake = (lay_stake * lay_odds) / current_odds
        back_stake_equal = (lay_stake * lay_odds) / current_odds
        
        # If selection wins:
        #   Lay loss: -lay_stake * (lay_odds - 1)  [your liability]
        #   Back profit: +back_stake * (current_odds - 1)
        #   Net = back_stake * (current_odds - 1) - lay_stake * (lay_odds - 1)
        
        # If selection loses:
        #   Lay profit: +lay_stake
        #   Back loss: -back_stake
        #   Net = lay_stake - back_stake
        
        profit_if_wins = back_stake_equal * (current_odds - 1) - lay_stake * (lay_odds - 1)
        profit_if_loses = lay_stake - back_stake_equal
        
        equal_profit = profit_if_loses
        status = "profit" if equal_profit > 0 else "loss"
        
        liability = lay_stake * (lay_odds - 1)
        
        return {
            "back_stake": round(back_stake_equal, 2),
            "profit_if_wins": round(profit_if_wins, 2),
            "profit_if_loses": round(profit_if_loses, 2),
            "profit_equal": round(equal_profit, 2),
            "status": status,
            "original_liability": round(liability, 2),
            "odds_moved": round(current_odds - lay_odds, 2),
            "pct_return": round((equal_profit / liability) * 100, 1) if liability > 0 else 0,
        }


# ── Position & Trade Tracking ────────────────────────────────────────────────

class Side(Enum):
    BACK = "BACK"
    LAY = "LAY"


@dataclass
class Bet:
    """A single back or lay bet within a position."""
    side: Side
    odds: float
    stake: float
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Position:
    """
    A trading position on a match. Can contain multiple backs and lays.
    Tracks net exposure and P&L.
    """
    match_id: str
    player_name: str  # Which player this position is on
    opponent_name: str
    strategy: str  # T1, T2, T3, T4, T5, T6
    surface: str
    tournament: str
    
    bets: List[Bet] = field(default_factory=list)
    status: str = "OPEN"  # OPEN, GREEN, RED, CLOSED
    closed_at: Optional[datetime] = None
    final_pnl: Optional[float] = None
    notes: str = ""
    
    def add_bet(self, side: Side, odds: float, stake: float):
        """Add a new bet to this position."""
        self.bets.append(Bet(side=side, odds=odds, stake=stake))
    
    @property
    def net_back_stake(self) -> float:
        """Total backed minus total laid."""
        backed = sum(b.stake for b in self.bets if b.side == Side.BACK)
        laid = sum(b.stake for b in self.bets if b.side == Side.LAY)
        return backed - laid
    
    @property
    def exposure(self) -> dict:
        """Calculate current exposure on both outcomes."""
        # If player wins
        profit_if_wins = 0
        profit_if_loses = 0
        
        for bet in self.bets:
            if bet.side == Side.BACK:
                profit_if_wins += bet.stake * (bet.odds - 1)
                profit_if_loses -= bet.stake
            else:  # LAY
                profit_if_wins -= bet.stake * (bet.odds - 1)
                profit_if_loses += bet.stake
        
        return {
            "if_wins": round(profit_if_wins, 2),
            "if_loses": round(profit_if_loses, 2),
            "is_green": profit_if_wins > 0 and profit_if_loses > 0,
            "worst_case": round(min(profit_if_wins, profit_if_loses), 2),
            "best_case": round(max(profit_if_wins, profit_if_loses), 2),
        }
    
    def close(self, pnl: float, notes: str = ""):
        """Close this position with final P&L."""
        self.status = "GREEN" if pnl > 0 else ("RED" if pnl < 0 else "FLAT")
        self.final_pnl = pnl
        self.closed_at = datetime.now()
        self.notes = notes


# ── Strategy Signal Generators ───────────────────────────────────────────────

@dataclass
class Signal:
    """A trading signal from one of the strategies."""
    strategy: str       # T1-T6
    action: Side        # BACK or LAY
    player: str         # Which player to back/lay
    odds_target: float  # Target entry odds
    confidence: float   # 0-1 scale
    reason: str         # Human-readable explanation
    stop_loss: str      # When to exit if wrong
    take_profit: str    # When to green up
    stake_pct: float    # Recommended stake as % of bankroll
    

def signal_t1_break_recovery(
    favorite_name: str, underdog_name: str,
    pre_match_fav_odds: float, current_fav_odds: float,
    sets_score: str, games_score: str,
    fav_serve_hold_pct: float, underdog_break_rate: float,
    surface: str, fav_elo: float, underdog_elo: float,
) -> Optional[Signal]:
    """
    Strategy T1: Break-of-Serve Trading
    
    When the favorite gets broken, their odds spike. Back them at inflated
    odds if the model predicts a break-back is likely.
    
    Entry: Favorite's odds have spiked 15%+ from pre-match
    Exit: Favorite breaks back (odds return) → green up
    Stop: Opponent serving for set at 5-3 or similar → cut loss
    """
    if current_fav_odds <= pre_match_fav_odds:
        return None  # Odds haven't spiked, no trade
    
    odds_spike = (current_fav_odds - pre_match_fav_odds) / pre_match_fav_odds
    
    if odds_spike < 0.15:
        return None  # Less than 15% spike — not enough edge
    
    # Estimate break-back probability
    # Higher if: fav has strong serve (high hold %), playing on fast surface,
    # early in the set (more games left to break back)
    
    # Parse games to estimate how early in the set we are
    # e.g., "1-3" means early, "3-5" means late
    try:
        parts = games_score.split("-")
        games_played = int(parts[0]) + int(parts[1])
    except:
        games_played = 6  # Default mid-set
    
    early_set_bonus = max(0, (10 - games_played) * 0.02)  # More games left = better
    
    # Surface adjustment (faster surfaces = harder to break = more likely to hold)
    surface_factor = {
        "Grass": 0.05, "Hard": 0.0, "Clay": -0.05
    }.get(surface, 0.0)
    
    # Elo gap
    elo_gap = fav_elo - underdog_elo
    elo_factor = min(0.05, elo_gap / 1000)  # Small bonus for class difference
    
    # Combined break-back probability estimate
    base_break_back = 0.35  # Average break-back rate
    adjusted = base_break_back + early_set_bonus + surface_factor + elo_factor
    adjusted += fav_serve_hold_pct * 0.1 if fav_serve_hold_pct else 0
    adjusted = max(0.20, min(0.60, adjusted))
    
    # Only signal if odds spike implies a worse probability than we calculate
    implied_fav_prob = 1 / current_fav_odds
    
    # Check if there's genuine edge
    # The trade isn't about who wins the match — it's about whether odds will
    # revert toward pre-match levels when the fav breaks back
    reversion_prob = adjusted  # Probability odds revert (break back happens)
    
    if reversion_prob < 0.30:
        return None  # Too unlikely
    
    confidence = min(0.9, reversion_prob + odds_spike * 0.3)
    
    return Signal(
        strategy="T1",
        action=Side.BACK,
        player=favorite_name,
        odds_target=current_fav_odds,
        confidence=confidence,
        reason=(f"Favorite broken, odds spiked {odds_spike*100:.0f}% "
                f"({pre_match_fav_odds:.2f} → {current_fav_odds:.2f}). "
                f"Break-back probability: {adjusted*100:.0f}%. "
                f"Games in set: {games_score}. Surface: {surface}."),
        stop_loss=f"Exit if opponent goes up 2 breaks or serves for set",
        take_profit=f"Green up when odds return to ~{pre_match_fav_odds*1.05:.2f}",
        stake_pct=min(0.03, 0.01 + confidence * 0.02),
    )


def signal_t3_serve_for_set_lay(
    server_name: str, returner_name: str,
    server_odds: float, 
    server_hold_pct: float,  # Historical serve hold %
    surface: str,
    set_score: str,  # e.g., "5-4"
    is_wta: bool = False,
) -> Optional[Signal]:
    """
    Strategy T3: Serving-for-Set Lay
    
    Lay the player serving to win the set. The risk/reward is asymmetric:
    - If they hold: odds drop slightly (small loss)  
    - If they get broken: odds spike massively (big profit)
    
    Best in WTA (more breaks) and on clay (harder to serve out).
    """
    # Only trigger when someone is serving for the set
    if set_score not in ("5-4", "5-3", "6-5"):
        return None
    
    # Estimate break probability for this specific game
    base_break_prob = 0.25  # Average across all service games
    
    # WTA has more breaks
    if is_wta:
        base_break_prob = 0.35
    
    # Adjust for surface
    surface_adj = {
        "Clay": 0.08,    # Harder to serve out on clay
        "Hard": 0.0,
        "Grass": -0.05,  # Easier to serve out on grass
    }.get(surface, 0.0)
    
    # Pressure adjustment — serving for the set is harder than regular service
    pressure_adj = 0.05
    
    # Player-specific hold rate
    if server_hold_pct:
        player_adj = (0.80 - server_hold_pct) * 0.3  # Weaker servers = more breaks
    else:
        player_adj = 0
    
    break_prob = base_break_prob + surface_adj + pressure_adj + player_adj
    break_prob = max(0.15, min(0.50, break_prob))
    
    # Risk/reward check
    # If they hold: odds drop maybe 5-15% → our loss
    # If they're broken: odds rise maybe 20-60% → our profit
    # We need break_prob * profit > (1 - break_prob) * loss
    
    # At server odds of 1.30:
    #   If holds: drops to ~1.20 (liability loss: 10 ticks on 100 stake)
    #   If broken: rises to ~1.50-1.80 (profit: 20-50 ticks)
    
    if server_odds > 1.80:
        return None  # Server isn't short enough for this strategy
    
    if server_odds < 1.05:
        return None  # Too short, liability is tiny but so is the upside
    
    # Calculate expected value
    loss_if_holds = 0.10 * server_odds  # Rough estimate of odds drop
    profit_if_broken = 0.30 * server_odds  # Rough estimate of odds rise
    
    ev = break_prob * profit_if_broken - (1 - break_prob) * loss_if_holds
    
    if ev <= 0:
        return None
    
    confidence = min(0.8, break_prob * 1.5)
    
    return Signal(
        strategy="T3",
        action=Side.LAY,
        player=server_name,
        odds_target=server_odds,
        confidence=confidence,
        reason=(f"{server_name} serving for set at {set_score}. "
                f"Break probability: {break_prob*100:.0f}%. "
                f"{'WTA' if is_wta else 'ATP'} on {surface}. "
                f"Server hold rate: {server_hold_pct*100:.0f}%." if server_hold_pct else
                f"{server_name} serving for set at {set_score}. "
                f"Break probability: {break_prob*100:.0f}%. "
                f"{'WTA' if is_wta else 'ATP'} on {surface}."),
        stop_loss="If server holds, exit immediately (loss is capped by odds structure)",
        take_profit=f"If broken, green up immediately as odds spike",
        stake_pct=0.02,  # Fixed 2% — strategy has natural loss cap
    )


def signal_t4_double_break_recovery(
    dominant_name: str, trailing_name: str,
    dominant_odds: float,
    games_score: str,  # e.g., "4-0", "5-1"
    trailing_comeback_rate: float,  # Historical comeback rate from 2+ breaks down
    surface: str,
    is_wta: bool = False,
) -> Optional[Signal]:
    """
    Strategy T4: Double-Break Recovery (Jackpot Lay)
    
    Your Strategy 11 from CricTrade, ported to tennis.
    
    When a player has a double break (leads 4-0 or 5-1), their odds crash
    to 1.02-1.08. Lay them with tiny liability. If the trailing player
    mounts ANY comeback, odds swing massively.
    
    Risk: £4 liability on a £100 lay at 1.04
    Reward: If odds move to 1.15, that's a huge proportional return
    """
    # Only trigger at extreme short prices
    if dominant_odds > 1.10:
        return None  # Not short enough for this strategy
    
    # Parse game score to confirm double break
    try:
        parts = games_score.split("-")
        lead = int(parts[0]) - int(parts[1])
    except:
        return None
    
    if lead < 3:
        return None  # Not a convincing enough lead
    
    # Estimate comeback probability
    base_comeback = 0.08  # 8% is roughly the base comeback rate from a double break
    
    # WTA has more breaks → more comebacks
    if is_wta:
        base_comeback = 0.14
    
    # Clay = more breaks = more comebacks
    surface_adj = {
        "Clay": 0.04, "Hard": 0.0, "Grass": -0.02
    }.get(surface, 0.0)
    
    # Player-specific comeback rate
    if trailing_comeback_rate:
        player_adj = (trailing_comeback_rate - 0.10) * 0.5
    else:
        player_adj = 0
    
    # Earlier in set = more games to recover
    games_played = sum(int(x) for x in games_score.split("-"))
    early_bonus = max(0, (8 - games_played) * 0.01)
    
    comeback_prob = base_comeback + surface_adj + player_adj + early_bonus
    comeback_prob = max(0.05, min(0.25, comeback_prob))
    
    # The key insight: at 1.04 odds, the market implies 96% win probability.
    # If our model says it's actually 88%, that's an 8% edge.
    # And the payoff structure is wildly asymmetric:
    #   Liability on £100 lay at 1.04 = £4
    #   If odds move to 1.15, green-up profit = £100 * (1 - 1.04/1.15) ≈ £9.57
    
    market_implied = 1 / dominant_odds
    our_estimate = 1 - comeback_prob
    edge = market_implied - our_estimate
    
    if edge < 0.03:
        return None  # Need at least 3% edge
    
    liability = 100 * (dominant_odds - 1)  # Per £100 lay
    potential_if_comeback = 100 * (1 - dominant_odds / (dominant_odds + 0.15))
    
    return Signal(
        strategy="T4",
        action=Side.LAY,
        player=dominant_name,
        odds_target=dominant_odds,
        confidence=min(0.7, comeback_prob * 3),
        reason=(f"Double break: {games_score}. {dominant_name} at {dominant_odds:.2f}. "
                f"Market implies {market_implied*100:.0f}% win, model says {our_estimate*100:.0f}%. "
                f"Comeback probability: {comeback_prob*100:.0f}%. "
                f"Liability per ₹100 lay: ₹{liability:.0f}. "
                f"{'WTA' if is_wta else 'ATP'} on {surface}."),
        stop_loss=f"Full liability (₹{liability:.0f} per ₹100 lay) — designed to be small",
        take_profit=f"Green up at first sign of break-back (any single break recovery)",
        stake_pct=0.01,  # Tiny stakes — jackpot play
    )


# ── Trading Engine ───────────────────────────────────────────────────────────

class TradingEngine:
    """
    Orchestrates all strategies, manages positions, enforces risk limits.
    """
    
    def __init__(self, bankroll: float = 50000, 
                 max_daily_loss_pct: float = 0.05,
                 max_positions: int = 2,
                 max_stake_pct: float = 0.03):
        self.bankroll = bankroll
        self.initial_bankroll = bankroll
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_positions = max_positions
        self.max_stake_pct = max_stake_pct
        
        self.positions: List[Position] = []
        self.closed_positions: List[Position] = []
        self.daily_pnl: float = 0
        self.today: str = datetime.now().strftime("%Y-%m-%d")
        
        self.green_up = GreenUpCalculator()
    
    @property
    def open_positions(self) -> List[Position]:
        return [p for p in self.positions if p.status == "OPEN"]
    
    @property
    def daily_loss_limit_hit(self) -> bool:
        max_loss = self.bankroll * self.max_daily_loss_pct
        return self.daily_pnl <= -max_loss
    
    def can_trade(self) -> Tuple[bool, str]:
        """Check if we're allowed to take a new trade."""
        if self.daily_loss_limit_hit:
            return False, f"Daily loss limit hit (₹{self.daily_pnl:,.0f})"
        
        if len(self.open_positions) >= self.max_positions:
            return False, f"Max {self.max_positions} open positions"
        
        if self.bankroll <= self.initial_bankroll * 0.5:
            return False, "Bankroll below 50% of starting — stop trading"
        
        return True, "OK"
    
    def calculate_stake(self, signal: Signal) -> float:
        """Calculate stake based on signal and risk limits."""
        stake_pct = min(signal.stake_pct, self.max_stake_pct)
        stake = self.bankroll * stake_pct
        return round(max(10, stake), 0)  # Minimum ₹10
    
    def process_signal(self, signal: Signal) -> Optional[str]:
        """
        Evaluate a signal and return action instruction.
        Does NOT auto-execute — returns what to do for manual confirmation.
        """
        can, reason = self.can_trade()
        if not can:
            return f"⊘ BLOCKED: {reason}"
        
        stake = self.calculate_stake(signal)
        
        if signal.action == Side.BACK:
            instruction = (
                f"📊 SIGNAL: {signal.strategy}\n"
                f"  BACK {signal.player} at {signal.odds_target:.2f}\n"
                f"  Stake: ₹{stake:,.0f}\n"
                f"  Confidence: {signal.confidence*100:.0f}%\n"
                f"  Reason: {signal.reason}\n"
                f"  Stop: {signal.stop_loss}\n"
                f"  Target: {signal.take_profit}"
            )
        else:
            liability = stake * (signal.odds_target - 1)
            instruction = (
                f"📊 SIGNAL: {signal.strategy}\n"
                f"  LAY {signal.player} at {signal.odds_target:.2f}\n"
                f"  Lay stake: ₹{stake:,.0f} (liability: ₹{liability:,.0f})\n"
                f"  Confidence: {signal.confidence*100:.0f}%\n"
                f"  Reason: {signal.reason}\n"
                f"  Stop: {signal.stop_loss}\n"
                f"  Target: {signal.take_profit}"
            )
        
        return instruction
    
    def open_position(self, match_id: str, player: str, opponent: str,
                      strategy: str, side: Side, odds: float, stake: float,
                      surface: str = "", tournament: str = "") -> Position:
        """Open a new trading position."""
        pos = Position(
            match_id=match_id,
            player_name=player,
            opponent_name=opponent,
            strategy=strategy,
            surface=surface,
            tournament=tournament,
        )
        pos.add_bet(side, odds, stake)
        self.positions.append(pos)
        return pos
    
    def close_position(self, position: Position, pnl: float, notes: str = ""):
        """Close a position and update bankroll."""
        position.close(pnl, notes)
        self.bankroll += pnl
        self.daily_pnl += pnl
        self.closed_positions.append(position)
    
    def session_summary(self) -> str:
        """Generate a summary of today's trading."""
        total_trades = len(self.closed_positions)
        wins = sum(1 for p in self.closed_positions if p.final_pnl and p.final_pnl > 0)
        total_pnl = sum(p.final_pnl or 0 for p in self.closed_positions)
        
        lines = [
            "═" * 50,
            "SESSION SUMMARY",
            "═" * 50,
            f"  Trades: {total_trades}",
            f"  Wins: {wins}/{total_trades}",
            f"  P&L: ₹{total_pnl:+,.0f}",
            f"  Bankroll: ₹{self.bankroll:,.0f} ({(self.bankroll/self.initial_bankroll - 1)*100:+.1f}%)",
            "",
        ]
        
        # Strategy breakdown
        by_strategy = {}
        for p in self.closed_positions:
            s = p.strategy
            if s not in by_strategy:
                by_strategy[s] = {"trades": 0, "pnl": 0}
            by_strategy[s]["trades"] += 1
            by_strategy[s]["pnl"] += p.final_pnl or 0
        
        if by_strategy:
            lines.append("  By strategy:")
            for s, data in sorted(by_strategy.items()):
                lines.append(f"    {s}: {data['trades']} trades, ₹{data['pnl']:+,.0f}")
        
        lines.append("═" * 50)
        return "\n".join(lines)


# ── Test / Demo ──────────────────────────────────────────────────────────────

def run_demo():
    """Demonstrate the trading engine with sample scenarios."""
    
    print("=" * 60)
    print("TennisTrade — Trading Engine Demo")
    print("=" * 60)
    
    # ── Green-up calculator demos ──
    calc = GreenUpCalculator()
    
    print("\n── Green-Up Calculator ──\n")
    
    # Scenario 1: Backed favorite, odds shortened (profit)
    result = calc.green_up(back_odds=2.00, back_stake=1000, current_odds=1.50)
    print("Scenario 1: Backed at 2.00, odds dropped to 1.50")
    print(f"  Lay ₹{result['lay_stake']:,.0f} at 1.50")
    print(f"  Profit if wins: ₹{result['profit_if_wins']:,.0f}")
    print(f"  Profit if loses: ₹{result['profit_if_loses']:,.0f}")
    print(f"  Status: {result['status']} ({result['pct_return']}% return)")
    
    # Scenario 2: Backed favorite, odds drifted (loss-cutting)
    result = calc.green_up(back_odds=1.80, back_stake=1000, current_odds=2.50)
    print("\nScenario 2: Backed at 1.80, odds drifted to 2.50 (cut loss)")
    print(f"  Lay ₹{result['lay_stake']:,.0f} at 2.50")
    print(f"  Loss locked: ₹{result['profit_equal']:,.0f}")
    print(f"  Status: {result['status']}")
    
    # Scenario 3: Laid for T3 strategy, player got broken (big profit)
    result = calc.reverse_green_up(lay_odds=1.30, lay_stake=1000, current_odds=1.80)
    print("\nScenario 3: T3 — Laid server at 1.30, got broken, odds now 1.80")
    print(f"  Back ₹{result['back_stake']:,.0f} at 1.80 to green up")
    print(f"  Original liability: ₹{result['original_liability']:,.0f}")
    print(f"  Locked profit: ₹{result['profit_equal']:,.0f}")
    print(f"  Return on liability: {result['pct_return']}%")
    
    # Scenario 4: T4 jackpot lay
    result = calc.reverse_green_up(lay_odds=1.04, lay_stake=5000, current_odds=1.20)
    print("\nScenario 4: T4 — Jackpot lay at 1.04, comeback! Odds now 1.20")
    print(f"  Back ₹{result['back_stake']:,.0f} at 1.20 to green up")
    print(f"  Original liability: ₹{result['original_liability']:,.0f}")
    print(f"  Locked profit: ₹{result['profit_equal']:,.0f}")
    print(f"  Return on liability: {result['pct_return']}%")
    
    # ── Signal generator demos ──
    print("\n\n── Strategy Signals ──\n")
    
    # T1: Break-of-serve trading
    sig = signal_t1_break_recovery(
        favorite_name="Djokovic", underdog_name="Ruud",
        pre_match_fav_odds=1.40, current_fav_odds=1.75,
        sets_score="0-0", games_score="1-3",
        fav_serve_hold_pct=0.88, underdog_break_rate=0.22,
        surface="Hard", fav_elo=2100, underdog_elo=1900,
    )
    if sig:
        print("T1 Signal:")
        print(f"  {sig.action.value} {sig.player} at {sig.odds_target}")
        print(f"  Reason: {sig.reason}")
        print(f"  Confidence: {sig.confidence*100:.0f}%")
    
    # T3: Serving for set lay
    sig = signal_t3_serve_for_set_lay(
        server_name="Swiatek", returner_name="Gauff",
        server_odds=1.35, server_hold_pct=0.72,
        surface="Clay", set_score="5-4", is_wta=True,
    )
    if sig:
        print(f"\nT3 Signal:")
        print(f"  {sig.action.value} {sig.player} at {sig.odds_target}")
        print(f"  Reason: {sig.reason}")
    
    # T4: Double-break recovery
    sig = signal_t4_double_break_recovery(
        dominant_name="Sabalenka", trailing_name="Pegula",
        dominant_odds=1.04, games_score="4-0",
        trailing_comeback_rate=0.12,
        surface="Hard", is_wta=True,
    )
    if sig:
        print(f"\nT4 Signal:")
        print(f"  {sig.action.value} {sig.player} at {sig.odds_target}")
        print(f"  Reason: {sig.reason}")
    
    # ── Trading engine demo ──
    print("\n\n── Trading Engine ──\n")
    
    engine = TradingEngine(bankroll=50000)
    
    can, reason = engine.can_trade()
    print(f"Can trade: {can} ({reason})")
    print(f"Bankroll: ₹{engine.bankroll:,.0f}")
    
    # Open a T1 position
    pos = engine.open_position(
        match_id="AO2026_SF1", player="Djokovic", opponent="Ruud",
        strategy="T1", side=Side.BACK, odds=1.75, stake=1000,
        surface="Hard", tournament="Australian Open"
    )
    print(f"\nOpened: BACK Djokovic at 1.75 for ₹1,000")
    print(f"  Exposure: {pos.exposure}")
    
    # Simulate green-up
    green = calc.green_up(back_odds=1.75, back_stake=1000, current_odds=1.45)
    print(f"\n  Djokovic breaks back! Odds now 1.45")
    print(f"  Green up: lay ₹{green['lay_stake']:,.0f} → profit ₹{green['profit_equal']:,.0f}")
    
    engine.close_position(pos, pnl=green['profit_equal'], notes="T1 break-back green up")
    
    print(f"\n{engine.session_summary()}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run demo")
    args = parser.parse_args()
    
    run_demo()
