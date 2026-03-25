"""
TennisTrade — Step 9: Wire Everything Together
================================================
This is the glue. It connects:
  - API-Tennis live feed → MatchState objects
  - PostgreSQL Elo ratings → loaded into each player at match start
  - PostgreSQL player stats → loaded into each player at match start
  - Radar scanner → Paper trader (auto-execute every signal)
  - Telegram bot → you get alerts + paper trade report

After this, one command starts everything:
    python step9_wire.py

Usage:
    python step9_wire.py                      # Full live radar + paper trading
    python step9_wire.py --dry-run            # Scan but don't alert
    python step9_wire.py --paper-only         # Paper trade only, no Telegram
    python step9_wire.py --test               # Test with simulated API responses
    python step9_wire.py --poll 10            # Poll every 10 seconds
"""

import os
import sys
import json
import time
import logging
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional, List, Dict

from db import get_connection, placeholder
from step5_trading import GreenUpCalculator, TradingEngine, Side, Signal
from step6_radar import (
    MatchState, PlayerState, MatchStatus, LiveScoreFetcher,
    StrategyScanner, ExitMonitor, TelegramBot, Radar,
)
from step7_paper import PaperTrader, format_telegram_report
from step8_stats import lookup_player_stats, lookup_player_stats_by_id, compute_h2h

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wire")


# ── Player Enrichment (Elo + Stats from DB) ──────────────────────────────────

class PlayerEnricher:
    """
    When the radar sees a live match, this looks up both players'
    Elo ratings and serve/break stats from the database.
    
    Without this, every signal uses generic averages.
    With this, signals use player-specific data.
    """
    
    def __init__(self, conn, db_type):
        self.conn = conn
        self.db_type = db_type
        self.cache = {}  # (player_name, surface) -> stats dict
    
    def enrich(self, player: PlayerState, surface: str):
        """Load Elo + stats for a player from DB."""
        cache_key = (player.name, surface)
        if cache_key in self.cache:
            stats = self.cache[cache_key]
        else:
            # Look up stats
            stats = lookup_player_stats(self.conn, self.db_type, player.name, surface)
            
            # Look up Elo
            elo = self._lookup_elo(player.name)
            if elo:
                stats.update(elo)
            
            self.cache[cache_key] = stats
        
        # Apply to player state
        if stats:
            player.serve_hold_pct = stats.get("serve_hold_pct_52w") or stats.get("serve_hold_pct")
            player.break_rate = stats.get("break_rate_52w") or stats.get("break_rate")
            player.comeback_rate = stats.get("comeback_rate")  # From point-by-point (future)
            
            if stats.get("elo_overall"):
                player.elo_overall = stats["elo_overall"]
            if stats.get("elo_surface"):
                player.elo_blended = (stats["elo_overall"] + stats["elo_surface"]) / 2
            elif stats.get("elo_overall"):
                player.elo_blended = stats["elo_overall"]
    
    def _lookup_elo(self, player_name: str) -> dict:
        """Look up Elo from tennis_elo_current table."""
        cur = self.conn.cursor()
        ph = placeholder(self.db_type)
        
        # Try joining with players table by name
        cur.execute(f"""
            SELECT e.elo_overall, e.elo_hard, e.elo_clay, e.elo_grass
            FROM tennis_elo_current e
            JOIN tennis_players p ON e.player_id = p.player_id
            WHERE (p.first_name || ' ' || p.last_name) = {ph}
               OR p.last_name = {ph}
            ORDER BY e.elo_overall DESC
            LIMIT 1
        """, (player_name, player_name.split()[-1] if player_name else ""))
        
        row = cur.fetchone()
        if row:
            return {
                "elo_overall": row[0],
                "elo_hard": row[1],
                "elo_clay": row[2],
                "elo_grass": row[3],
                "elo_surface": {
                    "Hard": row[1], "Clay": row[2], "Grass": row[3]
                }.get(None),  # Will be set per-match
            }
        return {}
    
    def enrich_match(self, match: MatchState):
        """Enrich both players in a match."""
        surface = match.surface or "Overall"
        
        self.enrich(match.player1, surface)
        self.enrich(match.player2, surface)
        
        # Set surface-specific Elo
        elo_key = {"Hard": "elo_hard", "Clay": "elo_clay", "Grass": "elo_grass"}
        key = elo_key.get(surface)
        if key:
            for p in [match.player1, match.player2]:
                cache = self.cache.get((p.name, surface), {})
                if cache.get(key):
                    p.elo_blended = ((cache.get("elo_overall", 1500) + cache[key]) / 2)
    
    def clear_cache(self):
        """Clear cache (call daily or when new matches start)."""
        self.cache.clear()


# ── API-Tennis Fetcher ───────────────────────────────────────────────────────

class APITennisFetcher(LiveScoreFetcher):
    """
    Fetches live match data from api-tennis.com.
    
    Endpoints used:
    - get_livescore: all currently live matches with scores + odds
    - get_odds: live in-play odds for a specific match
    
    Set API_TENNIS_KEY env var or pass key to constructor.
    """
    
    BASE_URL = "https://api.api-tennis.com/tennis/"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("API_TENNIS_KEY", "")
        if not self.api_key:
            log.warning("API_TENNIS_KEY not set — fetcher will not work")
        
        self.last_fetch_time = None
        self.match_cache: Dict[str, MatchState] = {}
        self.pre_match_odds: Dict[str, dict] = {}  # match_id -> first seen odds
    
    def fetch_live_matches(self) -> List[MatchState]:
        """Fetch all currently live tennis matches."""
        if not self.api_key:
            return []
        
        url = f"{self.BASE_URL}?method=get_livescore&APIkey={self.api_key}"
        
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "TennisTrade/1.0"
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            log.error(f"API-Tennis fetch failed: {e}")
            return []
        
        if not data.get("success") or not data.get("result"):
            return []
        
        matches = []
        results = data["result"]
        
        # Handle both list and dict formats
        if isinstance(results, dict):
            results = list(results.values())
        
        for event in results:
            try:
                match = self._parse_event(event)
                if match and match.status == MatchStatus.LIVE:
                    matches.append(match)
            except Exception as e:
                log.debug(f"Failed to parse event: {e}")
                continue
        
        self.last_fetch_time = datetime.now()
        return matches
    
    def _parse_event(self, event: dict) -> Optional[MatchState]:
        """Parse an API-Tennis event into a MatchState."""
        event_key = str(event.get("event_key", ""))
        
        p1_name = event.get("event_first_player", "")
        p2_name = event.get("event_second_player", "")
        
        if not p1_name or not p2_name:
            return None
        
        # Determine surface from tournament type
        tournament = event.get("tournament_name", "") or event.get("league_name", "")
        tour_type = event.get("event_type_type", "") or ""
        
        surface = self._detect_surface(tournament, tour_type)
        tour = "WTA" if "women" in tour_type.lower() or "wta" in tour_type.lower() else "ATP"
        
        # Parse scores
        scores = event.get("scores", [])
        p1_sets = 0
        p2_sets = 0
        p1_games = 0
        p2_games = 0
        current_set = 1
        
        for s in scores:
            s1 = int(s.get("score_first", 0) or 0)
            s2 = int(s.get("score_second", 0) or 0)
            set_num = int(s.get("score_set", 1) or 1)
            
            if set_num > current_set:
                current_set = set_num
            
            # If a set is complete (6+ games and 2+ game lead, or 7-6)
            if (s1 >= 6 and s1 - s2 >= 2) or (s1 == 7 and s2 == 6):
                p1_sets += 1
            elif (s2 >= 6 and s2 - s1 >= 2) or (s2 == 7 and s1 == 6):
                p2_sets += 1
            else:
                # Current set in progress
                p1_games = s1
                p2_games = s2
        
        # Determine who's serving
        serve = event.get("event_serve", "")
        p1_serving = "first" in serve.lower() if serve else False
        p2_serving = "second" in serve.lower() if serve else False
        
        # Parse live odds
        p1_odds = None
        p2_odds = None
        live_odds = event.get("live_odds", [])
        if isinstance(live_odds, list):
            for odd in live_odds:
                odd_name = (odd.get("odd_name") or "").lower()
                if "winner" in odd_name or "match" in odd_name or "home/away" in odd_name:
                    if odd.get("type", "").lower() in ("home", "first player"):
                        try:
                            p1_odds = float(odd.get("value", 0))
                        except (ValueError, TypeError):
                            pass
                    elif odd.get("type", "").lower() in ("away", "second player"):
                        try:
                            p2_odds = float(odd.get("value", 0))
                        except (ValueError, TypeError):
                            pass
        
        # Also check pre-match odds from separate field
        if not p1_odds:
            try:
                p1_odds = float(event.get("odds_1", 0) or 0) or None
            except:
                pass
        if not p2_odds:
            try:
                p2_odds = float(event.get("odds_2", 0) or 0) or None
            except:
                pass
        
        # Determine best_of
        best_of = 3
        if any(gs in tournament.lower() for gs in 
               ["australian open", "roland garros", "french open", "wimbledon", "us open"]):
            if tour == "ATP":
                best_of = 5
        
        # Build match state
        match = MatchState(
            match_id=event_key,
            tournament=tournament,
            surface=surface,
            tour=tour,
            round=event.get("tournament_round", ""),
            best_of=best_of,
            player1=PlayerState(
                name=p1_name,
                sets_won=p1_sets,
                games_in_current_set=p1_games,
                is_serving=p1_serving,
            ),
            player2=PlayerState(
                name=p2_name,
                sets_won=p2_sets,
                games_in_current_set=p2_games,
                is_serving=p2_serving,
            ),
            status=MatchStatus.LIVE,
            p1_back_odds=p1_odds if p1_odds and p1_odds > 1 else None,
            p2_back_odds=p2_odds if p2_odds and p2_odds > 1 else None,
            current_set_number=current_set,
            last_updated=datetime.now(),
        )
        
        # Store pre-match odds (first time we see this match)
        if event_key not in self.pre_match_odds:
            self.pre_match_odds[event_key] = {
                "p1": p1_odds, "p2": p2_odds
            }
        
        match.pre_match_p1_odds = self.pre_match_odds[event_key].get("p1")
        match.pre_match_p2_odds = self.pre_match_odds[event_key].get("p2")
        
        return match
    
    def _detect_surface(self, tournament: str, tour_type: str) -> str:
        """Detect surface from tournament name."""
        t = tournament.lower()
        
        clay_keywords = ["roland garros", "french open", "madrid", "rome", 
                         "monte carlo", "barcelona", "buenos aires", "rio",
                         "hamburg", "bastad", "umag", "gstaad", "kitzbuhel"]
        grass_keywords = ["wimbledon", "halle", "queen", "eastbourne", 
                          "s-hertogenbosch", "mallorca", "newport", "stuttgart grass"]
        
        for kw in clay_keywords:
            if kw in t:
                return "Clay"
        for kw in grass_keywords:
            if kw in t:
                return "Grass"
        
        return "Hard"  # Default


# ── Integrated Radar ─────────────────────────────────────────────────────────

class IntegratedRadar(Radar):
    """
    Extended radar with:
    - Auto player enrichment (Elo + stats from DB)
    - Paper trading auto-execution
    - Telegram paper trade reports
    """
    
    def __init__(self, fetcher, bot, engine, paper, enricher,
                 poll_interval=15):
        super().__init__(fetcher, bot, engine, poll_interval)
        self.paper = paper
        self.enricher = enricher
        self.enriched_matches = set()  # Track which matches we've already enriched
    
    def _tick(self, cycle, dry_run):
        """Override tick to add enrichment and paper trading."""
        try:
            matches = self.fetcher.fetch_live_matches()
            
            if cycle % 20 == 1:
                log.info(f"Monitoring {len(matches)} live matches")
            
            for match in matches:
                # Enrich with Elo + stats (once per match)
                if match.match_id not in self.enriched_matches:
                    self.enricher.enrich_match(match)
                    self.enriched_matches.add(match.match_id)
                    
                    if match.player1.serve_hold_pct:
                        log.info(
                            f"  Enriched: {match.player1.name} "
                            f"(hold={match.player1.serve_hold_pct*100:.0f}%, "
                            f"Elo={match.player1.elo_blended:.0f}) vs "
                            f"{match.player2.name} "
                            f"(hold={match.player2.serve_hold_pct*100:.0f}%, "
                            f"Elo={match.player2.elo_blended:.0f})"
                        )
                
                # Check exits for paper trades
                if match.match_id in self.paper.open_trades:
                    self._check_paper_exit(match)
                
                # Check exits for real positions
                if match.match_id in self.positions:
                    pos = self.positions[match.match_id]
                    if pos.status == "OPEN":
                        exit_result = self.exit_monitor.check_exit(pos, match)
                        if exit_result:
                            self._handle_exit(pos, match, exit_result, dry_run)
                
                # Scan for new signals
                can_trade, reason = self.engine.can_trade()
                if not can_trade:
                    continue
                
                signals = self.scanner.scan(match)
                for signal in signals:
                    # Paper trade every signal
                    paper_trade = self.paper.on_signal(signal, match)
                    if paper_trade:
                        log.info(
                            f"📝 Paper trade opened: {paper_trade.trade_id} "
                            f"({signal.strategy} {signal.action.value} "
                            f"{signal.player} @ {signal.odds_target:.2f})"
                        )
                    
                    # Also send real alert
                    self._handle_signal(signal, match, dry_run)
            
            # Close stale paper trades periodically
            if cycle % 100 == 0:
                self.paper.force_close_stale(max_age_hours=5)
                # Clear enrichment cache daily
                if cycle % 2000 == 0:
                    self.enricher.clear_cache()
                    self.enriched_matches.clear()
        
        except Exception as e:
            log.error(f"Tick error: {e}", exc_info=True)
    
    def _check_paper_exit(self, match: MatchState):
        """Check if a paper trade should be exited."""
        trade = self.paper.open_trades.get(match.match_id)
        if not trade:
            return
        
        # Get current odds for the traded player
        if trade.entry_player == match.player1.name:
            current_odds = match.p1_back_odds
        else:
            current_odds = match.p2_back_odds
        
        if not current_odds:
            return
        
        # Run exit logic
        # Build a minimal Position for the exit monitor
        pos = type('Pos', (), {
            'status': 'OPEN', 'strategy': trade.strategy,
            'player_name': trade.entry_player,
            'bets': [type('Bet', (), {
                'side': Side.BACK if trade.entry_side == "BACK" else Side.LAY,
                'odds': trade.entry_odds, 'stake': trade.entry_stake,
            })()],
        })()
        
        exit_result = self.exit_monitor.check_exit(pos, match)
        if exit_result:
            score = f"{match.sets_score} | {match.games_score}"
            closed = self.paper.on_exit(
                match.match_id, current_odds,
                exit_result["action"], exit_result["reason"], score
            )
            if closed:
                log.info(
                    f"📝 Paper trade closed: {closed.trade_id} "
                    f"P&L ₹{closed.pnl:+,.0f} ({closed.pnl_pct:+.0f}%)"
                )


# ── Main Entry Point ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisTrade — Full System")
    parser.add_argument("--db-url", type=str, help="PostgreSQL URL")
    parser.add_argument("--local", action="store_true", help="SQLite mode")
    parser.add_argument("--dry-run", action="store_true", help="No Telegram alerts")
    parser.add_argument("--paper-only", action="store_true", help="Paper trade only")
    parser.add_argument("--test", action="store_true", help="Test with fake data")
    parser.add_argument("--poll", type=int, default=15, help="Poll interval seconds")
    parser.add_argument("--bankroll", type=float, default=50000)
    parser.add_argument("--report", type=str, choices=["today", "weekly", "all"],
                        help="Show paper trade report and exit")
    args = parser.parse_args()
    
    # ── Connect to database ──
    conn, db_type = get_connection()
    db_label = "PostgreSQL" if db_type == "pg" else "SQLite"
    
    # ── Report mode ──
    if args.report:
        paper = PaperTrader(bankroll=args.bankroll, db_conn=conn, db_type=db_type)
        print(paper.report(period=args.report))
        print(f"\n{paper.trade_log()}")
        conn.close()
        return
    
    # ── Build components ──
    print("=" * 60)
    print("TennisTrade — Starting Full System")
    print("=" * 60)
    print(f"  Database: {db_label}")
    
    # Telegram bot
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    bot = TelegramBot(token, chat_id)
    
    # Trading engine (for real alerts)
    engine = TradingEngine(bankroll=args.bankroll)
    
    # Paper trader
    paper = PaperTrader(
        bankroll=args.bankroll, 
        db_conn=conn, db_type=db_type,
        max_open=3,
    )
    print(f"  Paper bankroll: ₹{paper.bankroll:,.0f} "
          f"({len(paper.open_trades)} open trades)")
    
    # Player enricher
    enricher = PlayerEnricher(conn, db_type)
    print(f"  Player enricher: ready")
    
    # Data fetcher
    if args.test:
        from step6_radar import ManualFetcher
        fetcher = ManualFetcher()
        print(f"  Fetcher: ManualFetcher (test mode)")
    else:
        api_key = os.environ.get("API_TENNIS_KEY", "")
        fetcher = APITennisFetcher(api_key)
        print(f"  Fetcher: API-Tennis ({'configured' if api_key else 'NO KEY — set API_TENNIS_KEY'})")
    
    # ── Start radar ──
    radar = IntegratedRadar(
        fetcher=fetcher,
        bot=bot,
        engine=engine,
        paper=paper,
        enricher=enricher,
        poll_interval=args.poll,
    )
    
    print(f"  Poll interval: {args.poll}s")
    print(f"  Telegram: {'ON' if bot.enabled else 'OFF'}")
    print(f"  Dry run: {args.dry_run}")
    print("=" * 60)
    
    # Send startup notification
    if bot.enabled and not args.dry_run:
        bot.send(
            f"🎾 TennisTrade LIVE\n"
            f"💰 Bankroll: ₹{args.bankroll:,.0f}\n"
            f"📝 Paper: ₹{paper.bankroll:,.0f}\n"
            f"⏱ Poll: {args.poll}s"
        )
    
    try:
        radar.run(dry_run=args.dry_run or args.paper_only)
    except KeyboardInterrupt:
        pass
    finally:
        # Show final paper report
        print(f"\n{paper.report(period='today')}")
        
        if bot.enabled:
            bot.send(format_telegram_report(paper, "today"))
        
        conn.close()


if __name__ == "__main__":
    main()
