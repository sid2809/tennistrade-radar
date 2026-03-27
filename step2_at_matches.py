"""TennisTrade v2 - Step 2: Pull All Historical AT Matches"""

import os, sys, json, time, argparse, threading, urllib.request, urllib.error, socket
from datetime import datetime
from typing import Optional, Set

from db import get_connection, placeholder

API_KEY  = os.environ.get("API_TENNIS_KEY", "8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b")
BASE_URL = "https://api.api-tennis.com/tennis/"
API_DELAY = 0.25

KEEP_TYPES = {"Atp Singles", "Wta Singles", "Challenger Men Singles", "Challenger Women Singles"}
SKIP_TOURNAMENTS = {
    "ATP Davis Cup - Group III", "ATP Davis Cup - Group IV", "ATP Davis Cup - Group V",
    "ATP Davis Cup - World Group", "ATP Davis Cup - World Group I", "ATP Davis Cup - World Group II",
    "ATP ATP Cup", "ATP Hopman Cup", "ATP Laver Cup", "ATP United Cup",
    "Olympic Games", "Asian Games", "Next Gen Finals - Jeddah", "Grand Slam Cup", "Zhuhai"
}

_state = {"matches": 0, "stats": 0, "pbp": 0, "errors": 0, "tournament": "", "start": time.time()}

def start_heartbeat(stop_event):
    def _beat():
        while not stop_event.wait(60):
            elapsed = (time.time() - _state["start"]) / 60
            print(f"  [heartbeat {elapsed:.0f}m] matches:{_state['matches']:,} stats:{_state['stats']:,} pbp:{_state['pbp']:,} errors:{_state['errors']} | {_state['tournament'][:50]}", flush=True)
    threading.Thread(target=_beat, daemon=True).start()

def fetch_tournaments():
    print("Fetching tournament list...", flush=True)
    url = f"{BASE_URL}?APIkey={API_KEY}&method=get_tournaments"
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "TennisTrade/2.0"}), timeout=60) as r:
        raw = r.read()
    print(f"  Got {len(raw):,} bytes, parsing...", flush=True)
    data = json.loads(raw)
    result = data.get("result", [])
    if isinstance(result, dict): result = list(result.values())
    print(f"  {len(result)} total tournaments", flush=True)
    return result

def api_fetch(params: dict) -> Optional[dict]:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?APIkey={API_KEY}&{qs}"
    delay = 1.0
    for attempt in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "TennisTrade/2.0"}), timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < 2:
                print(f"  HTTP {e.code}, retrying in {delay:.0f}s...", flush=True)
                time.sleep(delay); delay = min(delay * 2, 30)
            else:
                return None
        except Exception as ex:
            print(f"  Attempt {attempt+1} failed: {ex}", flush=True)
            if attempt < 2:
                time.sleep(delay); delay = min(delay * 2, 30)
            else:
                return None
    return None

def parse_sets(score):
    sets = []
    for part in (score or "").strip().split():
        part = part.split("[")[0]
        if "-" in part:
            try:
                a, b = part.split("-"); sets.append((int(a), int(b)))
            except ValueError:
                pass
    return sets

def parse_set_num(raw):
    s = str(raw or "0").replace("Set ", "").strip().split()[0]
    return int(s) if s.isdigit() else 0

def validate_match(m):
    errs = []
    if not m.get("event_key"): errs.append("missing event_key")
    if not m.get("event_winner"): errs.append("missing winner")
    if not m.get("event_final_result"): errs.append("missing score")
    d = m.get("event_date", "") or ""
    if d and (d < "2000-01-01" or d > "2027-01-01"): errs.append(f"bad date:{d}")
    return not errs, "; ".join(errs)

STAT_RANGES = {
    "Aces": (0,80), "Double Faults": (0,40), "1st Serve Percentage": (0,100),
    "1st Serve Points Won": (0,100), "2nd Serve Points Won": (0,100),
    "Break Points Saved": (0,100), "Winners": (0,200), "Unforced Errors": (0,200),
}

def parse_stat(raw):
    try: return float(str(raw or "").strip().rstrip("%"))
    except: return None

def resolve_winner(m):
    w = m.get("event_winner", "")
    p1, p2 = m.get("first_player_key"), m.get("second_player_key")
    if w == "First Player": return int(p1) if p1 else None
    if w == "Second Player": return int(p2) if p2 else None
    try:
        wk = int(w)
        if wk in (int(p1 or 0), int(p2 or 0)): return wk
    except: pass
    return None

def ensure_tables(conn, db_type):
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS at_matches (
        event_key BIGINT PRIMARY KEY, tournament_key INT, tournament_name TEXT,
        tour TEXT, surface TEXT, round TEXT, event_date TEXT,
        p1_key INT, p2_key INT, winner_key INT, p1_sets INT, p2_sets INT,
        score_json TEXT, elo_processed BOOLEAN DEFAULT FALSE, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS at_match_stats (
        id SERIAL PRIMARY KEY, event_key BIGINT, player_key INT,
        stat_name TEXT, stat_value TEXT, stat_numeric REAL,
        UNIQUE (event_key, player_key, stat_name))""")
    c.execute("""CREATE TABLE IF NOT EXISTS at_match_pbp (
        id SERIAL PRIMARY KEY, event_key BIGINT, set_num INT, game_num INT,
        point_num INT, server_key INT, point_winner_key INT, score TEXT,
        is_break_point BOOLEAN DEFAULT FALSE, is_set_point BOOLEAN DEFAULT FALSE,
        is_match_point BOOLEAN DEFAULT FALSE,
        UNIQUE (event_key, set_num, game_num, point_num))""")
    c.execute("""CREATE TABLE IF NOT EXISTS at_pull_errors (
        id SERIAL PRIMARY KEY, tournament_key INT, event_key BIGINT,
        error TEXT, created_at TEXT, resolved BOOLEAN DEFAULT FALSE)""")

def log_error(conn, db_type, tkey, ek, error):
    ph = placeholder(db_type)
    try:
        conn.cursor().execute(f"INSERT INTO at_pull_errors (tournament_key, event_key, error, created_at) VALUES ({ph},{ph},{ph},{ph})",
            (tkey, ek or 0, error[:500], datetime.utcnow().isoformat()))
    except: pass

def store_match(conn, db_type, m, tkey, tname, tour, surf):
    ph = placeholder(db_type)
    ek = int(m["event_key"])
    p1 = int(m["first_player_key"]) if m.get("first_player_key") else None
    p2 = int(m["second_player_key"]) if m.get("second_player_key") else None
    wk = resolve_winner(m)
    sets = parse_sets(m.get("event_final_result", "") or "")
    p1s = sum(1 for s in sets if s[0] > s[1])
    p2s = sum(1 for s in sets if s[1] > s[0])
    try:
        conn.cursor().execute(f"""INSERT INTO at_matches
            (event_key,tournament_key,tournament_name,tour,surface,round,event_date,
             p1_key,p2_key,winner_key,p1_sets,p2_sets,score_json,elo_processed,created_at)
            VALUES ({",".join([ph]*15)}) ON CONFLICT (event_key) DO NOTHING""",
            (ek,tkey,tname,tour,surf,m.get("tournament_round",""),m.get("event_date",""),
             p1,p2,wk,p1s,p2s,json.dumps(m.get("scores",[])),False,datetime.utcnow().isoformat()))
        return True
    except Exception as e:
        log_error(conn, db_type, tkey, ek, f"store_match:{e}"); return False

def store_stats(conn, db_type, ek, statistics):
    if not statistics: return 0
    ph = placeholder(db_type)
    cur = conn.cursor()
    rows = []
    for s in statistics:
        pk = s.get("player_key"); sn = s.get("stat_name",""); sv = s.get("stat_value","")
        if not pk or not sn: continue
        num = parse_stat(sv)
        if sn in STAT_RANGES and num is not None:
            lo, hi = STAT_RANGES[sn]
            if not (lo <= num <= hi): continue
        rows.append((ek, int(pk), sn, str(sv), num))
    if not rows: return 0
    try:
        from psycopg2.extras import execute_batch
        execute_batch(cur, f"INSERT INTO at_match_stats (event_key,player_key,stat_name,stat_value,stat_numeric) VALUES ({ph},{ph},{ph},{ph},{ph}) ON CONFLICT (event_key,player_key,stat_name) DO NOTHING",
            rows, page_size=500)
        return len(rows)
    except Exception as e:
        print(f"  Stats batch failed: {e}", flush=True)
        return 0

def store_pbp(conn, db_type, ek, pbp, p1, p2):
    if not pbp: return 0
    ph = placeholder(db_type)
    cur = conn.cursor()
    rows = []
    for game in pbp:
        sn = parse_set_num(game.get("set_number","0"))
        gn = int(game.get("number_game") or 0)
        sv = game.get("player_served","")
        sk = p1 if sv == "First Player" else (p2 if sv == "Second Player" else None)
        wv = game.get("serve_winner","")
        wk = p1 if wv == "First Player" else (p2 if wv == "Second Player" else None)
        for pt in (game.get("points") or []):
            pn = int(pt.get("number_point") or 0)
            rows.append((ek,sn,gn,pn,sk,wk,pt.get("score",""),
                         bool(pt.get("break_point")),bool(pt.get("set_point")),
                         bool(pt.get("match_point"))))
    if not rows: return 0
    try:
        from psycopg2.extras import execute_batch
        execute_batch(cur, f"""INSERT INTO at_match_pbp
            (event_key,set_num,game_num,point_num,server_key,point_winner_key,
             score,is_break_point,is_set_point,is_match_point)
            VALUES ({",".join([ph]*10)})
            ON CONFLICT (event_key,set_num,game_num,point_num) DO NOTHING""",
            rows, page_size=500)
        return len(rows)
    except Exception as e:
        print(f"  PBP batch failed: {e}", flush=True)
        return 0

def pull_tournament(conn, db_type, tournament, dry_run):
    tkey = tournament["tournament_key"]
    tname = tournament["tournament_name"]
    tour = tournament["event_type_type"]
    surf = tournament.get("tournament_sourface", "Hard") or "Hard"

    _state["tournament"] = f"{tname} ({tour})"

    data = api_fetch({"method": "get_fixtures", "tournament_key": str(tkey)})
    if not data or not data.get("result"):
        return {"matches": 0, "stats": 0, "pbp": 0, "errors": 1}

    fixtures = data["result"]
    if isinstance(fixtures, dict): fixtures = list(fixtures.values())
    finished = [m for m in fixtures if m.get("event_status") == "Finished"]

    mc = sc = pc = ec = 0
    for m in finished:
        ek = m.get("event_key")
        if not ek: continue
        ek = int(ek)
        ok, reason = validate_match(m)
        if not ok:
            if not dry_run: log_error(conn, db_type, tkey, ek, f"validation:{reason}")
            ec += 1; continue
        if dry_run:
            mc += 1
            sc += len(m.get("statistics") or [])
            pc += sum(len(g.get("points") or []) for g in (m.get("pointbypoint") or []))
            continue
        if not store_match(conn, db_type, m, tkey, tname, tour, surf):
            ec += 1; continue
        mc += 1
        sc += store_stats(conn, db_type, ek, m.get("statistics") or [])
        p1 = int(m["first_player_key"]) if m.get("first_player_key") else None
        p2 = int(m["second_player_key"]) if m.get("second_player_key") else None
        pc += store_pbp(conn, db_type, ek, m.get("pointbypoint") or [], p1, p2)
        _state["matches"] += 1
        _state["stats"] += sc
        _state["pbp"] += pc

    return {"matches": mc, "stats": sc, "pbp": pc, "errors": ec}

def show_stats(conn):
    cur = conn.cursor()
    for t in ["at_matches","at_match_stats","at_match_pbp","at_pull_errors"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t}: {cur.fetchone()[0]:,}")
        except Exception as e:
            print(f"  {t}: {e}")
    try:
        cur.execute("SELECT tour, COUNT(*) FROM at_matches GROUP BY tour ORDER BY COUNT(*) DESC")
        print("\nBy tour:")
        for r in cur.fetchall(): print(f"  {r[0]:35} {r[1]:,}")
    except: pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stats",   action="store_true")
    parser.add_argument("--tour",    type=str, default=None)
    parser.add_argument("--limit",   type=int, default=None)
    args = parser.parse_args()

    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}", flush=True)

    if args.stats:
        show_stats(conn); conn.close(); return

    if not args.dry_run:
        pass  # tables already exist, skip creation

    all_t = fetch_tournaments()
    tournaments = [t for t in all_t if t.get("event_type_type") in KEEP_TYPES
                   and t.get("tournament_name") not in SKIP_TOURNAMENTS]
    if args.tour:
        tournaments = [t for t in tournaments if t.get("event_type_type") == args.tour]
    if args.limit:
        tournaments = tournaments[:args.limit]
    print(f"Processing {len(tournaments)} tournaments", flush=True)

    # Start heartbeat
    stop = threading.Event()
    start_heartbeat(stop)
    _state["start"] = time.time()

    total = {"matches": 0, "stats": 0, "pbp": 0, "errors": 0}
    for i, t in enumerate(tournaments):
        result = pull_tournament(conn, db_type, t, dry_run=args.dry_run)
        for k in total: total[k] += result[k]
        _state.update({"matches": total["matches"], "stats": total["stats"],
                       "pbp": total["pbp"], "errors": total["errors"]})
        if (i+1) % 10 == 0 or (i+1) == len(tournaments):
            elapsed = time.time() - _state["start"]
            rate = (i+1) / elapsed if elapsed > 0 else 1
            eta = (len(tournaments) - i - 1) / rate
            print(f"  [{i+1:4}/{len(tournaments)}] matches:{total['matches']:,} stats:{total['stats']:,} pbp:{total['pbp']:,} errors:{total['errors']} | elapsed:{elapsed/60:.1f}m eta:{eta/60:.1f}m", flush=True)
        time.sleep(API_DELAY)

    stop.set()
    elapsed = time.time() - _state["start"]
    print(f"\nDone in {elapsed/60:.1f} minutes")
    print(f"  Matches: {total['matches']:,} | Stats: {total['stats']:,} | PBP: {total['pbp']:,} | Errors: {total['errors']}")
    if args.dry_run: print("\n[DRY RUN] Nothing saved.")
    else: print("\nau at_matches, at_match_stats, at_match_pbp populated")
    conn.close()

if __name__ == "__main__":
    main()
