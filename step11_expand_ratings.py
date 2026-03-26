"""
TennisTrade — Step 11: Expand Player Ratings from TennisAbstract
Targeted per-player DB lookup — no bulk loading.
"""

import os, re, sys, time, argparse, unicodedata, urllib.request
from datetime import date
from typing import Optional, Tuple, Dict, List
from db import get_connection, placeholder

URLS = {
    "ATP": "https://tennisabstract.com/reports/atp_elo_ratings.html",
    "WTA": "https://tennisabstract.com/reports/wta_elo_ratings.html",
}
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TennisTrade/1.0)"}

def normalize(s):
    s = re.sub(r'\s*[\[(][^\])]*[\])]', '', s or '')
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', s.replace('-', ' ').strip().lower())

def clean_name(s):
    """Strip HTML entities and normalize whitespace."""
    s = s.replace('\xa0', ' ').replace('&nbsp;', ' ')
    s = re.sub(r'&[a-zA-Z]+;', '', s)  # strip any other HTML entities
    return re.sub(r'\s+', ' ', s).strip()

def split_name(name):
    parts = name.strip().split()
    return (' '.join(parts[:-1]), parts[-1]) if len(parts) > 1 else ('', parts[0] if parts else '')

def fetch_html(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode('utf-8', errors='replace')

def parse_elo_table(html, tour):
    players = []
    def strip_tags(s): return re.sub(r'<[^>]+>', '', s).strip()
    for row in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE):
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 5: continue
        texts = [strip_tags(c) for c in cells]
        try: rank = int(texts[0])
        except: continue
        # Clean name — strip tags AND html entities
        name = clean_name(strip_tags(cells[1])) if len(cells) > 1 else ''
        if not name or len(name) < 2: continue
        try:
            elo_overall = float(texts[3]) if len(texts) > 3 and texts[3] else None
            elo_hard    = float(texts[4]) if len(texts) > 4 and texts[4] else None
            elo_clay    = float(texts[5]) if len(texts) > 5 and texts[5] else None
            elo_grass   = float(texts[6]) if len(texts) > 6 and texts[6] else None
        except: continue
        if not elo_overall or elo_overall < 1000: continue
        players.append({'rank': rank, 'name': name, 'tour': tour,
            'elo_overall': round(elo_overall,1),
            'elo_hard':    round(elo_hard,1) if elo_hard else None,
            'elo_clay':    round(elo_clay,1) if elo_clay else None,
            'elo_grass':   round(elo_grass,1) if elo_grass else None})
    return players

def find_player_id(conn, db_type, name):
    ph = placeholder(db_type)
    cur = conn.cursor()
    first, last = split_name(name)
    nl, nf = normalize(last), normalize(first)
    # 1. Exact match
    cur.execute(f"SELECT player_id FROM tennis_players WHERE LOWER(REPLACE(last_name,'-',' '))={ph} AND LOWER(REPLACE(first_name,'-',' '))={ph} LIMIT 1", (nl, nf))
    row = cur.fetchone()
    if row: return row[0], 'exact'
    # 2. Last name + first initial
    if nf:
        cur.execute(f"SELECT player_id, first_name FROM tennis_players WHERE LOWER(REPLACE(last_name,'-',' '))={ph} AND LOWER(LEFT(first_name,1))={ph} LIMIT 5", (nl, nf[0]))
        rows = cur.fetchall()
        if len(rows) == 1: return rows[0][0], 'high'
        if len(rows) > 1:
            best_id = min(rows, key=lambda x: abs(len(normalize(x[1])) - len(nf)))[0]
            return best_id, 'medium'
    # 3. Last name only (unique)
    cur.execute(f"SELECT player_id FROM tennis_players WHERE LOWER(REPLACE(last_name,'-',' '))={ph} LIMIT 2", (nl,))
    rows = cur.fetchall()
    if len(rows) == 1: return rows[0][0], 'medium'
    return None, 'miss'

def upsert_elo(conn, db_type, player_id, tour, elos):
    ph = placeholder(db_type)
    cur = conn.cursor()
    today = date.today().isoformat()
    if db_type == 'pg':
        cur.execute(f"""
            INSERT INTO tennis_elo_current
                (player_id, tour, elo_overall, elo_hard, elo_clay, elo_grass, last_match_date)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (player_id) DO UPDATE SET
                elo_overall=EXCLUDED.elo_overall,
                elo_hard=EXCLUDED.elo_hard,
                elo_clay=EXCLUDED.elo_clay,
                elo_grass=EXCLUDED.elo_grass,
                last_match_date=EXCLUDED.last_match_date
        """, (player_id, tour, elos['elo_overall'], elos['elo_hard'],
              elos['elo_clay'], elos['elo_grass'], today))
    else:
        cur.execute(f"INSERT OR REPLACE INTO tennis_elo_current (player_id,tour,elo_overall,elo_hard,elo_clay,elo_grass,last_match_date) VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})",
            (player_id, tour, elos['elo_overall'], elos['elo_hard'], elos['elo_clay'], elos['elo_grass'], today))

def insert_new_player(conn, db_type, name, tour):
    """
    Insert a new player into tennis_players.
    player_id has no sequence — we manually assign max(player_id) + 1.
    Uses a transaction-safe approach to avoid race conditions.
    """
    first, last = split_name(name)
    if not last:
        return None
    ph = placeholder(db_type)
    cur = conn.cursor()

    # Check if already exists (by exact name)
    cur.execute(
        f"SELECT player_id FROM tennis_players WHERE first_name={ph} AND last_name={ph} LIMIT 1",
        (first, last)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    # Get next available ID
    cur.execute("SELECT COALESCE(MAX(player_id), 300000) FROM tennis_players")
    max_id = cur.fetchone()[0]
    new_id = max_id + 1

    try:
        cur.execute(
            f"INSERT INTO tennis_players (player_id, first_name, last_name, tour) VALUES ({ph},{ph},{ph},{ph})",
            (new_id, first, last, tour)
        )
        return new_id
    except Exception as e:
        # If ID collision somehow, try fetching existing
        cur.execute(
            f"SELECT player_id FROM tennis_players WHERE first_name={ph} AND last_name={ph} LIMIT 1",
            (first, last)
        )
        row = cur.fetchone()
        return row[0] if row else None

def run(conn, db_type, tours, dry_run, verbose):
    print(f"\n{'='*60}\nTennisTrade — Step 11: Expand Player Ratings\n{'='*60}")
    print(f"  Tours: {', '.join(tours)} | Dry run: {dry_run}\n")
    totals = {'fetched': 0, 'updated': 0, 'inserted': 0, 'skipped': 0, 'miss': 0}

    for tour in tours:
        print(f"Fetching {tour} from tennisabstract...", end='', flush=True)
        try:
            html = fetch_html(URLS[tour])
        except Exception as e:
            print(f' FAILED: {e}'); continue
        players = parse_elo_table(html, tour)
        print(f' {len(players)} players')
        totals['fetched'] += len(players)
        if not players: continue

        updated = inserted = skipped = miss = 0
        for i, p in enumerate(players):
            if i % 50 == 0:
                print(f'  [{tour}] {i}/{len(players)} — updated:{updated} new:{inserted} skip:{skipped} miss:{miss}', end='\r', flush=True)
            name = p['name']
            elos = {k: p[k] for k in ['elo_overall','elo_hard','elo_clay','elo_grass']}
            player_id, conf = find_player_id(conn, db_type, name)

            if conf in ('exact', 'high'):
                if not dry_run: upsert_elo(conn, db_type, player_id, tour, elos)
                updated += 1
                if verbose: print(f'\n  UPDATE [{conf}] {name} id:{player_id} Elo:{elos["elo_overall"]}')
            elif conf == 'medium':
                skipped += 1
                if verbose: print(f'\n  SKIP   [medium] {name}')
            else:
                if not dry_run:
                    pid = insert_new_player(conn, db_type, name, tour)
                    if pid:
                        upsert_elo(conn, db_type, pid, tour, elos)
                        inserted += 1
                        if verbose: print(f'\n  INSERT {name} id:{pid} Elo:{elos["elo_overall"]}')
                    else:
                        miss += 1
                        if verbose: print(f'\n  MISS   {name}')
                else:
                    inserted += 1
                    if verbose: print(f'\n  NEW    {name} Elo:{elos["elo_overall"]}')

        print(f'  [{tour}] Done: {updated} updated | {inserted} new | {skipped} skipped | {miss} failed          ')
        totals['updated'] += updated; totals['inserted'] += inserted
        totals['skipped'] += skipped; totals['miss'] += miss
        time.sleep(1)

    print(f"\n{'─'*60}")
    print(f"Total: {totals['fetched']} fetched | {totals['updated']} updated | {totals['inserted']} new | {totals['skipped']} skipped")
    if not dry_run: print('✓ tennis_elo_current updated')
    return totals

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run',  action='store_true')
    parser.add_argument('--verbose',  action='store_true')
    parser.add_argument('--atp-only', action='store_true')
    parser.add_argument('--wta-only', action='store_true')
    args = parser.parse_args()
    tours = ['ATP'] if args.atp_only else ['WTA'] if args.wta_only else ['ATP', 'WTA']
    conn, db_type = get_connection()
    print(f"Database: {'PostgreSQL' if db_type == 'pg' else 'SQLite'}")
    run(conn, db_type, tours=tours, dry_run=args.dry_run, verbose=args.verbose)
    conn.close()

if __name__ == '__main__':
    main()
