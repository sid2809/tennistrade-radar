"""
TennisTrade — Database Connection Module
Connects to the same Railway PostgreSQL instance as CricTrade.
"""

import os
import sys
import argparse

def get_connection(args=None):
    """
    Returns (connection, db_type) tuple.
    
    Priority:
    1. DATABASE_URL env var (Railway sets this automatically)
    2. --db-url CLI argument
    3. --local flag → SQLite at data/tennistrade.db
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db-url", type=str, default=None)
    parser.add_argument("--local", action="store_true")
    parsed, _ = parser.parse_known_args(args or sys.argv[1:])

    db_url = os.environ.get("DATABASE_URL") or parsed.db_url

    if db_url and not parsed.local:
        import psycopg2
        # Railway sometimes uses postgres:// but psycopg2 needs postgresql://
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        return conn, "pg"
    else:
        import sqlite3
        os.makedirs("data", exist_ok=True)
        conn = sqlite3.connect("data/tennistrade.db")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn, "sqlite"


def placeholder(db_type):
    """Returns the correct placeholder for the DB type."""
    return "%s" if db_type == "pg" else "?"


def serial_pk(db_type):
    """Returns correct auto-increment PK syntax."""
    if db_type == "pg":
        return "SERIAL PRIMARY KEY"
    return "INTEGER PRIMARY KEY AUTOINCREMENT"


def on_conflict_ignore(db_type):
    """Returns correct upsert-ignore syntax."""
    if db_type == "pg":
        return "ON CONFLICT DO NOTHING"
    return "OR IGNORE"


def batch_insert(cursor, table, columns, rows, db_type, conflict_cols=None):
    """
    Insert rows in batches. Ignores duplicates if conflict_cols provided.
    
    Args:
        cursor: DB cursor
        table: table name
        columns: list of column names
        rows: list of tuples
        db_type: 'pg' or 'sqlite'
        conflict_cols: list of columns for ON CONFLICT (pg) / OR IGNORE (sqlite)
    """
    if not rows:
        return 0

    ph = placeholder(db_type)
    cols = ", ".join(columns)
    vals = ", ".join([ph] * len(columns))

    if conflict_cols and db_type == "pg":
        conflict = ", ".join(conflict_cols)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT ({conflict}) DO NOTHING"
    elif conflict_cols and db_type == "sqlite":
        sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({vals})"
    else:
        sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"

    batch_size = 500
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        if db_type == "pg":
            from psycopg2.extras import execute_batch
            execute_batch(cursor, sql, batch, page_size=batch_size)
        else:
            cursor.executemany(sql, batch)
        inserted += len(batch)

    return inserted
