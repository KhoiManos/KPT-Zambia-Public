import os
import sqlite3
from contextlib import contextmanager

DB_PATH = "ecs.db"
SCHEMA_PATH = "schema.sql"


def init_db():
    """Create tables from schema.sql if they don't exist."""
    if not os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
                conn.executescript(f.read())
            conn.commit()
            print(f"Database created at {DB_PATH}")
        finally:
            conn.close()


def reset_db():
    """Delete all data from tables but keep table structure."""
    tables = [
        "fuel_measurement",
        "fuel_meta",
        "exact_measurement",
        "exact_meta",
        "conn_ids",
        "conn_measurements",
        "fuel_cons_per_hhid",
        "cooking_events",
    ]
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        for table in tables:
            try:
                conn.execute(f"DELETE FROM {table}")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
