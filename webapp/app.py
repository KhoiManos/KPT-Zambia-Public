"""
ECS Data Pipeline — FastAPI Backend
Provides CSV upload/ETL, database exploration, and SQL query endpoints.
"""

import os
import re
import sqlite3
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_DIR, "Datenanalyse", "ECS_Database.db")
STATIC_DIR = os.path.join(BASE_DIR, "static")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="ECS Data Pipeline", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# In-memory query history (last 100)
query_history: deque = deque(maxlen=100)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Dangerous SQL patterns
BLOCKED_PATTERNS = re.compile(
    r"\b(DROP|ALTER|TRUNCATE|CREATE|ATTACH|DETACH|PRAGMA\s+(?!table_info|database_list))\b",
    re.IGNORECASE,
)
WRITE_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|REPLACE)\b",
    re.IGNORECASE,
)

MAX_RESULT_ROWS = 10_000
QUERY_TIMEOUT_SEC = 30


def get_db():
    """Return a new SQLite connection."""
    conn = sqlite3.connect(DB_PATH, timeout=QUERY_TIMEOUT_SEC)
    conn.row_factory = sqlite3.Row
    return conn


def detect_csv_type(filepath: str) -> Optional[str]:
    """Auto-detect whether a CSV is FUEL or EXACT based on content."""
    basename = os.path.basename(filepath).upper()
    if "FUEL" in basename:
        return "FUEL"
    if "EXACT" in basename:
        return "EXACT"

    # Fallback: peek at the sensor type row
    try:
        df = pd.read_csv(filepath, skiprows=1, header=None, nrows=6, encoding="latin-1")
        sensor_type = str(df.iloc[4, 1]).strip().upper()
        if "FUEL" in sensor_type:
            return "FUEL"
        if "EXACT" in sensor_type:
            return "EXACT"
    except Exception:
        pass
    return None


def get_next_id(conn: sqlite3.Connection, table: str, id_col: str) -> int:
    """Get the next auto-increment-style ID for a table."""
    cur = conn.execute(f"SELECT MAX({id_col}) FROM {table}")
    row = cur.fetchone()
    max_val = row[0] if row and row[0] is not None else -1
    return max_val + 1


def check_duplicate(conn: sqlite3.Connection, csv_type: str, hhid: str, sensor_id: str, start_time: str, end_time: str) -> bool:
    """Check if a record with overlapping time range already exists."""
    meta_table = "fuel_meta" if csv_type == "FUEL" else "exact_meta"
    try:
        cur = conn.execute(
            f"SELECT COUNT(*) FROM {meta_table} WHERE hhid = ? AND sensor_id = ? AND start_time = ?",
            (hhid, sensor_id, start_time),
        )
        return cur.fetchone()[0] > 0
    except Exception:
        return False


def process_fuel_csv(filepath: str, conn: sqlite3.Connection) -> dict:
    """ETL pipeline for a single FUEL CSV."""
    meta_df = pd.read_csv(filepath, skiprows=1, header=None, nrows=13, encoding="latin-1")

    hhid = str(meta_df.iloc[2, 1]).strip()
    sensor_id = str(meta_df.iloc[3, 1]).strip()
    fuel_type = str(meta_df.iloc[9, 1]).strip()
    start_time = str(meta_df.iloc[5, 1]).strip()
    end_time = str(meta_df.iloc[6, 1]).strip()

    # Check duplicates
    if check_duplicate(conn, "FUEL", hhid, sensor_id, start_time, end_time):
        return {
            "file": os.path.basename(filepath),
            "type": "FUEL",
            "status": "skipped",
            "reason": "Duplicate record (same hhid, sensor_id, start_time)",
            "hhid": hhid,
            "sensor_id": sensor_id,
        }

    dt = datetime.strptime(start_time, TIME_FORMAT)
    fuel_id = get_next_id(conn, "fuel_meta", "fuel_id")

    # Insert meta
    meta_data = {
        "hhid": [hhid],
        "sensor_id": [sensor_id],
        "fuel_type": [fuel_type],
        "start_time": [dt],
        "fuel_id": [fuel_id],
    }
    pd.DataFrame(meta_data).to_sql("fuel_meta", conn, if_exists="append", index=False)

    # Insert measurement data (cut to 1 week = 10081 rows)
    df = pd.read_csv(filepath, skiprows=17, encoding="latin-1", nrows=10081)
    df["fuel_id"] = fuel_id
    df.to_sql("fuel_measurement", conn, if_exists="append", index=False)

    return {
        "file": os.path.basename(filepath),
        "type": "FUEL",
        "status": "success",
        "hhid": hhid,
        "sensor_id": sensor_id,
        "fuel_type": fuel_type,
        "rows_inserted": len(df),
    }


def process_exact_csv(filepath: str, conn: sqlite3.Connection) -> dict:
    """ETL pipeline for a single EXACT CSV."""
    fixed_names = ["timestamp", "usage", "gradient", "temperature"]
    meta_df = pd.read_csv(filepath, skiprows=1, header=None, nrows=14, encoding="latin-1")

    hhid = str(meta_df.iloc[2, 1]).strip()
    sensor_id = str(meta_df.iloc[3, 1]).strip()
    stove_name = str(meta_df.iloc[10, 1]).strip()
    max_temp = str(meta_df.iloc[8, 1]).strip()
    start_time = str(meta_df.iloc[5, 1]).strip()
    end_time = str(meta_df.iloc[6, 1]).strip()

    # Check duplicates
    if check_duplicate(conn, "EXACT", hhid, sensor_id, start_time, end_time):
        return {
            "file": os.path.basename(filepath),
            "type": "EXACT",
            "status": "skipped",
            "reason": "Duplicate record (same hhid, sensor_id, start_time)",
            "hhid": hhid,
            "sensor_id": sensor_id,
        }

    dt = datetime.strptime(start_time, TIME_FORMAT)
    exact_id = get_next_id(conn, "exact_meta", "exact_id")

    # Insert meta
    meta_data = {
        "hhid": [hhid],
        "sensor_id": [sensor_id],
        "stove_name": [stove_name],
        "start_time": [dt],
        "exact_id": [exact_id],
        "max_temp": [max_temp],
    }
    pd.DataFrame(meta_data).to_sql("exact_meta", conn, if_exists="append", index=False)

    # Insert measurement data (cut to 1 week = 5041 rows)
    df = pd.read_csv(filepath, skiprows=17, encoding="latin-1", nrows=5041, names=fixed_names)
    df["exact_id"] = exact_id
    df.to_sql("exact_measurement", conn, if_exists="append", index=False)

    return {
        "file": os.path.basename(filepath),
        "type": "EXACT",
        "status": "success",
        "hhid": hhid,
        "sensor_id": sensor_id,
        "stove_name": stove_name,
        "rows_inserted": len(df),
    }


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class QueryRequest(BaseModel):
    sql: str
    read_only: bool = True


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/api/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    """Upload one or more CSV files and run the ETL pipeline."""
    results = []
    conn = sqlite3.connect(DB_PATH, timeout=QUERY_TIMEOUT_SEC)

    try:
        for upload in files:
            if not upload.filename or not upload.filename.lower().endswith(".csv"):
                results.append({
                    "file": upload.filename or "unknown",
                    "status": "error",
                    "reason": "Not a CSV file",
                })
                continue

            # Save to temp
            temp_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex}_{upload.filename}")
            content = await upload.read()
            with open(temp_path, "wb") as f:
                f.write(content)

            csv_type = detect_csv_type(temp_path)
            if csv_type is None:
                results.append({
                    "file": upload.filename,
                    "status": "error",
                    "reason": "Cannot detect CSV type (FUEL/EXACT). Filename must contain FUEL or EXACT.",
                })
                os.remove(temp_path)
                continue

            try:
                if csv_type == "FUEL":
                    result = process_fuel_csv(temp_path, conn)
                else:
                    result = process_exact_csv(temp_path, conn)
                conn.commit()
                results.append(result)
            except Exception as e:
                conn.rollback()
                results.append({
                    "file": upload.filename,
                    "status": "error",
                    "reason": str(e),
                })
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
    finally:
        conn.close()

    return {"results": results}


@app.get("/api/tables")
async def list_tables():
    """List all tables with row counts."""
    conn = get_db()
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = []
        for row in cur.fetchall():
            name = row["name"]
            count_cur = conn.execute(f'SELECT COUNT(*) as cnt FROM "{name}"')
            count = count_cur.fetchone()["cnt"]
            tables.append({"name": name, "row_count": count})
        return {"tables": tables}
    finally:
        conn.close()


@app.get("/api/schema/{table_name}")
async def get_schema(table_name: str):
    """Return column info and sample data for a table."""
    conn = get_db()
    try:
        # Validate table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        # Column info
        col_cur = conn.execute(f'PRAGMA table_info("{table_name}")')
        columns = [
            {"cid": r["cid"], "name": r["name"], "type": r["type"], "notnull": r["notnull"]}
            for r in col_cur.fetchall()
        ]

        # Row count
        count_cur = conn.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        row_count = count_cur.fetchone()["cnt"]

        # Sample rows
        sample_cur = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 50')
        col_names = [desc[0] for desc in sample_cur.description]
        sample = [dict(zip(col_names, row)) for row in sample_cur.fetchall()]

        return {
            "table": table_name,
            "columns": columns,
            "row_count": row_count,
            "sample": sample,
        }
    finally:
        conn.close()


@app.post("/api/execute")
async def execute_query(req: QueryRequest):
    """Execute a SQL query with safety limits."""
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Empty query")

    # Safety checks
    if BLOCKED_PATTERNS.search(sql):
        raise HTTPException(
            status_code=403,
            detail="Blocked: DDL statements (DROP, ALTER, CREATE, TRUNCATE) are not allowed.",
        )
    if req.read_only and WRITE_PATTERNS.search(sql):
        raise HTTPException(
            status_code=403,
            detail="Read-only mode is enabled. Disable it to run INSERT/UPDATE/DELETE queries.",
        )

    conn = get_db()
    start = time.time()
    try:
        # Add LIMIT if not present for SELECT queries
        sql_upper = sql.upper().strip().rstrip(";")
        if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
            sql_exec = f"{sql.rstrip(';')} LIMIT {MAX_RESULT_ROWS}"
        else:
            sql_exec = sql

        cur = conn.execute(sql_exec)
        elapsed = round(time.time() - start, 4)

        if cur.description:
            col_names = [desc[0] for desc in cur.description]
            rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
        else:
            col_names = []
            rows = []
            conn.commit()

        entry = {
            "sql": sql,
            "timestamp": datetime.now().isoformat(),
            "rows_returned": len(rows),
            "duration_sec": elapsed,
            "status": "success",
        }
        query_history.appendleft(entry)

        return {
            "columns": col_names,
            "rows": rows,
            "row_count": len(rows),
            "duration_sec": elapsed,
            "truncated": len(rows) >= MAX_RESULT_ROWS,
        }
    except sqlite3.OperationalError as e:
        elapsed = round(time.time() - start, 4)
        entry = {
            "sql": sql,
            "timestamp": datetime.now().isoformat(),
            "rows_returned": 0,
            "duration_sec": elapsed,
            "status": "error",
            "error": str(e),
        }
        query_history.appendleft(entry)
        raise HTTPException(status_code=400, detail=f"SQL Error: {e}")
    finally:
        conn.close()


@app.get("/api/query-history")
async def get_query_history():
    return {"history": list(query_history)}
