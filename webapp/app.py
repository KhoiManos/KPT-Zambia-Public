"""
ECS Data Pipeline — FastAPI Backend.

Dieses Modul enthält alle API-Routes für:
- HTML-Auslieferung (Startseite)
- Tabellenauflistung
- Schema-Informationen
- SQL-Query-Ausführung
- CSV-Upload mit ETL-Pipeline

TURSO-HINWEIS:
==============
Um mit Turso zu arbeiten, müssen folgende Änderungen vorgenommen werden:

1. database.py:
   - Statt sqlite3: from cosmoz import connect
   - async def get_db(): -> await connect(url, auth_token=token)

2. app.py:
   - Alle Route-Funktionen sind bereits async (bleibt so)
   - ETL-Aufrufe: process_fuel_csv() -> await process_fuel_csv()

3. etl.py:
   - Alle Funktionen zu async def machen
   - conn.execute() -> await conn.execute()

Die API-Routes bleiben IDENTISCH!
"""

import os
import uuid
import time
import re
from collections import deque
from datetime import datetime

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from database import get_db, init_db, reset_db
from etl import (
    detect_csv_type,
    process_fuel_csv,
    process_exact_csv,
    run_pipeline_post_upload,
    cleanup_temp_csv,
)

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    """Initialize and reset database on server startup."""
    init_db()
    reset_db()


STATIC_DIR = "static"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

MAX_RESULT_ROWS = 10_000

BLOCKED_PATTERNS = re.compile(
    r"\b(DROP|ALTER|TRUNCATE|CREATE|ATTACH|DETACH)\b", re.IGNORECASE
)
WRITE_PATTERNS = re.compile(r"\b(INSERT|UPDATE|DELETE|REPLACE)\b", re.IGNORECASE)

query_history = deque(maxlen=100)


@app.get("/")
async def root():
    """
    Startseite ausliefern.
    """
    return FileResponse("static/index.html")


@app.get("/api/tables")
async def list_tables():
    """
    Listet alle Tabellen mit Zeilenanzahl auf.

    TURSO: Keine Änderungen nötig - SQL ist identisch.
    """
    with get_db() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = []
        for row in cursor:
            name = row["name"]
            count_cursor = conn.execute(f'SELECT COUNT(*) as cnt FROM "{name}"')
            count = count_cursor.fetchone()["cnt"]
            tables.append({"name": name, "row_count": count})
    return {"tables": tables}


@app.get("/api/schema/{table_name}")
async def get_schema(table_name: str):
    """
    Gibt Schema-Info und Beispieldaten für eine Tabelle zurück.

    TURSO: Keine Änderungen nötig - PRAGMA funktioniert auch mit libSQL.
    """
    with get_db() as conn:
        cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
        columns = [
            {
                "cid": row["cid"],
                "name": row["name"],
                "type": row["type"],
                "notnull": row["notnull"],
            }
            for row in cursor
        ]

        count_cursor = conn.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        count = count_cursor.fetchone()["cnt"]

        sample_cursor = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 50')
        sample = [dict(row) for row in sample_cursor]

    return {
        "table": table_name,
        "columns": columns,
        "row_count": count,
        "sample": sample,
    }


class QueryRequest(BaseModel):
    sql: str
    read_only: bool = True


@app.post("/api/execute")
async def execute_query(req: QueryRequest):
    """
    Führt SQL Query aus mit Safety-Checks.

    Safety:
    - Blockiert gefährliche DDL-Statements (DROP, ALTER, etc.)
    - Read-only Mode verhindert INSERT/UPDATE/DELETE
    - Auto-LIMIT für große Ergebnismengen

    TURSO: Keine Änderungen nötig - SQL ist identisch.
    """
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Empty query")

    if BLOCKED_PATTERNS.search(sql):
        raise HTTPException(status_code=403, detail="Blocked SQL statement")

    if req.read_only and WRITE_PATTERNS.search(sql):
        raise HTTPException(status_code=403, detail="Read-only mode enabled")

    with get_db() as conn:
        start = time.time()
        cursor = conn.execute(sql)
        elapsed = round(time.time() - start, 4)

        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(row) for row in cursor]
        else:
            columns = []
            rows = []
            conn.commit()

        truncated = len(rows) >= MAX_RESULT_ROWS
        if truncated:
            rows = rows[:MAX_RESULT_ROWS]

        query_history.appendleft(
            {
                "sql": sql,
                "timestamp": datetime.now().isoformat(),
                "row_count": len(rows),
                "duration": elapsed,
                "status": "success",
            }
        )

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "duration_sec": elapsed,
        "truncated": truncated,
    }


@app.post("/api/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    """
    Upload Endpoint für CSV-Dateien.

    Workflow:
    1. Dateityp prüfen (nur CSV)
    2. FUEL/EXACT erkennen
    3. CSV temporär speichern
    4. ETL-Pipeline ausführen
    5. Temporäre Datei löschen

    TURSO: Hier werden die größten Änderungen nötig sein:
        - async with get_db() as conn:  ->  async with get_db() as conn:
        - process_fuel_csv(filepath, conn)  ->  await process_fuel_csv(filepath, conn)
    """
    results = []

    for file in files:
        if not file.filename or not file.filename.lower().endswith(".csv"):
            results.append(
                {
                    "file": file.filename or "unknown",
                    "status": "error",
                    "reason": "Not a CSV file",
                }
            )
            continue

        content = await file.read()
        content_str = content.decode("latin-1", errors="ignore")

        csv_type = detect_csv_type(file.filename, content_str)
        if csv_type is None:
            results.append(
                {
                    "file": file.filename,
                    "status": "error",
                    "reason": "Cannot detect CSV type. Filename must contain FUEL or EXACT.",
                }
            )
            continue

        unique_name = f"{uuid.uuid4().hex}_{file.filename}"
        filepath = os.path.join(UPLOAD_DIR, unique_name)

        with open(filepath, "wb") as f:
            f.write(content)

        try:
            with get_db() as conn:
                if csv_type == "FUEL":
                    result = process_fuel_csv(filepath, conn)
                else:
                    result = process_exact_csv(filepath, conn)
                results.append(result)
        except Exception as e:
            results.append({"file": file.filename, "status": "error", "reason": str(e)})
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    pipeline_result = None
    if results:
        with get_db() as conn:
            pipeline_result = run_pipeline_post_upload(conn)

    if pipeline_result:
        cleanup_temp_csv()

    return {"results": results, "pipeline": pipeline_result}


@app.get("/api/query-history")
async def get_query_history():
    """
    Gibt die letzten 100 Queries zurück.
    """
    return {"history": list(query_history)}


@app.post("/api/reset")
async def reset_database():
    """
    Reset database - delete all data but keep table structure.
    """
    reset_db()
    return {"status": "success", "message": "All data cleared"}
