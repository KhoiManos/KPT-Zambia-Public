"""
ETL-Modul für ECS Data Pipeline.
Verarbeitet FUEL und EXACT Sensor-CSV-Dateien und fügt sie in die Datenbank ein.

TURSO-HINWEIS:
==============
Um mit Turso zu arbeiten, müssen folgende Änderungen vorgenommen werden:

1. database.py anpassen:
   - Statt sqlite3: from cosmoz import connect
   - connection = await connect("libsql://deine-db.turso.io", auth_token="...")
   - Alle execute() werden zu: await connection.execute()
   - commit() bleibt: await connection.commit()

2. etl.py anpassen:
   - Alle Funktionen werden zu async def
   - conn.execute() -> await conn.execute()
   - conn.commit() -> await conn.commit()
   - get_next_id() wird async

3. app.py anpassen:
   - ETL-Funktionen mit await aufrufen

Die LOGIK bleibt identisch - nur die DB-Treiber ändern sich!
"""

import os
from datetime import datetime
from typing import Optional

import pandas as pd

from cooking_events import run_cooking_events_pipeline

MAX_FUEL_ROWS = 10081
MAX_EXACT_ROWS = 5041
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def detect_csv_type(filename: str, content: str = "") -> Optional[str]:
    """
    Erkennt ob CSV FUEL oder EXACT Sensor-Daten enthält.

    Args:
        filename: Dateiname (z.B. "FUEL_001.csv")
        content: Erste Zeilen des Inhalts (optional)

    Returns:
        "FUEL", "EXACT" oder None
    """
    filename_upper = filename.upper()

    if "FUEL" in filename_upper:
        return "FUEL"
    if "EXACT" in filename_upper:
        return "EXACT"

    if content:
        content_upper = content[:500].upper()
        if "FUEL" in content_upper:
            return "FUEL"
        if "EXACT" in content_upper:
            return "EXACT"

    return None


def get_next_id(conn, table: str, id_col: str) -> int:
    """
    Ermittelt die nächste ID für eine Tabelle.

    TURSO: Diese Funktion wird async:
        async def get_next_id(conn, table: str, id_col: str) -> int:
            result = await conn.execute(f"SELECT MAX({id_col})...")
            row = await result.fetchone()
    """
    cursor = conn.execute(f"SELECT MAX({id_col}) as max_val FROM {table}")
    row = cursor.fetchone()
    max_val = row["max_val"] if row and row["max_val"] is not None else 0
    return max_val + 1


def check_duplicate(
    conn, csv_type: str, hhid: str, sensor_id: str, start_time: str, end_time: str
) -> dict:
    """
    Prüft ob ein Dataset Duplikat ist basierend auf Zeitraum-Vergleich.

    Logik (identisch mit remove_duplicates.py):
    - File B ist Duplikat von A wenn: B.start >= A.start UND B.end <= A.end
    - Das längere Recording bleibt erhalten!

    Returns:
        dict mit:
            - skip_upload: True wenn neue Datei in bestehender enthalten ist
            - deleted_ids: IDs die gelöscht wurden (weil neue Datei sie enthält)
            - error: Fehlermeldung falls was schief geht
    """
    meta_table = "fuel_meta" if csv_type == "FUEL" else "exact_meta"
    meas_table = "fuel_measurement" if csv_type == "FUEL" else "exact_measurement"
    id_col = "fuel_id" if csv_type == "FUEL" else "exact_id"

    dt_new_start = datetime.strptime(start_time, TIME_FORMAT)
    dt_new_end = datetime.strptime(end_time, TIME_FORMAT)

    cursor = conn.execute(
        f"SELECT {id_col}, start_time, end_time FROM {meta_table} WHERE hhid = ? AND sensor_id = ?",
        [hhid, sensor_id],
    )
    rows = cursor.fetchall()

    skip_upload = False
    deleted_ids = []

    for row in rows:
        existing_id = row[0]
        if row[1] is None or row[2] is None:
            continue
        dt_existing_start = datetime.strptime(row[1], TIME_FORMAT)
        dt_existing_end = datetime.strptime(row[2], TIME_FORMAT)

        is_contained = (dt_new_start >= dt_existing_start) and (
            dt_new_end <= dt_existing_end
        )
        is_identical = (dt_new_start == dt_existing_start) and (
            dt_new_end == dt_existing_end
        )
        contains_existing = (dt_new_start <= dt_existing_start) and (
            dt_new_end >= dt_existing_end
        )

        if is_contained:
            if is_identical:
                skip_upload = True
            else:
                skip_upload = True
        elif contains_existing:
            deleted_ids.append(existing_id)

    if deleted_ids:
        for del_id in deleted_ids:
            conn.execute(f"DELETE FROM {meas_table} WHERE {id_col} = ?", [del_id])
            conn.execute(f"DELETE FROM {meta_table} WHERE {id_col} = ?", [del_id])

    return {"skip_upload": skip_upload, "deleted_ids": deleted_ids}


def process_fuel_csv(filepath: str, conn) -> dict:
    """
    ETL-Pipeline für FUEL Sensor CSVs.

    Liest Meta-Daten aus dem Header (Zeilen 2-16) und Messdaten (ab Zeile 18).
    Fügt Daten in fuel_meta und fuel_measurement Tabellen ein.

    TURSO: Diese Funktion wird async:
        async def process_fuel_csv(filepath: str, conn) -> dict:
            # Alle conn.execute() -> await conn.execute()
            # Alle conn.commit() -> await conn.commit()

    Args:
        filepath: Pfad zur CSV-Datei
        conn: SQLite/libSQL Verbindung

    Returns:
        Dict mit Status und Metadaten
    """
    meta_df = pd.read_csv(
        filepath, skiprows=1, header=None, nrows=13, encoding="latin-1"
    )

    hhid = str(meta_df.iloc[2, 1]).strip()
    sensor_id = str(meta_df.iloc[3, 1]).strip()
    fuel_type = str(meta_df.iloc[9, 1]).strip()
    start_time = str(meta_df.iloc[5, 1]).strip()
    end_time = str(meta_df.iloc[6, 1]).strip()

    if not start_time or start_time.lower() == "nan":
        raise ValueError(f"Missing start_time in {os.path.basename(filepath)}")
    if not end_time or end_time.lower() == "nan":
        raise ValueError(f"Missing end_time in {os.path.basename(filepath)}")

    dup_result = check_duplicate(conn, "FUEL", hhid, sensor_id, start_time, end_time)
    if dup_result["deleted_ids"]:
        conn.commit()
    if dup_result["skip_upload"]:
        return {
            "file": os.path.basename(filepath),
            "type": "FUEL",
            "status": "skipped",
            "reason": "Duplicate record (time range contained in existing record)",
            "hhid": hhid,
            "sensor_id": sensor_id,
        }

    dt = datetime.strptime(start_time, TIME_FORMAT)
    fuel_id = get_next_id(conn, "fuel_meta", "fuel_id")

    conn.execute(
        "INSERT INTO fuel_meta (hhid, sensor_id, fuel_type, start_time, fuel_id) VALUES (?, ?, ?, ?, ?)",
        [hhid, sensor_id, fuel_type, dt, fuel_id],
    )

    df = pd.read_csv(filepath, skiprows=17, encoding="latin-1", nrows=MAX_FUEL_ROWS)

    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO fuel_measurement (timestamp, usage, fuel_id) VALUES (?, ?, ?)",
            [row.iloc[0], row.iloc[1], fuel_id],
        )

    conn.commit()

    return {
        "file": os.path.basename(filepath),
        "type": "FUEL",
        "status": "success",
        "hhid": hhid,
        "sensor_id": sensor_id,
        "fuel_type": fuel_type,
        "rows_inserted": len(df),
    }


def process_exact_csv(filepath: str, conn) -> dict:
    """
    ETL-Pipeline für EXACT Sensor CSVs.

    Liest Meta-Daten aus dem Header (Zeilen 2-16) und Messdaten (ab Zeile 18).
    Fügt Daten in exact_meta und exact_measurement Tabellen ein.

    TURSO: Diese Funktion wird async:
        async def process_exact_csv(filepath: str, conn) -> dict:
            # Alle conn.execute() -> await conn.execute()
            # Alle conn.commit() -> await conn.commit()

    Args:
        filepath: Pfad zur CSV-Datei
        conn: SQLite/libSQL Verbindung

    Returns:
        Dict mit Status und Metadaten
    """
    fixed_names = ["timestamp", "usage", "gradient", "temperature"]
    meta_df = pd.read_csv(
        filepath, skiprows=1, header=None, nrows=13, encoding="latin-1"
    )

    hhid = str(meta_df.iloc[2, 1]).strip()
    sensor_id = str(meta_df.iloc[3, 1]).strip()
    stove_name = str(meta_df.iloc[10, 1]).strip()
    max_temp = str(meta_df.iloc[8, 1]).strip()
    start_time = str(meta_df.iloc[5, 1]).strip()
    end_time = str(meta_df.iloc[6, 1]).strip()

    if not start_time or start_time.lower() == "nan":
        raise ValueError(f"Missing start_time in {os.path.basename(filepath)}")
    if not end_time or end_time.lower() == "nan":
        raise ValueError(f"Missing end_time in {os.path.basename(filepath)}")

    dup_result = check_duplicate(conn, "EXACT", hhid, sensor_id, start_time, end_time)
    if dup_result["deleted_ids"]:
        conn.commit()
    if dup_result["skip_upload"]:
        return {
            "file": os.path.basename(filepath),
            "type": "EXACT",
            "status": "skipped",
            "reason": "Duplicate record (time range contained in existing record)",
            "hhid": hhid,
            "sensor_id": sensor_id,
        }

    dt = datetime.strptime(start_time, TIME_FORMAT)
    exact_id = get_next_id(conn, "exact_meta", "exact_id")

    conn.execute(
        "INSERT INTO exact_meta (hhid, sensor_id, stove_name, start_time, exact_id, max_temp) VALUES (?, ?, ?, ?, ?, ?)",
        [hhid, sensor_id, stove_name, dt, exact_id, max_temp],
    )

    df = pd.read_csv(
        filepath,
        skiprows=17,
        encoding="latin-1",
        nrows=MAX_EXACT_ROWS,
        names=fixed_names,
    )

    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO exact_measurement (timestamp, usage, gradient, temperature, exact_id) VALUES (?, ?, ?, ?, ?)",
            [row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3], exact_id],
        )

    conn.commit()

    return {
        "file": os.path.basename(filepath),
        "type": "EXACT",
        "status": "success",
        "hhid": hhid,
        "sensor_id": sensor_id,
        "stove_name": stove_name,
        "rows_inserted": len(df),
    }


def process_csv(filepath: str, conn) -> dict:
    """
    Haupteinstiegspunkt für CSV-Verarbeitung.
    Erkennt CSV-Typ und ruft entsprechende ETL-Funktion auf.

    Args:
        filepath: Pfad zur CSV-Datei
        conn: SQLite/libSQL Verbindung

    Returns:
        Ergebnis-Dict von process_fuel_csv oder process_exact_csv
    """
    csv_type = detect_csv_type(filepath)

    if csv_type == "FUEL":
        return process_fuel_csv(filepath, conn)
    elif csv_type == "EXACT":
        return process_exact_csv(filepath, conn)
    else:
        return {
            "file": os.path.basename(filepath),
            "status": "error",
            "reason": "Cannot detect CSV type. Filename must contain FUEL or EXACT.",
        }


def get_project_dirs():
    """
    Ermittelt die Projekt-Verzeichnisse.
    SQL-Dateien liegen in webapp/sql/.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(script_dir, "sql")
    temp_csv_dir = os.path.join(script_dir, "temp_csv")
    return sql_dir, temp_csv_dir


def execute_sql_file(conn, filename: str) -> dict:
    """
    Führt eine SQL-Datei aus.

    Args:
        conn: Datenbank-Verbindung
        filename: Name der SQL-Datei (ohne Pfad)

    Returns:
        Dict mit Status
    """
    data_dir, _ = get_project_dirs()
    sql_file_path = os.path.join(data_dir, filename)

    if not os.path.exists(sql_file_path):
        return {"status": "error", "reason": f"SQL file not found: {filename}"}

    cursor = conn.cursor()
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_content = file.read()
    cursor.executescript(sql_content)
    conn.commit()

    return {"status": "success", "file": filename}


def export_conn_measurements_csv(conn) -> dict:
    """
    Exportiert conn_measurements Tabelle nach CSV.
    Speichert im temporären temp_csv Ordner.

    Args:
        conn: Datenbank-Verbindung

    Returns:
        Dict mit Pfad zur exportierten Datei
    """
    _, temp_csv_dir = get_project_dirs()
    os.makedirs(temp_csv_dir, exist_ok=True)

    csv_path = os.path.join(temp_csv_dir, "conn_measurements.csv")

    try:
        df = pd.read_sql("SELECT * FROM conn_measurements", conn)
        df.to_csv(csv_path, index=False, encoding="utf-8")
        return {"status": "success", "path": csv_path}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def cleanup_temp_csv() -> None:
    """
    Löscht den temporären CSV-Ordner.
    """
    _, temp_csv_dir = get_project_dirs()
    csv_path = os.path.join(temp_csv_dir, "conn_measurements.csv")

    if os.path.exists(csv_path):
        os.remove(csv_path)

    if os.path.exists(temp_csv_dir):
        try:
            os.rmdir(temp_csv_dir)
        except OSError:
            pass


def run_pipeline_post_upload(conn) -> dict:
    """
    Führt die Post-Upload Pipeline aus (Schritte 6-11).
    - Indizes erstellen
    - IDs verbinden
    - Measurements verbinden
    - Export nach CSV
    - Cooking Events + Fuel Cons Berechnung

    Args:
        conn: Datenbank-Verbindung

    Returns:
        Dict mit Pipeline-Status
    """
    from datetime import datetime

    print(
        f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Starting pipeline (steps 6-11)..."
    )

    steps = [
        ("create_idx.sql", "Creating indices"),
        ("conn_id.sql", "Connecting IDs"),
        ("del_exact_id.sql", "Removing duplicate exact_ids"),
        ("connect_measure.sql", "Connecting measurements"),
    ]

    results = []

    for sql_file, desc in steps:
        print(f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Step: {desc}")
        result = execute_sql_file(conn, sql_file)
        results.append({"step": desc, "file": sql_file, "status": result["status"]})
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Completed: {desc} - Status: {result['status']}"
        )

        if result["status"] == "error":
            print(
                f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] ERROR in {desc}: {result.get('reason')}"
            )
            return {
                "status": "error",
                "step": desc,
                "reason": result.get("reason", "Unknown error"),
                "results": results,
            }

    print(
        f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Step: Export conn_measurements to CSV"
    )
    export_result = export_conn_measurements_csv(conn)
    results.append({"step": "Export to CSV", "status": export_result["status"]})
    print(
        f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Completed: Export - Status: {export_result['status']}"
    )

    if export_result["status"] == "error":
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] ERROR in Export: {export_result.get('reason')}"
        )
        return {
            "status": "error",
            "step": "Export to CSV",
            "reason": export_result.get("reason"),
            "results": results,
        }

    print(
        f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Step: Cooking events & fuel consumption (Step 11)"
    )
    cooking_result = run_cooking_events_pipeline(conn)
    results.append(
        {
            "step": "Cooking events & fuel consumption",
            "status": cooking_result["status"],
        }
    )
    print(
        f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Completed: Cooking events - Status: {cooking_result['status']}"
    )
    if cooking_result["status"] == "success":
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] Cooking events: {cooking_result.get('events_total', 0)} total, {cooking_result.get('events_valid', 0)} valid"
        )

    if cooking_result["status"] == "error":
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] ERROR in Cooking events: {cooking_result.get('reason')}"
        )
        return {
            "status": "error",
            "step": "Cooking events",
            "reason": cooking_result.get("reason"),
            "results": results,
        }

    return {
        "status": "success",
        "steps_completed": len(results),
        "results": results,
    }
