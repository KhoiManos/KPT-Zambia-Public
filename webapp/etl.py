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
