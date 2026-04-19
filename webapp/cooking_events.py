"""
Cooking Events und Fuel Consumption Berechnung.
Portierung von ECS_Skripte_python/cookingEvents_consPerHHID.py

Liest aus conn_measurements Tabelle und erstellt:
- cooking_events Tabelle
- fuel_cons_per_hhid Tabelle
"""

import os
import pandas as pd


MIN_CONSUMPTION = 0.025
MAX_INVALID = 1
TEMP_LIMIT = 40
FUEL_LOOKBACK_MIN = 90


def get_csv_path():
    """Gibt Pfad zur temp CSV-Datei zurück."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "temp_csv", "conn_measurements.csv")


def load_data_from_db(conn):
    """Lädt Daten direkt aus conn_measurements Tabelle (DB statt CSV)."""
    df = pd.read_sql("SELECT * FROM conn_measurements", conn)

    if df.empty:
        raise ValueError("conn_measurements table is empty")

    cols_to_fix = ["Weight (kg)", "consumption_kg", "temperature"]
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by=["exact_id", "timestamp"])

    df["temperature"] = df.groupby("exact_id")["temperature"].ffill(limit=3)
    return df


def identify_cooking_events(df, temp_limit):
    """Gruppiert Temperatur-Inseln zu Events."""
    df = df.copy()
    df["is_cooking"] = df["temperature"] >= temp_limit
    df["block_id"] = (df["is_cooking"] != df["is_cooking"].shift()).cumsum()

    events = (
        df[df["is_cooking"]]
        .groupby(["exact_id", "fuel_id", "block_id"])
        .agg(
            start_time=("timestamp", "min"),
            stop_time=("timestamp", "max"),
            avg_temp=("temperature", "mean"),
            fuel_type=("fuel_type", "first"),
            hhid=("hhid", "first"),
        )
        .reset_index()
    )

    events["duration_min"] = (
        events["stop_time"] - events["start_time"]
    ) / pd.Timedelta(minutes=1)
    events["duration_min"] = events["duration_min"].astype(float)
    events = events.drop(columns=["block_id"])
    events["event_id"] = range(len(events))
    return events


def validate_events_optimized(events, df, lookback_minutes, min_consumption):
    """Prüft effizient, ob vor dem Kochen Brennstoff verbraucht wurde."""
    df = df.copy()
    events = events.copy()

    fuel_usage = df[df["consumption_kg"] > min_consumption][
        ["fuel_id", "timestamp"]
    ].copy()

    fuel_usage = fuel_usage.sort_values("timestamp")
    events = events.sort_values("start_time")

    matches = pd.merge_asof(
        events,
        fuel_usage,
        left_on="start_time",
        right_on="timestamp",
        by="fuel_id",
        direction="backward",
        tolerance=pd.Timedelta(minutes=lookback_minutes),
    )

    events["is_valid"] = matches["timestamp"].notna().astype(int)
    return events


def keep_valid_hhids(cooking_events, max_invalid):
    """Filtert HHIDs mit zu vielen invaliden Events."""
    cooking_events = cooking_events.copy()
    cooking_events["hhid"] = cooking_events["hhid"].astype(str)
    cooking_events["date"] = cooking_events["start_time"].dt.date

    biomass_events = cooking_events[
        cooking_events["fuel_type"]
        .astype(str)
        .str.contains("charcoal|pellet", case=False, na=False)
    ].copy()

    day_stats = (
        biomass_events.groupby(["hhid", "date"])
        .agg(total_events=("is_valid", "count"), valid_events=("is_valid", "sum"))
        .reset_index()
    )

    day_stats["invalid_count"] = day_stats["total_events"] - day_stats["valid_events"]

    day_stats["clean_day"] = (day_stats["invalid_count"] <= max_invalid) & (
        day_stats["total_events"] > 0
    )

    clean_day_set = set(
        day_stats[day_stats["clean_day"]][["hhid", "date"]].itertuples(
            index=False, name=None
        )
    )

    return clean_day_set


def calc_per_hhid(valid_hhid, df, cooking_events):
    """Berechnet täglichen Verbrauch pro HHID."""
    df = df.copy()
    cooking_events = cooking_events.copy()

    df["hhid"] = df["hhid"].astype(str)
    df["date"] = df["timestamp"].dt.date

    if len(cooking_events) == 0:
        daily_duration = pd.DataFrame(columns=["hhid", "date", "cooking_duration_min"])
    else:
        cooking_events["date"] = cooking_events["start_time"].dt.date
        daily_duration = (
            cooking_events.groupby(["hhid", "date"])["duration_min"].sum().reset_index()
        )
    daily_duration.columns = ["hhid", "date", "cooking_duration_min"]

    df_clean = df[df.set_index(["hhid", "date"]).index.isin(valid_hhid)].copy()
    df_unique_fuel = df_clean.groupby(["fuel_id", "timestamp"]).first().reset_index()

    temp_cons = (
        df_unique_fuel.groupby(["hhid", "date", "fuel_type"])["consumption_kg"]
        .sum()
        .reset_index()
    )

    final_consumption = temp_cons.pivot_table(
        index=["hhid", "date"],
        columns="fuel_type",
        values="consumption_kg",
        fill_value=0,
    ).reset_index()

    rename_dict = {"charcoal": "charcoal_consumption", "pellet": "pellet_consumption"}
    final_consumption = final_consumption.rename(columns=rename_dict)

    final_consumption = pd.merge(
        final_consumption, daily_duration, on=["hhid", "date"], how="left"
    )
    return final_consumption


def create_cooking_events_table(conn, cooking_events):
    """Erstellt die cooking_events Tabelle in der Datenbank."""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS cooking_events")

    cursor.execute("""
        CREATE TABLE cooking_events (
            event_id INTEGER,
            exact_id TEXT,
            fuel_id TEXT,
            hhid TEXT,
            start_time TEXT,
            stop_time TEXT,
            avg_temp REAL,
            fuel_type TEXT,
            duration_min REAL,
            is_valid INTEGER
        )
    """)

    for _, row in cooking_events.iterrows():
        start_time = (
            row["start_time"].isoformat() if pd.notna(row["start_time"]) else None
        )
        stop_time = row["stop_time"].isoformat() if pd.notna(row["stop_time"]) else None

        cursor.execute(
            """
            INSERT INTO cooking_events 
            (event_id, exact_id, fuel_id, hhid, start_time, stop_time, avg_temp, fuel_type, duration_min, is_valid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["event_id"],
                row["exact_id"],
                row["fuel_id"],
                row["hhid"],
                start_time,
                stop_time,
                row["avg_temp"],
                row["fuel_type"],
                row["duration_min"],
                row["is_valid"],
            ],
        )

    conn.commit()


def create_fuel_cons_per_hhid_table(conn, final_consumption):
    """Erstellt die fuel_cons_per_hhid Tabelle in der Datenbank."""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS fuel_cons_per_hhid")

    cursor.execute("""
        CREATE TABLE fuel_cons_per_hhid (
            hhid TEXT,
            date TEXT,
            charcoal_consumption REAL,
            pellet_consumption REAL,
            cooking_duration_min REAL
        )
    """)

    for _, row in final_consumption.iterrows():
        date_str = row["date"].isoformat() if pd.notna(row["date"]) else None

        cursor.execute(
            """
            INSERT INTO fuel_cons_per_hhid 
            (hhid, date, charcoal_consumption, pellet_consumption, cooking_duration_min)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                row["hhid"],
                date_str,
                row.get("charcoal_consumption", 0),
                row.get("pellet_consumption", 0),
                row.get("cooking_duration_min"),
            ],
        )

    conn.commit()


def run_cooking_events_pipeline(conn) -> dict:
    """
    Führt die Cooking Events Pipeline aus.

    Args:
        conn: Datenbank-Verbindung

    Returns:
        Dict mit Pipeline-Status
    """
    from datetime import datetime

    try:
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Loading data from conn_measurements..."
        )
        df = load_data_from_db(conn)
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Loaded {len(df)} rows"
        )

        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Identifying cooking events (temp >= {TEMP_LIMIT})..."
        )
        cooking_events = identify_cooking_events(df, TEMP_LIMIT)
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Found {len(cooking_events)} cooking events"
        )

        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Validating events (lookback {FUEL_LOOKBACK_MIN}min)..."
        )
        cooking_events = validate_events_optimized(
            cooking_events, df, FUEL_LOOKBACK_MIN, MIN_CONSUMPTION
        )

        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Filtering valid HHIDs (max {MAX_INVALID} invalid/day)..."
        )
        valid_hhid = keep_valid_hhids(cooking_events, MAX_INVALID)
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] {len(valid_hhid)} valid (hhid, date) pairs"
        )

        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Calculating fuel consumption per HHID..."
        )
        final_cons = calc_per_hhid(valid_hhid, df, cooking_events)
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Calculated consumption for {len(final_cons)} rows"
        )

        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Creating tables in DB..."
        )
        create_cooking_events_table(conn, cooking_events)
        create_fuel_cons_per_hhid_table(conn, final_cons)
        print(
            f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] Tables created successfully"
        )

        valid_count = cooking_events["is_valid"].sum()

        return {
            "status": "success",
            "events_total": len(cooking_events),
            "events_valid": valid_count,
        }

    except FileNotFoundError as e:
        print(f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] ERROR: {e}")
        return {"status": "error", "reason": str(e)}
    except Exception as e:
        print(f"[DEBUG] [{datetime.now().strftime('%H:%M:%S')}] [Step11] ERROR: {e}")
        return {"status": "error", "reason": f"Pipeline error: {str(e)}"}
