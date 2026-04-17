import db_4table
import hh_id_sort
import os
import remove_duplicates
import glob
import cookingEvents_consPerHHID
import pandas as pd
from pprint import pprint


def execute_sql(conn, data_dir: str, name: str, filename: str):
    print(f"execute {name}...")
    cursor = conn.cursor()
    sql_file_path = os.path.join(data_dir, filename)
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_content = file.read()
    cursor.executescript(sql_content)
    conn.commit()


def export_conn_measurements_to_csv(conn, data_dir: str):
    print("Exporting conn_measurements to CSV...")
    df = pd.read_sql("SELECT * FROM conn_measurements", conn)
    csv_path = os.path.join(data_dir, "conn_measurements.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Saved to {csv_path}")


def main():
    # ABSOLUTER PFAD zum Skript-Standort
    print("Init...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, "Datenanalyse")

    # Gehe eine Ebene höher zum Projekt-Hauptverzeichnis
    project_folder = os.path.dirname(script_dir)

    exactAuswertung = os.path.join(project_folder, "ECS_EXACT")
    fuelAuswertung = os.path.join(project_folder, "ECS_FUEL")

    print(f"Creating folder in: {project_folder}")

    os.makedirs(exactAuswertung, exist_ok=True)
    os.makedirs(fuelAuswertung, exist_ok=True)

    print("Sorting files by household ID...")
    # First, we need to sort the files by their household ID (and filter out the files with error = 1). This is done in the hh_id_sort module.
    hh_id_sort.domainExpansion("FUEL", fuelAuswertung)
    hh_id_sort.domainExpansion("EXACT", exactAuswertung)

    # Second, we need to remove any duplicate files. This is done in the remove_duplicates module.
    print("Removing duplicates...")
    _, all_fuel_folders, all_exact_folders = remove_duplicates.get_paths()
    remove_duplicates.remove_duplicates(all_fuel_folders)
    remove_duplicates.remove_duplicates(all_exact_folders)

    print("Creating database...")
    # Create Database (the interconnected one)
    conn, all_fuel_files, all_exact_files, time_format = db_4table.get_paths()
    db_4table.process_csv_files(conn, all_fuel_files, time_format)
    db_4table.process_exact_files(conn, all_exact_files, time_format)

    execute_sql(conn, data_dir, "index.sql", "create_idx.sql")
    execute_sql(conn, data_dir, "conn_id.sql", "conn_id.sql")
    execute_sql(conn, data_dir, "del_exact_id.sql", "del_exact_id.sql")
    execute_sql(conn, data_dir, "conn_measurements.sql", "connect_measure.sql")

    export_conn_measurements_to_csv(conn, data_dir)

    print("now to step 11 and 12...")

    file_input, file_output, final_output = cookingEvents_consPerHHID.getPaths()
    min_consumption, max_invalid, temp_limit, fuel_lookback_min = (
        cookingEvents_consPerHHID.getValues()
    )

    # 1. Laden
    df = cookingEvents_consPerHHID.load_and_clean_data(file_input)

    # 2. Events finden
    cooking_events = cookingEvents_consPerHHID.identify_cooking_events(df, temp_limit)

    # 3. Validieren (Optimierte Methode statt iterrows)
    cooking_events = cookingEvents_consPerHHID.validate_events_optimized(
        cooking_events, df, fuel_lookback_min, min_consumption
    )
    valid_hhid = cookingEvents_consPerHHID.keep_valid_hhids(cooking_events, max_invalid)
    final_cons = cookingEvents_consPerHHID.calc_per_hhid(
        valid_hhid, df, cooking_events, final_output
    )

    cooking_events.to_csv(file_output, index=False, encoding="latin-1")
    final_cons.to_csv(final_output, index=False, encoding="latin-1")

    # 4. Speichern & Statistik

    valid_count = cooking_events["is_valid"].sum()
    print(
        f"Ergebnis: {len(cooking_events)} Events gefunden, davon {valid_count} valide."
    )
    pprint(valid_hhid)


if __name__ == "__main__":
    main()
