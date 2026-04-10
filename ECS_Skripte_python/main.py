import db_4table
import hh_id_sort
import os
import remove_duplicates
import glob


def main():
    # ABSOLUTER PFAD zum Skript-Standort
    print("Init...")
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Gehe eine Ebene höher zum Projekt-Hauptverzeichnis
    project_folder = os.path.dirname(script_dir)

    exactAuswertung = os.path.join(project_folder, "ECS_EXACT")
    fuelAuswertung = os.path.join(project_folder, "ECS_FUEL")

    print(f"Creating folder in: {project_folder}")

    os.makedirs(exactAuswertung, exist_ok=True)
    os.makedirs(fuelAuswertung, exist_ok=True)

    print("Sorting files by household ID...")
    # First, we need to sort the files by their household ID. This is done in the hh_id_sort module.
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


if __name__ == "__main__":
    main()