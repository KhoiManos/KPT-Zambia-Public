import sqlite3
import pandas as pd
import os
import glob
from datetime import datetime

current_folder = os.path.dirname(__file__)
project_folder = os.path.dirname(current_folder)
database_pfad = os.path.join(project_folder, "Datenanalyse" ,"ECS_Database.db")

conn = sqlite3.connect(database_pfad)

fuel_path = os.path.join(project_folder, "ECS_FUEL", "**")
all_fuel_folders = glob.glob(fuel_path, recursive=True)

search_path_exact = os.path.join(project_folder, "ECS_EXACT", "**")
all_exact_folders = glob.glob(search_path_exact, recursive=True)

time_format = "%Y-%m-%d %H:%M:%S"

def remove_duplicates(all_folders):
    """removes all files where its timestamp is fully covered by another file 
    with the same sensor_id and hh_id.
    """
    delete_count = 0

    for folder in all_folders:
        if not os.path.isdir(folder):
            continue
            
        csv_files = glob.glob(os.path.join(folder, "*.csv"))
        
        # first read all files
        file_data = []
        for file in csv_files:
            try:
                
                df = pd.read_csv(file, skiprows=1, header=None, nrows=13, encoding='latin-1')
                hhid = str(df.iloc[2, 1]).strip()
                sensor_id = str(df.iloc[3, 1]).strip()
                begin_stamp = df.iloc[5, 1]
                end_stamp = df.iloc[6, 1]
                
                dt_start = datetime.strptime(begin_stamp, time_format)
                dt_end = datetime.strptime(end_stamp, time_format)
                
                # Speichern als Dictionary in unserer Liste
                file_data.append({
                    "pfad": file, 
                    "hhid": hhid,
                    "sensor_id": sensor_id,
                    "start": dt_start, 
                    "end": dt_end
                })
            except Exception as e:
                print(f"No good at {file}: {e}")

        # comparison and store duplicates
        files_to_delete = set() 
        
        for i in range(len(file_data)):
            for j in range(len(file_data)):
                # no comparison with itself and only compare files with the same hhid and sensor_id
                if (i == j) or (file_data[i]["hhid"] != file_data[j]["hhid"]) or (file_data[i]["sensor_id"] != file_data[j]["sensor_id"]): 
                    continue
                
                datei_A = file_data[i]
                datei_B = file_data[j]
                
                is_within = (datei_B["start"] >= datei_A["start"]) and (datei_B["end"] <= datei_A["end"])
                
                is_identical = (datei_B["start"] == datei_A["start"]) and (datei_B["end"] == datei_A["end"])

                if is_within:
                    if is_identical:
                        # if the files are identical, we only keep one of them (the one with the smaller index)
                        if j > i:
                            files_to_delete.add(datei_B["pfad"])
                    else:
                        files_to_delete.add(datei_B["pfad"])

        # delete the duplicates
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                os.remove(file_path)
                delete_count += 1
                print(f"kapow: {os.path.basename(file_path)}")
                print(f"+ deleted files: {delete_count}")

        print(f"{delete_count}")


remove_duplicates(all_fuel_folders)
remove_duplicates(all_exact_folders)

