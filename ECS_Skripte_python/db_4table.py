import sqlite3
import pandas as pd
import os
import glob
from datetime import datetime

# this file's purpose is to read all csv files and turn them in to a database, 
# so that we can easily access the data for our analysis.

current_folder = os.path.dirname(__file__)
project_folder = os.path.dirname(current_folder)
database_pfad = os.path.join(project_folder, "Datenanalyse" ,"ECS_Database.db")

# dircetor existance check and create if not exist
os.makedirs(os.path.dirname(database_pfad), exist_ok=True)

conn = sqlite3.connect(database_pfad)

search_path_fuel = os.path.join(project_folder, "ECS_FUEL", "**", "*.csv")
all_fuel_files = glob.glob(search_path_fuel, recursive=True)

search_path_exact = os.path.join(project_folder, "ECS_EXACT", "**", "*.csv")
all_exact_files = glob.glob(search_path_exact, recursive=True)

time_format = "%Y-%m-%d %H:%M:%S"
count_id = 0
fixed_names = ['timestamp', 'usage', 'gradient', 'temperature']


def process_csv_files(conn):
    """reads all csv files, extracts meta data and data, connects them and writes to database.
     IMPORTANT: this function also cuts the .csv files to one week of data gathered.
     This function is not modular
    """
    
    for file in all_fuel_files:
        global count_id
        

        try:            
                meta_df = pd.read_csv(file, skiprows=1, header=None, nrows=13, encoding='latin-1')

                hhid = str(meta_df.iloc[2, 1]).strip()
                sensor_id = str(meta_df.iloc[3, 1]).strip()
                fuel_type = str(meta_df.iloc[9, 1]).strip()
                start_time = meta_df.iloc[5,1]
                dt = datetime.strptime(start_time, time_format)

                data = {
                'hhid': [hhid],
                'sensor_id': [sensor_id],
                'fuel_type': [fuel_type],
                'start_time': [dt],
                'fuel_id': [count_id]
                }
        
                df_new = pd.DataFrame(data)

                df_new.to_sql("fuel_meta", conn, if_exists='append', index=False)
                

                # reading the rest and cut it to one week of data
                df = pd.read_csv(file, skiprows = 17, encoding='latin-1',  nrows = 10081)
                df['fuel_id'] = count_id
                
                

                df.to_sql('fuel_measurement', conn, if_exists='append', index=False)
                count_id += 1

           

        except Exception as e:
            print(f"Fehler beim Verarbeiten von {file}: {e}")

def process_exact_files(conn):
    """reads all csv files, extracts meta data and data, connects them and writes to database.
     IMPORTANT: this function also cuts the .csv files to one week of data gathered.
     This function is not modular
    """
    global count_id
    count_id = 0
    
    for file in all_exact_files:
        

        try:            
                meta_df = pd.read_csv(file, skiprows=1, header=None, nrows=13, encoding='latin-1')

                hhid = str(meta_df.iloc[2, 1]).strip()
                sensor_id = str(meta_df.iloc[3, 1]).strip()
                stove_name = str(meta_df.iloc[10, 1]).strip()
                max_temp = str(meta_df.iloc[8, 1]).strip()


                start_time = meta_df.iloc[5,1]
                dt = datetime.strptime(start_time, time_format)

                data = {
                'hhid': [hhid],
                'sensor_id': [sensor_id],
                'stove_name': [stove_name],
                'start_time': [dt],
                'exact_id': [count_id],
                'max_temp': [max_temp]
                }
        
                df_new = pd.DataFrame(data)

                df_new.to_sql("exact_meta", conn, if_exists='append', index=False)
                

                # reading the rest and cut it to one week of data
                df = pd.read_csv(file, skiprows = 17, encoding='latin-1',  nrows = 5041, names= fixed_names)
                df['exact_id'] = count_id
                
                

                df.to_sql('exact_measurement', conn, if_exists='append', index=False)
                count_id += 1

           

        except Exception as e:
            print(f"Fehler beim Verarbeiten von {file}: {e}")

process_csv_files(conn)
process_exact_files(conn)
# process_csv_files(all_exact_files, conn, "measurements")
conn.close()