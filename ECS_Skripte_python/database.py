import sqlite3
import pandas as pd
import os
import glob

# this file's purpose is to read all csv files and turn them in to a database, 
# so that we can easily access the data for our analysis.
# this has no interconnections between tables. db_4tables is the right database for that. 
# This is just a first step to get the data in a structured way.

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

def process_csv_files(all_files, conn, category):
    """reads all csv files, extracts meta data and data, connects them and writes to database.
     IMPORTANT: this function also cuts the .csv files to one week of data gathered.
     This function is not modular
    """
    for file in all_files:
        table_name = os.path.basename(file).split('.')[0]

        try:


            if(category == "FUEL"):
                meta_df = df = pd.read_csv(file, skiprows=1, header=None, nrows=13, encoding='latin-1')

                hhid = str(meta_df.iloc[2, 1]).strip()
                sensor_id = str(meta_df.iloc[3, 1]).strip()
                fuel_type = str(meta_df.iloc[9, 1]).strip()
                consumption = str(meta_df.iloc[12, 1]).strip()

                # reading the rest and cut it to one week of data
                df = pd.read_csv(file, skiprows = 17, encoding='latin-1',  nrows = 10081)

                # connecting meta data and data
                df['hh_id'] = hhid
                df['sensor_id'] = sensor_id
                df['fuel_type'] = fuel_type
                df['consumption_kg_day'] = consumption

                df.to_sql(table_name, conn, if_exists='replace', index=False)

            elif(category == "EXACT"):
                # first meta data extract, then data extract and write to database
                meta_df = df = pd.read_csv(file, skiprows=1, header=None, nrows=14, encoding='latin-1')

                hhid = str(df.iloc[2, 1]).strip()
                sensor_id = str(df.iloc[3, 1]).strip()
                max_temp = str(df.iloc[8, 1]).strip()
                name = str(df.iloc[10, 1]).strip()
                cooking_days = str(df.iloc[13, 1]).strip()

                # reading the rest and cut it to one week of data
                df = pd.read_csv(file, skiprows = 17, encoding='latin-1', nrows = 5041)


                # connecting meta data and data
                df['hh_id'] = hhid
                df['sensor_id'] = sensor_id
                df['max_temp'] = max_temp
                df['stove_name'] = name
                df['cooking_days'] = cooking_days

                df.to_sql(table_name, conn, if_exists='replace', index=False)

        except Exception as e:
            print(f"Fehler beim Verarbeiten von {file}: {e}")

process_csv_files(all_fuel_files, conn, "FUEL")
process_csv_files(all_exact_files, conn, "EXACT")
conn.close()