import pandas as pd
import os
import glob
import shutil

# This file is for testing purposes, to check if we can read the meta data and the data from the csv files correctly. It is not part of the final code, but it helps us to understand how to extract the information we need for our analysis.

current = os.path.dirname(__file__)
project = os.path.dirname(current)

ornder_pfad = os.path.join(project, "ECS_HHID")

datei_pfad = os.path.join(project, "ECS_HHID", "101", "FUELv2 25011_2025-11-07_11-15-09_CLEAN.csv")
subordner_pfad = os.path.join(ornder_pfad, "301")
folder_list = []
folder_list.append(subordner_pfad)

df = pd.read_csv(datei_pfad, skiprows = 17, encoding='latin-1',  nrows = 10081)
print(df.iloc[-1,0])