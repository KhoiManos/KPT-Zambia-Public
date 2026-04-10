import pandas as pd
import os
import shutil  


ordner = "ECS_RAW"

all_data = os.listdir(ordner)



def domainExpansion(type, folder):
    for datei in all_data:
        if datei.endswith(".csv") & datei.startswith(type): 
        # Option nur FUEL oder nur EXACT Dateien
        # & datei.startswith("FUEL")
        # & datei.startswith("EXACT")

            dateipfad = os.path.join(ordner, datei)
            df = pd.read_csv(dateipfad, skiprows=1, header=None, nrows=12, encoding='latin-1')

            # Überprüfen ob Error = 1
            error = df.iloc[9, 1]
            if error == 1:
                continue

            # HHID wird gelesen
            hhid = df.iloc[2, 1]

            # Ordner erstelen und Dateien kopieren (Argument anpassen, damit es in den richtigen Ordner kommt)
            newFolder = os.path.join(folder, hhid)
            os.makedirs(newFolder, exist_ok=True)

            # Datei zu neuem Pfad kopieren
            goal = os.path.join(newFolder, datei)

            shutil.copy(dateipfad, goal)


#domainExpansion("FUEL", fuelAuswertung)
#domainExpansion("EXACT", exactAuswertung)

        

