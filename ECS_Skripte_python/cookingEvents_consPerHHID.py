import pandas as pd
import os
from pprint import pprint


current_folder = os.path.dirname(os.path.abspath(__file__))
project_folder = os.path.dirname(current_folder)

FILE_INPUT = os.path.join(project_folder, "Datenanalyse", "conn_measurements.csv")
FILE_OUTPUT = os.path.join(project_folder, "Datenanalyse", "cooking_events.csv")
FINAL_OUTPUT = os.path.join(project_folder, "Datenanalyse", "cons_per_hhid.csv")


MIN_CONSUMPTION = 0.025
MAX_INVALID = 1
TEMP_LIMIT = 40
FUEL_LOOKBACK_MIN = 90

def load_and_clean_data(filepath):
    """Lädt die Daten und bereinigt Typen sowie Header-Duplikate."""
    df = pd.read_csv(filepath, low_memory=False)
    
    # Header-Zeilen entfernen
    df = df[~df['timestamp'].astype(str).str.contains('imestamp', case=False)]
    
    # Numerische Konvertierung
    cols_to_fix = ['Weight (kg)', 'consumption (kg)', 'temperature']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['exact_id', 'timestamp'])
    
    # Lücken füllen
    df['temperature'] = df.groupby('exact_id')['temperature'].ffill(limit=3)
    return df

def identify_cooking_events(df, temp_limit):
    """Gruppiert Temperatur-Inseln zu Events."""
    df['is_cooking'] = df['temperature'] >= temp_limit
    # ID für zusammenhängende Blöcke erstellen
    df['block_id'] = (df['is_cooking'] != df['is_cooking'].shift()).cumsum()
    
    # Nur tatsächliche Koch-Events aggregieren
    events = df[df['is_cooking']].groupby(['exact_id', 'fuel_id', 'block_id']).agg(
        start_time=('timestamp', 'min'),
        stop_time=('timestamp', 'max'),
        avg_temp=('temperature', 'mean')
    ).reset_index()
    
    events['duration_min'] = (events['stop_time'] - events['start_time']) / pd.Timedelta(minutes = 1)
    events['duration_min'] = events['duration_min'].astype(float)
    events = events.drop(columns=['block_id'])
    events['event_id'] = range(len(events))
    return events

def validate_events_optimized(events, df, lookback_minutes, min_consumption):
    """
    Prüft effizient, ob vor dem Kochen Brennstoff verbraucht wurde.
    Bei 1 Mio. Zeilen ist ein Merge/Join besser als iterrows!
    """
    # Vorbereitung: Nur Zeilen mit Verbrauch extrahieren
    fuel_usage = df[df['consumption (kg)'] > min_consumption][['fuel_id', 'timestamp']].copy()
    
    # Wir nutzen merge_asof für den Zeit-Check (sehr schnell!)
    # Dazu müssen beide Dataframes nach Zeit sortiert sein
    fuel_usage = fuel_usage.sort_values('timestamp')
    events = events.sort_values('start_time')
    
    # Suche für jedes Event den zeitlich nächsten Fuel-Eintrag davor
    matches = pd.merge_asof(
        events, 
        fuel_usage, 
        left_on='start_time', 
        right_on='timestamp', 
        by='fuel_id', 
        direction='backward',
        tolerance=pd.Timedelta(minutes=lookback_minutes)
    )
    
    # Wenn ein Zeitstempel gefunden wurde (nicht NaN), ist das Event valide
    events['is_valid'] = matches['timestamp'].notna().astype(int)
    return events

def keep_valid_hhids(cooking_events, max_invalid):
    if 'hhid' not in cooking_events.columns:
        cooking_events['hhid'] = cooking_events['exact_id'].astype(str).str.split('_').str[0]
    else:
        cooking_events['hhid'] = cooking_events['hhid'].astype(str)
    # hhid aus der ID extrahieren
    cooking_events['date'] = cooking_events['start_time'].dt.date
    # Tag des CE bestimmen
    biomass_events = cooking_events[
        cooking_events['fuel_id'].str.contains('charcoal|pellet', case=False, na=False)
    ].copy()

    day_stats = biomass_events.groupby(['hhid', 'date']).agg(
        total_events = ('is_valid', 'count'),
        valid_events = ('is_valid', 'sum')
    ).reset_index()


    day_stats['invalid_count'] = day_stats['total_events'] - day_stats['valid_events']

    day_stats['clean_day'] = (day_stats['invalid_count'] <=  max_invalid) & (day_stats['total_events'] > 0)

    clean_day_set = set(
        day_stats[day_stats['clean_day'] == True][['hhid', 'date']].itertuples(index = False, name=None)
    )
    print(f"Gefundene Haushalte: {cooking_events['hhid'].unique()}")
    print(f"Saubere Haushalt-Tage: {len(clean_day_set)}")
    return clean_day_set

def calc_per_hhid(valid_hhid, df, cooking_events, final_output):
    # HHID sicherstellen
    if 'hhid' not in df.columns:
        df['hhid'] = df['exact_id'].astype(str).str.split('_').str[0]
    else:
        df['hhid'] = df['hhid'].astype(str)
        
    df['date'] = df['timestamp'].dt.date
 
    daily_duration = cooking_events.groupby(['hhid', 'date'])['duration_min'].sum().reset_index()
    daily_duration.columns = ['hhid', 'date', 'cooking_duration_min']

    df_clean = df[df.set_index(['hhid', 'date']).index.isin(valid_hhid)].copy()
    #nur saubere Daten
    df_unique_fuel = df_clean.groupby(['fuel_id', 'timestamp']).first().reset_index()
    final_consumption = df_unique_fuel.groupby(['hhid', 'date', 'fuel_type'])['consumption (kg)'].sum().reset_index()
    #print("Täglicher Verbrauch pro Haushalt und Brennstoff (bereinigt):")
    #print(final_consumption.sort_values(by=['hhid', 'date']))
    temp_cons = df_unique_fuel.groupby(['hhid', 'date', 'fuel_type'])['consumption (kg)'].sum().reset_index()
    # 4. PIVOTIEREN: Aus 'fuel_type' Spalten machen
    # Wir machen aus den Zeilen 'charcoal' und 'pellet' eigene Spalten
    final_consumption = temp_cons.pivot_table(
        index=['hhid', 'date'], 
        columns='fuel_type', 
        values='consumption (kg)',
        fill_value=0 # Wenn ein Haushalt an einem Tag nur eins von beiden nutzt, fülle das andere mit 0
    ).reset_index()
    
    # 5. Spalten umbenennen (optional, für schönere Header)
    # Das sorgt dafür, dass die Spalten genau so heißen, wie du es wolltest
    rename_dict = {
        'charcoal': 'charcoal_consumption',
        'pellet': 'pellet_consumption'
    }
    final_consumption = final_consumption.rename(columns=rename_dict)

    final_consumption = pd.merge(
        final_consumption, 
        daily_duration, 
        on=['hhid', 'date'], 
        how='left'
    )
    return final_consumption


def main():
    print("Starte Analyse...")
    
    # 1. Laden
    df = load_and_clean_data(FILE_INPUT)
    
    # 2. Events finden
    cooking_events = identify_cooking_events(df, TEMP_LIMIT)
    
    # 3. Validieren (Optimierte Methode statt iterrows)
    cooking_events = validate_events_optimized(cooking_events, df, FUEL_LOOKBACK_MIN, MIN_CONSUMPTION)
    valid_hhid = keep_valid_hhids(cooking_events, MAX_INVALID)
    final_cons = calc_per_hhid(valid_hhid, df, cooking_events, FINAL_OUTPUT) 
    
    cooking_events.to_csv(FILE_OUTPUT, index=False, encoding='latin-1')
    
    final_cons.to_csv(FINAL_OUTPUT, index=False, encoding='latin-1')
   
    # 4. Speichern & Statistik
    
    valid_count = cooking_events['is_valid'].sum()
    print(f"Ergebnis: {len(cooking_events)} Events gefunden, davon {valid_count} valide.")
    pprint(valid_hhid)

if __name__ == "__main__":
    main()