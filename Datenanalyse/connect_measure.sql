DROP TABLE IF EXISTS conn_measurements;

CREATE TABLE conn_measurements AS
SELECT 
    i.exact_id, 
    i.hhid, 
    i.fuel_id, 
    f."Weight (kg)", 
    f."consumption (kg)", 
    -- Wir priorisieren den Zeitstempel der Brennstoff-Messung (jede Minute)
    COALESCE(f.timestamp, e.timestamp) AS combined_timestamp, 
    e.temperature
FROM 
    connect_meta AS i
-- 1. Wir starten mit den Brennstoff-Daten (jede Minute vorhanden)
LEFT JOIN fuel_measurement AS f 
    ON i.fuel_id = f.fuel_id
-- 2. Wir joinen die Temperatur-Daten dazu (nur jede 2. Minute vorhanden)
LEFT JOIN exact_measurement AS e 
    ON i.exact_id = e.exact_id 
    AND e.timestamp = f.timestamp;
