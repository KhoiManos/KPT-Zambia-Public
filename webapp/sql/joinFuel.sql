CREATE TABLE fuel_join AS
SELECT 
    fuel_meta.hhid, 
    fuel_type, 
    "Weight (kg)", 
    "consumption (kg)", 
    combined_timestamp, 
    temperature
FROM conn_measurements
JOIN fuel_meta
    ON conn_measurements.fuel_id = fuel_meta.fuel_id;