CREATE TABLE conn_measurements AS
SELECT 
    i.exact_id, 
    i.hhid, 
    i.fuel_id, 
    f."Weight (kg)", 
    f."consumption (kg)", 
    COALESCE(f.timestamp, e.timestamp) AS timestamp, 
    e.temperature,
    fm.fuel_type,
    em.stove_name
FROM conn_ids AS i
-- Erst die Haupt-Messdaten joinen
LEFT JOIN fuel_measurement AS f 
    ON i.fuel_id = f.fuel_id
LEFT JOIN exact_measurement AS e 
    ON i.exact_id = e.exact_id 
    AND e.timestamp = f.timestamp
-- Dann die Meta-Informationen (ebenfalls als LEFT JOIN, um keine Daten zu verlieren)
LEFT JOIN exact_meta AS em
    ON em.exact_id = i.exact_id
LEFT JOIN fuel_meta AS fm
    ON fm.fuel_id = i.fuel_id;