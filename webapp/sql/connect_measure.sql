DROP TABLE IF EXISTS conn_measurements;

CREATE TABLE conn_measurements AS
SELECT
    i.exact_id,
    i.hhid,
    i.fuel_id,
    f.usage AS consumption_kg,
    f.timestamp,
    e.temperature,
    fm.fuel_type,
    em.stove_name
FROM conn_ids AS i
JOIN fuel_measurement AS f
    ON i.fuel_id = f.fuel_id
LEFT JOIN exact_measurement AS e
    ON i.exact_id = e.exact_id
    AND ABS(JULIANDAY(e.timestamp) - JULIANDAY(f.timestamp)) * 24 * 60 <= 5
LEFT JOIN exact_meta AS em
    ON em.exact_id = i.exact_id
LEFT JOIN fuel_meta AS fm
    ON fm.fuel_id = i.fuel_id;