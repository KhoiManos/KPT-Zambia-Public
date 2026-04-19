DELETE FROM conn_ids
WHERE exact_id IN (
    SELECT exact_id
    FROM conn_ids
    GROUP BY exact_id
    HAVING COUNT(DISTINCT fuel_id) > 1
);