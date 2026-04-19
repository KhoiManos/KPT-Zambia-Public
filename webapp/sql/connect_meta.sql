DROP TABLE IF EXISTS connect_meta;

CREATE TABLE connect_meta (
    conn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    exact_id INTEGER,
    fuel_id INTEGER,
    hhid INTEGER
);

INSERT INTO connect_meta (exact_id, fuel_id, hhid)
SELECT 
    e.exact_id, 
    f.fuel_id, 
    e.hhid
FROM exact_meta AS e
JOIN fuel_meta AS f ON e.hhid = f.hhid
WHERE 
    (e.stove_name IN ('Char Bas', 'Char Pro') AND f.fuel_type = 'charcoal')
    OR 
    (e.stove_name IN ('minimoto', 'supamoto') AND f.fuel_type = 'pellet');