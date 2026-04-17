CREATE TABLE conn_ids AS 
 SELECT e.exact_id, f.fuel_id, e.hhid
    FROM exact_meta AS e
    JOIN fuel_meta AS f ON e.hhid = f.hhid
    WHERE 
        (e.stove_name IN ('Char Bas', 'Char Pro') AND f.fuel_type = 'charcoal')
        OR 
        (e.stove_name IN ('mimimoto', 'supamoto') AND f.fuel_type = 'pellet')