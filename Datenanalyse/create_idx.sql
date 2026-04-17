-- 1. Indizes für die großen Messdaten-Tabellen (Composite Indexes)
-- Diese beschleunigen den FULL OUTER JOIN über ID und Zeitstempel massiv.
CREATE INDEX idx_exact_meas_id_time ON exact_measurement (exact_id, timestamp);
CREATE INDEX idx_fuel_meas_id_time ON fuel_measurement (fuel_id, timestamp);

-- 2. Indizes für die Metadaten-Tabellen (Joins und Filterung)
-- Beschleunigt das Zusammenführen der Sensoren in der Subquery via HHID.
CREATE INDEX idx_exact_meta_hhid ON exact_meta (hhid);
CREATE INDEX idx_fuel_meta_hhid ON fuel_meta (hhid);

-- 3. Optionale Filter-Indizes (falls die Tabellen sehr groß sind)
-- Hilft der Datenbank, die stove_name und fuel_type Filter schneller zu verarbeiten.
CREATE INDEX idx_exact_meta_stove ON exact_meta (stove_name);
CREATE INDEX idx_fuel_meta_type ON fuel_meta (fuel_type);