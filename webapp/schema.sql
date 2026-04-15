-- ECS Zambia Data Pipeline - Database Schema
-- Ausführen: sqlite3 ecs.db < schema.sql

-- FUEL Sensor Metadata
CREATE TABLE IF NOT EXISTS fuel_meta (
    hhid TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    fuel_type TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    fuel_id INTEGER PRIMARY KEY
);

-- FUEL Sensor Messdaten
CREATE TABLE IF NOT EXISTS fuel_measurement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    usage REAL,
    fuel_id INTEGER NOT NULL,
    FOREIGN KEY (fuel_id) REFERENCES fuel_meta(fuel_id)
);

-- EXACT Sensor Metadata
CREATE TABLE IF NOT EXISTS exact_meta (
    hhid TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    stove_name TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    max_temp TEXT,
    exact_id INTEGER PRIMARY KEY
);

-- EXACT Sensor Messdaten
CREATE TABLE IF NOT EXISTS exact_measurement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    usage REAL,
    gradient REAL,
    temperature REAL,
    exact_id INTEGER NOT NULL,
    FOREIGN KEY (exact_id) REFERENCES exact_meta(exact_id)
);

-- Indexes für bessere Performance
CREATE INDEX IF NOT EXISTS idx_fuel_meta_hhid ON fuel_meta(hhid);
CREATE INDEX IF NOT EXISTS idx_fuel_meta_sensor ON fuel_meta(sensor_id);
CREATE INDEX IF NOT EXISTS idx_fuel_measurement_fuel_id ON fuel_measurement(fuel_id);

CREATE INDEX IF NOT EXISTS idx_exact_meta_hhid ON exact_meta(hhid);
CREATE INDEX IF NOT EXISTS idx_exact_meta_sensor ON exact_meta(sensor_id);
CREATE INDEX IF NOT EXISTS idx_exact_measurement_exact_id ON exact_measurement(exact_id);
