-- ===== Sample reset database ===== --
drop table if exists Event;
drop table if exists Landslide_event;
drop table if exists Data;
drop table if exists Soil;
drop table if exists Weather_data;
drop table if exists Location;
drop table if exists Datetime;
-- 1. Create the Location table
CREATE TABLE Location (
  location_id SERIAL PRIMARY KEY,
  country VARCHAR(12) NULL,
  near VARCHAR(100) NULL,
  continent VARCHAR(4)  NULL,
  elevation INT NOT NULL,
  longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
  latitude DECIMAL(11, 8) NOT NULL   -- High precision for coordinates
);

-- 2. Create the Datetime table
CREATE TABLE Datetime (
  datetime_id SERIAL PRIMARY KEY,
  date smallint NOT NULL,
  time TIME NULL,
  timezone VARCHAR(15) NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
  year INTEGER NOT NULL
);

-- 3. Create the Weather_data table (foreign keys)
CREATE TABLE Weather_data (
  fact_id SERIAL PRIMARY KEY,
  location_id INTEGER NOT NULL REFERENCES Location(location_id),
  datetime_id INTEGER NOT NULL REFERENCES Datetime(datetime_id),
  temperature DECIMAL(5, 2) NOT NULL,
  precipitation DECIMAL(5, 2) NOT NULL,
  rain DECIMAL(5, 2) NOT NULL,
  relative_humidity SMALLINT NOT NULL CHECK (relative_humidity BETWEEN 0 AND 100)  -- Enforce humidity range (0-100)
);
-- 4. Create the Soil table
CREATE TABLE Soil (
  soil_value_id SERIAL PRIMARY KEY,
  weather_data_id INTEGER REFERENCES Weather_data(fact_id),
  depth VARCHAR(15) NOT NULL,
  temperature DECIMAL(5, 2) NOT NULL,
  moisture DECIMAL(6, 3) NOT NULL
);

-- 5. Create the Landslide_event table (foreign keys)
CREATE TABLE Landslide_event (
  event_id INTEGER NOT NULL PRIMARY KEY,
  location_id INTEGER NOT NULL REFERENCES Location(location_id),
  datetime_id INTEGER NOT NULL REFERENCES Datetime(datetime_id),
  hazard_type VARCHAR(50) NULL,
  landslide_type VARCHAR(50) NULL,
  size VARCHAR(20) NULL
);

-- ===== Cache for insertion ===== --
CREATE TABLE Event (
  event_id SERIAL PRIMARY KEY,
  location_id INTEGER NULL ,
  datetime_id INTEGER NULL ,
  hazard_type VARCHAR(50) NULL,
  landslide_type VARCHAR(50) NULL,
  size VARCHAR(20) NULL,
  date SMALLINT NOT NULL,
  time TIME NULL,
  timezone VARCHAR(15) NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
  year INTEGER NOT NULL,
  country VARCHAR(12) NULL,
  near VARCHAR(100) NULL,
  continent VARCHAR(4)  NULL,
  elevation INT NOT NULL ,
  longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
  latitude DECIMAL(11, 8) NOT NULL   -- High precision for coordinates
);

CREATE TABLE Data (
  longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
  latitude DECIMAL(11, 8) NOT NULL,   -- High precision for coordinates
  -- Parse to fact table
  temperature DECIMAL(5, 2) NOT NULL,
  precipitation DECIMAL(5, 2) NOT NULL,
  rain DECIMAL(5, 2) NOT NULL,
  relative_humidity SMALLINT NOT NULL CHECK (relative_humidity BETWEEN 0 AND 100),
  -- Enforce humidity range (0-100)
  -- Parse to soil table
  soil_temperature_0_to_7 DECIMAL(5, 2) NOT NULL,
  soil_moisture_0_to_7 DECIMAL(6, 3) NOT NULL,

  soil_temperature_7_to_28 DECIMAL(5, 2) NOT NULL,
  soil_moisture_7_to_28 DECIMAL(6, 3) NOT NULL,

  soil_temperature_28_to_100 DECIMAL(5, 2) NOT NULL,
  soil_moisture_28_to_100 DECIMAL(6, 3) NOT NULL,

  soil_temperature_100_to_255 DECIMAL(5, 2) NOT NULL,
  soil_moisture_100_to_255 DECIMAL(6, 3) NOT NULL,

    -- Parse to datetime table
  date smallint NOT NULL,
  time TIME NULL,
  timezone VARCHAR(15) NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
  year INTEGER NOT NULL
)
-- -- ===== Inference table ===== --
-- CREATE TABLE Inference
-- (
--   longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
--   latitude DECIMAL(11, 8) NOT NULL,   -- High precision for coordinates
--   date smallint NOT NULL,
--   time TIME NULL,
--   month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
--   year INTEGER NOT NULL
-- )
