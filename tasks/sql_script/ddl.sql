-- 1. Create the Location table
CREATE TABLE Location (
  location_id SERIAL PRIMARY KEY,
  country VARCHAR(50) NULL,
  near VARCHAR(50) NULL,
  continent VARCHAR(2)  NULL,
  longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
  latitude DECIMAL(11, 8) NOT NULL   -- High precision for coordinates
);

-- 2. Create the Datetime table
CREATE TABLE Datetime (
  datetime_id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  time TIME NULL,
  timezone VARCHAR(15) NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
  year INTEGER NOT NULL
);

-- 3. Create the Soil table
CREATE TABLE Soil (
  soil_value_id SERIAL PRIMARY KEY,
  depth VARCHAR(15) NOT NULL,
  temperature DECIMAL(5, 2) NOT NULL,
  moisture DECIMAL(6, 3) NOT NULL
);

-- 4. Create the Weather_data table (foreign keys)
CREATE TABLE Weather_data (
  fact_id SERIAL PRIMARY KEY,
  location_id INTEGER NOT NULL REFERENCES Location(location_id),
  datetime_id INTEGER NOT NULL REFERENCES Datetime(datetime_id),
  soil_value_id INTEGER REFERENCES Soil(soil_value_id),
  temperature DECIMAL(5, 2) NOT NULL,
  precipitation DECIMAL(5, 2) NOT NULL,
  rain DECIMAL(5, 2) NOT NULL,
  relative_humidity SMALLINT NOT NULL CHECK (relative_humidity BETWEEN 0 AND 100)  -- Enforce humidity range (0-100)
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
  date DATE NOT NULL,
  time TIME NULL,
  timezone VARCHAR(15) NOT NULL,
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),  -- Enforce month range (1-12)
  year INTEGER NOT NULL,
  country VARCHAR(50) NULL,
  near VARCHAR(50) NULL,
  continent VARCHAR(2)  NULL,
  longitude DECIMAL(11, 8) NOT NULL,  -- High precision for coordinates
  latitude DECIMAL(11, 8) NOT NULL   -- High precision for coordinates
);



-- ===== Sample insertion ===== --
insert into Location(country, near, continent, longitude, latitude) values
('Vietnam', 'Hanoi','AS', 105.8461, 21.0245),
('Vietnam', 'Hue', 'EU',107.6050, 16.4667)

-- ===== Sample reset database ===== --
drop table Event;
drop table Weather_data;
drop table Landslide_event;
drop table Location;
drop table Datetime;
drop table Soil;
