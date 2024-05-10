-- 1. Define the function to copy updated data
--  Create the trigger on update event
CREATE OR REPLACE FUNCTION copy_updated_event()
RETURNS TRIGGER AS $$
    DECLARE
        location_idd integer;
        datetime_idd integer;
        BEGIN
          -- Get the updated data from the trigger
            SELECT location_id into location_idd
            FROM Location
            WHERE longitude = NEW.longitude AND latitude = NEW.latitude;

            if location_idd is null then
                INSERT INTO Location (country, near, continent, elevation, longitude, latitude)
                VALUES (NEW.country, NEW.near, NEW.continent, NEW.elevation, NEW.longitude, NEW.latitude)
                returning location_id into location_idd;
            end if;


          INSERT INTO Datetime ( date, time, timezone, month, year) -- Replace with target table & columns
          VALUES (NEW.date, NEW.time, NEW.timezone, NEW.month, NEW.year) returning datetime_id into datetime_idd;

          INSERT INTO Landslide_event (event_id,location_id,datetime_id , hazard_type, landslide_type, size) -- Replace with target table & columns
          VALUES (NEW.event_id, location_idd, datetime_idd, NEW.hazard_type, NEW.landslide_type, NEW.size);

          UPDATE Event
          SET location_id = location_idd, datetime_id = datetime_idd
          WHERE event_id = NEW.event_id;
          
          RETURN NEW;
        END;
$$ LANGUAGE plpgsql;

--  Create the trigger on update data
CREATE OR REPLACE FUNCTION copy_updated_data()
    RETURNS TRIGGER AS $$
DECLARE
    weatherdata_idd integer;
    datetime_idd integer;
    location_idd integer;
BEGIN
    SELECT location_id into location_idd from Location where longitude = NEW.longitude and latitude = NEW.latitude;
    -- SELECT datetime_id into datetime_idd from Datetime where Datetime.date = NEW.date and month = NEW.month and year = NEW.year;
    -- Get the updated data from the trigger
    INSERT INTO Datetime ( date, time, timezone, month, year) -- Replace with target table & columns
    VALUES (NEW.date, NEW.time, NEW.timezone, NEW.month, NEW.year) returning datetime_id into datetime_idd;

    INSERT INTO Weather_data (location_id,datetime_id , temperature, precipitation, rain, relative_humidity) -- Replace with target table & columns
    VALUES (location_idd, datetime_idd, NEW.temperature, NEW.precipitation, NEW.rain, NEW.relative_humidity) returning fact_id into weatherdata_idd;

    INSERT INTO Soil ( weather_data_id ,depth, temperature, moisture) -- Replace with target table & columns
    VALUES ( weatherdata_idd, '0 to 7 cm', NEW.soil_temperature_0_to_7, NEW.soil_moisture_0_to_7) ;

    INSERT INTO Soil ( weather_data_id ,depth, temperature, moisture) -- Replace with target table & columns
    VALUES ( weatherdata_idd, '7 to 28 cm', NEW.soil_temperature_7_to_28, NEW.soil_moisture_7_to_28) ;

    INSERT INTO Soil ( weather_data_id ,depth, temperature, moisture) -- Replace with target table & columns
    VALUES ( weatherdata_idd, '28 to 100 cm', NEW.soil_temperature_28_to_100, NEW.soil_moisture_28_to_100) ;

    INSERT INTO Soil ( weather_data_id ,depth, temperature, moisture) -- Replace with target table & columns
    VALUES ( weatherdata_idd, '100 to 255 cm', NEW.soil_temperature_100_to_255, NEW.soil_moisture_100_to_255) ;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Create the trigger on update event 
-- Create the trigger on update event 
DROP TRIGGER IF EXISTS copy_on_update_event ON Event;
CREATE TRIGGER copy_on_update_event
AFTER INSERT ON Event
FOR EACH ROW
EXECUTE PROCEDURE copy_updated_event();
-- Create the trigger on update data
DROP TRIGGER IF EXISTS copy_on_update_data ON Data;
CREATE TRIGGER copy_on_update_data
AFTER INSERT ON Data
FOR EACH ROW
EXECUTE PROCEDURE copy_updated_data();



