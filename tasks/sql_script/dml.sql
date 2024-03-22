
-- 1. Define the function to copy updated data
CREATE OR REPLACE FUNCTION copy_updated_data()
RETURNS TRIGGER AS $$
    DECLARE
        location_idd integer;
        datetime_idd integer;
        BEGIN
          -- Get the updated data from the trigger
          INSERT INTO Location (country, near, continent, longitude, latitude) -- Replace with target table & columns
          VALUES (NEW.country, NEW.near, NEW.continent, NEW.longitude, NEW.latitude) returning location_id into location_idd;

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

-- 2. Create the trigger on update event (replace "source_table" with your actual table name)
DROP TRIGGER IF EXISTS copy_on_update ON Event;
CREATE TRIGGER copy_on_update
AFTER INSERT ON Event
FOR EACH ROW
EXECUTE PROCEDURE copy_updated_data();




INSERT INTO Event (
    hazard_type,
    landslide_type,
    size,
    date,
    time,
    timezone,
    month,
    year,
    country,
    near,
    continent,
    longitude,
    latitude
)
VALUES (
           'Rockfall',  -- Replace with hazard type (optional)
           'Debris flow',  -- Replace with landslide type (optional)
           'Small',  -- Replace with size category (optional)
           '2024-03-18',  -- Replace with actual date
           '14:30:00',  -- Replace with actual time (optional)
           'UTC',  -- Replace with timezone
           3,  -- Month (March)
           2024,  -- Year
           'Vietnam',  -- Replace with country (optional)
           'ang',  -- Replace with nearby location (optional)
           'AS',  -- Continent code (optional)
           108.33,  -- Replace with actual longitude
           16.06 -- Replace with actual latitude
       );
