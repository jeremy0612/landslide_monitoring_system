from database_hook.connection import PostgreSQLConnection
from database_hook.config import load_config
import pandas as pd

psql_conn = PostgreSQLConnection()
psql_conn.connect()
psql_conn.execute_query("insert into Location(country, near, continent, longitude, latitude) values\
                        ('Vietnam', 'Vinh','AS', 105.8461, 21.0245) returning location_id, country, near;")
psql_conn.execute_query("INSERT INTO Event ( hazard_type,landslide_type,size,date,time,timezone,month,year,country,near,continent,longitude,latitude)\
    VALUES ('Rockfall',    'Debris flow',     'Small', '2024-03-18', '14:30:00',  'UTC',  3, 2024,  'Vietnam', 'ang',  'AS', 108.33,  16.06 )\
                        returning event_id,location_id, datetime_id;")
psql_conn.disconnect()

# Define the filename of the JSON file
# json_file_path = "tmp/date.json"

# Read the data from the JSON file
# try:
#   with open(json_file_path, 'r') as f:
#     data_df = pd.read_json(f)
#   print("Data loaded successfully from", json_file_path)
# except FileNotFoundError:
#   print("Error: File", json_file_path, "not found.")

# # Convert string columns back to datetime (assuming same format used earlier)
# data_df['time'] = pd.to_datetime(data_df['time'], format='%H:%M:%S',errors='coerce')  # Handle potential missing values

# # Now you can work with the data_df DataFrame
# print(data_df.tail(35))  # Display the first few rows



