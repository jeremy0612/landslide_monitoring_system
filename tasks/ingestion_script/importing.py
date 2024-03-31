from database_hook.connection import PostgreSQLConnection
from database_hook.config import load_config
import pandas as pd

# psql_conn = PostgreSQLConnection()
# psql_conn.connect()
# psql_conn.execute_query("insert into Location(country, near, continent, longitude, latitude) values\
#                         ('Vietnam', 'Vinh','AS', 105.8461, 21.0245) returning location_id, country, near;")
# psql_conn.execute_query("INSERT INTO Event ( hazard_type,landslide_type,size,date,time,timezone,month,year,country,near,continent,longitude,latitude)\
#     VALUES ('Rockfall',    'Debris flow',     'Small', '2024-03-18', '14:30:00',  'UTC',  3, 2024,  'Vietnam', 'ang',  'AS', 108.33,  16.06 )\
#                         returning event_id,location_id, datetime_id;")
# psql_conn.disconnect()


# Define the filename of the JSON file
json_file_path = "/app/buffer/metrics_xlsx.json"
psql_conn = PostgreSQLConnection()
psql_conn.connect()
def insert_record(row):
    query = "INSERT INTO Event ( hazard_type,landslide_type,size,date,time,timezone,month,year,country,near,continent,longitude,latitude)\
        VALUES ('{}', '{}', '{}', {}, '{}', '{}', {}, {}, '{}', '{}', '{}', {}, {})\
        returning event_id,location_id, datetime_id;"\
        .format(row['hazard_type'], row['landslide_type'], row['landslide_size'], row['date'], row['time'], "UTC", row['month'],row['year'], row['country'], row['near'], row['continentcode'], row['longitude'], row['latitude'])
    print(query)
    # psql_conn.execute_query(query)
    

# Read the data from the JSON file and import into the database
try:
    with open(json_file_path, 'r') as f:
        data_df = pd.read_json(f)
        print("Data loaded successfully from", json_file_path)
        # data_df = data_df.fillna()

        print(data_df.info())
        print(data_df.head(10)['date'])
        # for index, row in data_df.head(10).iterrows():
        #     insert_record(row)
except FileNotFoundError:
    print("Error: File", json_file_path, "not found.")

