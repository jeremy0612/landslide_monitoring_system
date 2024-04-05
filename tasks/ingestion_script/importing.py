from database_hook.connection import PostgreSQLConnection
from database_hook.config import load_config
import pandas as pd
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--source', type=str, default='csv', help='Source of the data (csv or excel)')
args = parser.parse_args()
# psql_conn = PostgreSQLConnection()
# psql_conn.connect()
# psql_conn.execute_query("insert into Location(country, near, continent, longitude, latitude) values\
#                         ('Vietnam', 'Vinh','AS', 105.8461, 21.0245) returning location_id, country, near;")
# psql_conn.execute_query("INSERT INTO Event ( hazard_type,landslide_type,size,date,time,timezone,month,year,country,near,continent,longitude,latitude)\
#     VALUES ('Rockfall',    'Debris flow',     'Small', '2024-03-18', '14:30:00',  'UTC',  3, 2024,  'Vietnam', 'ang',  'AS', 108.33,  16.06 )\
#                         returning event_id,location_id, datetime_id;")
# psql_conn.disconnect()



def insert_event(row):
    query = """INSERT INTO Event ( hazard_type,landslide_type,size,date,time,timezone,month,year,country,near,continent,elevation,longitude,latitude)\
        VALUES ('{}', '{}', '{}', {}, '{}', '{}', {}, {}, '{}', '{}', '{}', {}, {}, {})\
        returning event_id,location_id, datetime_id;"""\
        .format(row['hazard_type'], row['landslide_type'], row['landslide_size'], row['date'], row['time'],\
                 "UTC", row['month'],row['year'], str(row['country']).replace("'", "''"), str(row['near']).replace("'", "''"), row['continentcode'],\
                 row['elevation'], row['longitude_info'], row['latitude_info'])
    query = query.replace("'nan'", "NULL")
    # print(query)
    result = psql_conn.execute_query(query)
    if not result:
        sys.exit(0)
def insert_data(row):
    query = """INSERT INTO Data (longitude, latitude, temperature, precipitation, rain, relative_humidity, \
        soil_temperature_0_to_7, soil_moisture_0_to_7, soil_temperature_7_to_28, soil_moisture_7_to_28, \
        soil_temperature_28_to_100, soil_moisture_28_to_100, soil_temperature_100_to_255, soil_moisture_100_to_255,\
        date, time, timezone, month, year)
        VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {}, {});"""\
        .format(row['longitude_info'], row['latitude_info'], row['temperature_2m'], row['precipitation'],\
                row['rain'], row['relative_humidity_2m'], row['soil_temperature_0_to_7cm'], row['soil_moisture_0_to_7cm'],\
                row['soil_temperature_7_to_28cm'], row['soil_moisture_7_to_28cm'], row['soil_temperature_28_to_100cm'],\
                row['soil_moisture_28_to_100cm'], row['soil_temperature_100_to_255cm'], row['soil_moisture_100_to_255cm'],\
                row['date'], row['datetime'], "UTC", row['month'], row['year'])
    query = query.replace("'nan'", "NULL")
    query = query.replace("nan", "NULL")
    # print(query)
    result = psql_conn.execute_query(query)
    if not result:
        sys.exit(0)
if __name__ == '__main__':
    # Define the filename of the JSON file
    if args.source == 'csv':
        file_path = "/app/buffer/metrics_csv.csv"
    else:
        file_path = "/app/buffer/metrics_xlsx.csv"
    psql_conn = PostgreSQLConnection()
    psql_conn.connect()
    # Read the data from the JSON file and import into the database
    try:
        with open(file_path, 'r') as f:
            data_df = pd.read_csv(f)
            # data_df = data_df['date', 'time', 'country', 'near', 'continent', 'hazard_type', 'landslide_type', 'landslide_size', 'elevation', 'latitude_info', 'longitude_info']
            print("Data loaded successfully from", file_path)
            # data_df = data_df.fillna()

            # print(data_df.info())
            # print(data_df.tail(10))
            for index, row in data_df.iterrows():
                if(index%24==0):
                    insert_event(row)
                    insert_data(row)
                else:
                    insert_data(row)
    except FileNotFoundError:
        print("Error: File", file_path, "not found.")

