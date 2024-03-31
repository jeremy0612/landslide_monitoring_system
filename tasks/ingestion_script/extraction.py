import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--source', type=str, default='csv', help='Source of the data (csv or excel)')
args = parser.parse_args()

# Data paths
csv_file_path = "/app/data/dev_sample.csv"
excel_file_path = "/app/data/dev_sample.xlsx"

# Select the columns you need
selected_columns = ["id","date", "time", "country", "near", "continentcode",
                    "hazard_type", "landslide_type", 
                    "landslide_size", "latitude", "longitude"]
def convert_to_datetime(time_str):
    try:
        obj = pd.to_datetime(time_str, format='%H:%M:%S')
        return obj.strftime('%H:%M:%S')
    except:
        return pd.NaT
    
def convert_to_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%m/%d/%Y')
    except:
        return pd.NaT
        
def extract_data(source):
    # Reading CSV, XLSX files
    # Create a new DataFrame with only the selected columns
    if source == "csv":
        filtered_data_df = pd.read_csv(csv_file_path)[selected_columns]
    else:
        filtered_data_df = pd.read_excel(excel_file_path)[selected_columns]
    filtered_data_df["time"] = filtered_data_df['time'].map(convert_to_datetime)
    filtered_data_df["date"] = filtered_data_df['date'].map(convert_to_date)
    # print(filtered_data_df.info())

    # Create a new DataFrame for Date
    date_df = filtered_data_df[['id','date','time']]
    date_df.loc[:, 'year'] = date_df['date'].dt.year
    date_df.loc[:, 'month'] = date_df['date'].dt.month
    date_df.loc[:, 'date'] = date_df['date'].dt.day
    # print(date_df.dropna().head(10))  
    # Create a new DataFrame for Location
    location_df = filtered_data_df[['id','country','near','continentcode','latitude','longitude']]
    # print(location_df.head(10))
    # Create a new DataFrame for Hazard
    hazard_df = filtered_data_df[['id','hazard_type','landslide_type','landslide_size']]
    # print(hazard_df.head(10))

    merged_df = date_df.merge(location_df, on='id', how='outer').merge(hazard_df, on='id', how='outer')
    if source == 'csv':
        merged_df.to_json('/app/buffer/date_csv.json', orient='records')
    else:
        merged_df.to_json('/app/buffer/date_xlsx.json', orient='records')
    return merged_df
    # Push to JSON files
    # date_df.to_json('/buffer/date.json', orient='records')
    # location_df.to_json('/buffer/location.json', orient='records')
    # hazard_df.to_json('/buffer/hazard.json', orient='records')
    


if __name__ == '__main__':
    if args.source == 'csv':
        print('Extracting data from CSV file')
        extract_data('csv')
    else:
        print('Extracting data from Excel file')
        extract_data('xlsx')