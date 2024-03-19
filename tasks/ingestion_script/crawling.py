import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

class Crawler:
    def __init__(self):
        # Initialize API client with cache and retry
        self.cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)
        self.hourly_variables = ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm"]

    def fetch_weather_data(self, params):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": params['latitude'],
            "longitude": params['longitude'],
            "start_date": params['start_date'],
            "end_date": params['end_date'],
            "hourly": self.hourly_variables
        }
        responses = self.openmeteo.weather_api(url, params=params)
        return responses[0]  # Handling only the first location

    def process_response(self, response):
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }

        for i, variable in enumerate(self.hourly_variables):
            hourly_data[variable] = hourly.Variables(i).ValuesAsNumpy()
        return pd.DataFrame(data=hourly_data)
    
    def fetch_data(self,params):
        hourly_dataframe = self.process_response(self.fetch_weather_data(params=params))
        return hourly_dataframe
        # TODO: fetch data from OpenMeteo

if __name__ == "__main__":
    crawler = Crawler()
    response = crawler.fetch_data(
        {
            'latitude':52.52,
            'longitude':13.41,
            'start_date':"2024-03-03",
            'end_date':"2024-03-17",
        }
    )
    print(response)
    