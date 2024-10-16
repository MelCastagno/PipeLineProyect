# extract.py

import requests
import pandas as pd

def fetch_weather_data(api_key, lat, lon):
    """
    Fetch 5-day weather forecast data for a specified latitude and longitude using OpenWeatherMap's /data/2.5/forecast API.
    """
    # OpenWeatherMap Forecast API URL
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    
    # Make the API request
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Check if 'list' key exists
        if 'list' in data:
            forecast_list = data['list']
            df = pd.json_normalize(forecast_list)
            return df
        else:
            print(f"'list' key not found in the response. Full response: {data}")
            return None
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
        return None

if __name__ == "__main__":
    API_KEY = 'APY KEY'  
    LAT = 41.3851  # Latitude for Barcelona
    LON = 2.1734    # Longitude for Barcelona

    # Fetch and save the weather data
    weather_df = fetch_weather_data(API_KEY, LAT, LON)
    if weather_df is not None:
        weather_df.to_csv('raw_weather_data.csv', index=False)
        print("Data extracted and saved to raw_weather_data.csv")
