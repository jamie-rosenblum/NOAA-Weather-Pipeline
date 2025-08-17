import requests
import pandas as pd
import os
from datetime import datetime, timedelta

API_TOKEN = os.getenv('NOAA_API_TOKEN')
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

def fetch_noaa_data(station="GHCND:USW00094728", days=1):
    """fetch daily weather data from NOAA for a given station and number of days."""
    end = datetime.today()
    start = end - timedelta(days=days)

    headers = {"token": API_TOKEN}
    params = {
        "datasetid": "GHCND",
        "stationid": station,
        "startdate": start.strftime('%Y-%m-%d'),
        "enddate": end.strftime('%Y-%m-%d'),
        "limit": 1000, 
        "units": "standard",
    }

    response = requests.get(BASE_URL, headers=headers, params=params)
    data = response.json().get('results', [])

    df = pd.DataFrame(data)
    if df.empty:
        print("No data found for the specified station and date range.")
        return None
    os.makedirs('data/raw', exist_ok=True)
    df.to_csv(f"data/raw/weather_{end.strftime('%Y%m%d')}.csv", index=False)
    print(f"Data fetched and saved to data/raw/weather_{end.strftime('%Y%m%d')}.csv")
    return df

if __name__ == "__main__":
    if API_TOKEN is None:
        print("Please set the NOAA_API_TOKEN environment variable.")
    else:
        fetch_noaa_data()