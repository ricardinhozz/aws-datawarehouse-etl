import requests
from datetime import date, timedelta


def extract_api_open_meteo():
    today = date.today()
    yesterday = today - timedelta(days=1)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -23.5475,
        "longitude": -46.6361,
        "hourly": "temperature_2m,relative_humidity_2m,rain,cloud_cover",
        "start_date": yesterday,
        "end_date": today,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()