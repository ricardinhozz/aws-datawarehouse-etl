import requests
import os
from datetime import date, timedelta

API_KEY = os.getenv("WEATHER_API_KEY")


def extract_weatherbit_api():
    today = date.today()
    yesterday = today - timedelta(days=1)

    url = "https://api.weatherbit.io/v2.0/history/hourly"
    params = {
        "city": "SaoPaulo",
        "start_date": yesterday,
        "end_date": today,
        "key": API_KEY,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()