import requests
import os
import json
from datetime import date, timedelta

API_KEY = os.getenv("WEATHER_API_KEY")


today = date.today()
yesterday = today - timedelta(days=1)


def extract_weatherbit_api():
    url = f'https://api.weatherbit.io/v2.0/history/hourly?city=SaoPaulo&start_date={yesterday}&end_date={today}&key={API_KEY}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data