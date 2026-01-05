import requests
import json
from datetime import date, timedelta

today = date.today()
yesterday = today - timedelta(days=1)

for date in [today,yesterday]:
    date = date.strftime("%YYYY-mm-dd")

def extract_api_open_meteo():
    url = f'https://api.open-meteo.com/v1/forecast?latitude=-23.5475&longitude=-46.6361&hourly=temperature_2m,relative_humidity_2m,rain,cloud_cover&start_date={yesterday}&end_date={today}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data