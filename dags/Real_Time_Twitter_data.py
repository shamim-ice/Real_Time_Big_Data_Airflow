import requests
import time

API_KEY = "your_api_key"
CITY = "London"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

while True:
    response = requests.get(URL).json()
    print(response)
    time.sleep(50)  # Fetch data every 10 seconds
