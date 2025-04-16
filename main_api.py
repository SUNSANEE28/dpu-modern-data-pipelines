# main.py

import requests


# ----- Configuration -----
API_KEY = "eaba6b84-ec0f-4a86-8e93-1fa9851fd676"
city = 'Bangkok'
state = 'Bangkok'
country = 'Thailand'


# ----- Step 1: Get AQI data from AirVisual API -----
def get_aqi_data():
    url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise error if not 200
        data = response.json()
        return data.get('data', {})
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return None
