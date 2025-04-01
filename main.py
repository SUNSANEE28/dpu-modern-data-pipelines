import requests


API_KEY = "eaba6b84-ec0f-4a86-8e93-1fa9851fd676"
city = 'Bangkok'
state = 'Bangkok'
country = 'Thailand'

url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
print(data)