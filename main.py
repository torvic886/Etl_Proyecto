import json

import requests
response = requests.get("https://www.datos.gov.co/resource/ka9f-zy7y.json")
json.loads(response.content)
print(json.loads(response.content))

