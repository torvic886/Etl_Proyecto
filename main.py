import requests
# Api del Estaciones Meteorológicas e Hidrológicas de Colombia
# prueba
# Implementacion Api
# Prueba conexion repositorio
url = "https://www.datos.gov.co/resource/ka9f-zy7y.json"

headers = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip"
}

response = requests.get(url, headers=headers)

print(response.text)
