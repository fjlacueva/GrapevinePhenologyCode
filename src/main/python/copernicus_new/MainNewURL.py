import json
import requests

def get_access_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                          data=data,
                          )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
        )
    return r.json()["access_token"]


access_token = get_access_token("*******@*******.***", "***********.")

print(str(access_token))

url = 'https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/collections/'

headers = {"Authorization":  access_token}

session = requests.Session()
session.headers.update(headers)
response = session.get(url, headers=headers, stream=True)

data = json.loads(response.text)

print(str(data))


url = 'https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/collections/sentinel-2-l2a'

headers = {"Authorization":  access_token}

session = requests.Session()
session.headers.update(headers)
response = session.get(url, headers=headers, stream=True)

data = json.loads(response.text)

print(str(data))


datos = {
    "bbox": [13, 45, 14, 46],
    "datetime": "2019-09-10T00:00:00Z/2019-12-10T23:59:59Z",
    "collections": ["sentinel-2-l2a"],
    "limit": 5,
}

access_token = get_access_token("****************@*******.***", "*************")

print(str(access_token))

#url = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/conformance"
url = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"

#headers = {"Authorization":  access_token}
#headers = {"Authorization":  "Bearer " + access_token, 'Content-type': 'application/geo+json'}
headers = {"Authorization":  access_token, 'Content-type': 'application/geo+json'}

response = requests.get(url, headers=headers, json=datos)
data = json.loads(response.text)
print(str(data))