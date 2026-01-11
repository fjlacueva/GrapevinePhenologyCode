from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests
import json

# Your client credentials
client_id = 'sh-f26d60a3-8f3d-439a-a639-a620c58fc82d'
client_secret = 'wwkfGACdHbwtJAqxUSwsLbAoB8RUCMTa'

# Create a session
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)

# Get token for the session
token = oauth.fetch_token(token_url='https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
                          client_secret=client_secret)

# All requests using this session will have an access token automatically added

resp = oauth.get("https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/")
data = json.loads(resp.text)
print(str(data))

boxDimension = [
                13.822174072265625,
                45.85080395917834,
                14.55963134765625,
                46.29191774991382,
            ]

dateStart = "2018-01-27T00:00:00Z"
dateFinish = "2018-12-27T23:59:59Z"

'''boxDimension = [
                    -2.3565673828124996,
                    39.76210275375139,
                    1.043701171875,
                    42.99661231842139,
                ]'''

type = "sentinel-2-l1c"

evalscript = """
//VERSION=3
function setup() {
  return {
    input: ["B02", "B03", "B04"],
    mosaicking: Mosaicking.ORBIT,
    output: { id: "default", bands: 3 },
  }
}

function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
  outputMetadata.userData = { scenes: scenes.orbits }
}

function evaluatePixel(samples) {
  return [2.5 * samples[0].B04, 2.5 * samples[0].B03, 2.5 * samples[0].B02]
}
"""

request = {
    "input": {
        "bounds": {
            "bbox": boxDimension
        },
        "data": [
            {
                "type": type,
                "dataFilter": {
                    "timeRange": {
                        "from": dateStart,
                        "to": dateFinish,
                    }
                },
            }
        ],
    },
    "output": {
        "width": 512,
        "height": 512,
        "responses": [
            {
                "identifier": "default",
                "format": {"type": "image/tiff"},
            },
            {
                "identifier": "userdata",
                "format": {"type": "application/json"},
            },
        ],
    },
    "evalscript": evalscript,
}

url = "https://sh.dataspace.copernicus.eu/api/v1/process"
response = oauth.post(url, json=request, headers={"Accept": "application/tar"})

fileName = 'tarfile-' + str(1) + '.tar'

if response.status_code in (200,):
    with open(fileName, 'wb') as tarfile:
        tarfile.write(response.content)

'''
#url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products"
url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(060882f4-0a34-5f14-8e25-6876e4470b0d)/$value"

response = oauth.get(url, stream=True)

with open("product.zip", "wb") as file:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            file.write(chunk)'''