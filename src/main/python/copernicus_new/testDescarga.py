# Utilities
import os
import datetime
# import numpy as np
# import matplotlib.pyplot as plt
# import pandas as pd
import requests
# import json
import datetime
import logging

#logging
########
logger = logging.getLogger(__name__)
if not logger.handlers:
  if os.name  == 'nt':
    realClimateDataCalculus_loggingPath=r'C:\TEMP\GRAPEVINE\climaticDataSiarWinkle.log'
  else:
    realClimateDataCalculus_loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'climaticDataSiarWinkle')
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  fh = logging.FileHandler(realClimateDataCalculus_loggingPath, mode="w", encoding="utf-8")
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, realClimateDataCalculus_loggingPath))

# from sentinelhub import (SHConfig, DataCollection, SentinelHubCatalog, SentinelHubRequest, BBox, bbox_to_dimensions, CRS, MimeType, Geometry)
# Import credentials
from creds import *
# from utils import plot_image
moduleName= "downloadCopernicusSentinel2Products"

def getCopernicusAccessToken(credentials:dict =None) -> str:

    if credentials is None or len(credentials.keys())==0:
        raise Exception(f"{moduleName}.getCopernicusAccessToken: the parameter credentials is not correctly set.")
    
    if "username" not in credentials.keys():
        raise Exception(f"{moduleName}.getCopernicusAccessToken: the parameter credentials does not contain key 'usename'.")
    if "password" not in credentials.keys():
        raise Exception(f"{moduleName}.getCopernicusAccessToken: the parameter credentials does not contain key 'password'.")

    username = credentials['username']
    if username is None or len(username)==0:
        raise Exception(f"{moduleName}.getCopernicusAccessToken: the parameter credentials['username'] is not correcly set.")
    password = credentials['password']
    if password is None or len(password)==0:
        raise Exception(f"{moduleName}.getCopernicusAccessToken: the parameter credentials['password'] is not correcly set.")

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
            f"{moduleName}.getCopernicusAccessToken: Keycloak token creation failed. Reponse from the server was: {r.json()}"
            )
    return r.json()["access_token"]





def downloadCopernicusSentinel2Products(credentials:dict=None, destinationFilePath:str=None,
    start_date:str = None,
    end_date:str = None,
    aoi:str = None,
    cloud_coverage:float=15.00)->bool:

    downloaded=True
    data_collection = "SENTINEL-2"
    productsFilter = "'MSIL2A'"


    if start_date is None or len(start_date)==0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter start_date is not correctly set.")
    if end_date is None or len(end_date)==0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter end_date is not correctly set.")
    
    if credentials is None or len(credentials.keys())==0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter credentials is not correctly set.")
    if destinationFilePath is None or len(destinationFilePath)==0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter destinationFilePath is not correctly set.")
    
    if cloud_coverage is None or cloud_coverage<0.0 or cloud_coverage>100.0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter cloud_coverage is not correctly set. It must be a float value in range [0.0, 100.0]")


    if aoi is None or len(aoi)==0:
        raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter area of interest (aoi) is not correctly set.")


    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(<productID>)/$value"

    # json = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 40.00) and ContentDate/Start gt 2022-01-01T00:00:00.000Z and ContentDate/Start lt 2022-01-03T00:00:00.000Z&$top=10").json()
    # df = pd.DataFrame.from_dict(json['value'])

    try:
        # Termino de verificar los parámetros
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if start_date_obj >= datetime.datetime.today():
            raise Exception(f"The parameter start_date ({start_date})is a future date.")
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        if start_date_obj>= end_date_obj:
            raise Exception(f"The parameter start_date ({start_date}) can not be later than end_date ({end_date})")
        
        if os.path.exists(destinationFilePath):
            if os.path.isdir(destinationFilePath):
                if os.access(destinationFilePath, os.W_OK):
                    pass
                else:
                    raise Exception(f"The parameter destinationFilePath is a valid path to a folder but the process is not allowed to write in it: {destinationFilePath}")
            else:
                    raise Exception(f"The parameter destinationFilePath is a valid path but not to a folder: {destinationFilePath}")
        else:
            raise Exception(f"The parameter destinationFilePath is not a a valid to a folder or file: {destinationFilePath}")
        
        catalogQueryURL = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' "+\
            f"and contains(Name, {productsFilter}) "+ \
            f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) "+ \
            f"and ContentDate/Start gt {start_date}T00:00:00.000Z "+ \
            f"and ContentDate/Start lt {end_date}T00:00:00.000Z "  +  \
            f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_coverage})" + \
            f"&$orderby=ContentDate/Start%20asc"+ \
            f"&$top=1000"
        response = requests.get(catalogQueryURL)
        response.raise_for_status()
        products = response.json()

        keycloak_token = getCopernicusAccessToken(credentials=credentials)

        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {keycloak_token}'})

        for product in products["value"]:
            id = product["Id"]
            name = product["Name"]
            url2Download = url.replace("<productID>", id)
            response = session.get(url2Download, allow_redirects=False)
            while response.status_code in (301, 302, 303, 307, 401):
                try:
                    if response.status_code != 401:
                        if "Location" in response.headers.keys():
                            url2Download = response.headers['Location']
                            response = session.get(url2Download, allow_redirects=False)
                        else: 
                            if "location" in response.headers.keys():
                                url2Download = response.headers['location']
                                response = session.get(url2Download, allow_redirects=False)
                            else:
                                logger.error(f"{moduleName}.downloadCopernicusSentinel2Products response.headers does not contain key 'Location' or 'location' {response.headers.keys()}")                                
                    else:
                        if response.reason=='Unauthorized':
                            keycloak_token = getCopernicusAccessToken(credentials=credentials)
                            session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                            response = session.get(url2Download, allow_redirects=False)
                        else:
                            if response.reason=='Expired signature!':
                                keycloak_token = getCopernicusAccessToken(credentials=credentials)
                                session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                                response = session.get(url2Download, allow_redirects=False)
                            else:
                                logger.error(f"{moduleName}.downloadCopernicusSentinel2Products got reason: {response.reason} to HTTP error 401 when downloading product:{name}")

                except Exception as e:
                    logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: catches exception e: {e}\n\t when trying to write file: {filePath2Write}")
            if response.status_code >=200 and response.status_code< 300:
                try:
                    file = session.get(url2Download, verify=False, allow_redirects=True)
                    filePath2Write = destinationFilePath + name + '.zip'
                    with open(filePath2Write, 'wb') as p:
                        p.write(file.content)
                except Exception as e:
                    logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: catches exception e: {e}\n\t when trying to write file: {filePath2Write}")
            else:
                if response.status_code==401 and response.reason=='Unauthorized':
                    keycloak_token = getCopernicusAccessToken(credentials=credentials)
                    session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                else:
                    if response.status_code==401 and response.reason=='Expired signature!':
                        keycloak_token = getCopernicusAccessToken(credentials=credentials)
                        session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                    else:
                        logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: not able to download Sentinel-2 product {name}. When requesting to dataspace.copernicus.eu I recived the error {file.status_code} and the message {file.text}. ")
                    
    except Exception as e:
        raise Exception(
            f"{moduleName}.downloadCopernicusSentinel2Products: catched exception: {e}"
            )

    return downloaded

credentials = {
    "username" : '*****************',
    "password" : '********'
}
#filePath= rf"c:\temp\Grapevine\\"
filePath= rf"/projects/grapevine/GIT/src/data/copernicusNew/"

start_date = "2023-10-01"
end_date = "2024-01-01"
aoi = "POLYGON((-0.999755859375 42.99661231842139, \
-2.3565673828124996 41.31082388091818, \
-1.5216064453125 39.76210275375139, \
-0.39001464843749994 39.87601941962116, \
1.043701171875 41.42625319507269, \
0.7800292968749999 42.85180609584705,  \
-0.999755859375 42.99661231842139))'"

aoi = "POLYGON((-2 42.93, \
1 42.93, \
1 39.9, \
-2 39.9, \
-2 42.93))'"


cloud_coverage = 40.00

downloadCopernicusSentinel2Products(credentials=credentials, destinationFilePath=filePath,
        start_date=start_date, end_date=end_date, aoi=aoi, cloud_coverage=cloud_coverage)