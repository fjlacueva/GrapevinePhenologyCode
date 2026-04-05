import geopandas as gpd
from sentinelsat.sentinel import SentinelAPI
import rasterio 
import pandas
from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import shapely
import os
import datetime
import config
import logging
import traceback
import zipfile
from os import listdir
import glob
import os
import requests
from creds import *

logger= config.logger

class ProductsDownloader():

    def __init__(self, parameters:dict=None):

        self.__PARAMETERSKEYS = ["boundaryGeojsonPath","parcelsGeojsonPath", "outputFolder","startDate", "endDate"]

        if parameters is None or not isinstance(parameters, dict):
            raise Exception ("ProductsDownloader.exist name parameter parameters must be a non empty dictionary." )
        
        self.parameters = parameters

        keys = parameters.keys()

        if len(keys) == 0:
            raise Exception ("ProductsDownloader.exist name parameter parameters must be a non empty dictionary." )
        for key in self.__PARAMETERSKEYS:
            if not key in keys:
                raise Exception ("ProductsDownloader.exist name parameter parameters does not contain parameter: "+ str(key) + " review your code" )
            value = parameters[key]

            if value is None:
                raise Exception ("ProductsDownloader.exist name parameter parameters[ "+ str(key) + "] dose not provide any value" )

        self.boundaryPath = parameters["boundaryGeojsonPath"]
        if not os.path.exists(self.boundaryPath):
            raise Exception ("ProductsDownloader.exist name parameters['boundaryGeojsonPath'] does not provide the path to a geojson:"+ self.boundaryPath )

        self.parcelsPath = parameters["parcelsGeojsonPath"]
        if not os.path.exists(self.parcelsPath):
            raise Exception ("ProductsDownloader.exist name parameters['parcelsGeojsonPath'] does not provide the path to a geojson:"+ self.parcelsPath )     

        self.outputFolder = parameters["outputFolder"]
        if not os.path.exists(self.outputFolder) or not os.path.isdir(self.outputFolder):
            os.makedirs(self.outputFolder)
            #raise Exception ("ProductsDownloader.exist name parameters['outputFolder'] does not provide the path to a valid folder:"+ self.outputFolder )

        self.startDate = parameters["startDate"]
        try:
            datetime.datetime.strptime(self.startDate, '%Y-%m-%d')
        except ValueError:
            raise ValueError("ProductsDownloader.exist name parameters['startDate'] has an incorrect data format, should be YYYYMMDD")

        self.endDate = parameters["endDate"]
        try:
            datetime.datetime.strptime(self.endDate, '%Y-%m-%d')
        except ValueError:
            raise ValueError("ProductsDownloader.exist name parameters['endDate'] has an incorrect data format, should be YYYYMMDD")
            
        self.downloaded=[]

    def __readParcels(self):
        parcels = gpd.read_file(self.parcelsPath).to_crs({'init': 'epsg:4326'})
        #parcels["geometry"] = parcels.geometry.map(lambda multipolygon: shapely.ops.transform(lambda x, y: (y, x), multipolygon))
        parcels.crs = "EPSG:4326"

        #print(parcels.head())

        return parcels

    def __readBoundary(self):
        pol = gpd.read_file(self.boundaryPath)

        return pol
       
    def __apiQuery(self):

        footprint =self.__readBoundary()['geometry'].iloc[0].convex_hull
        logger.debug('Invoking URL {};credentials:{};***'.format(config.Sentinel_URL, config.Sentinel_user))
        api = SentinelAPI(config.Sentinel_user,config.Sentinel_password, config.Sentinel_URL)
        #desde principio 2021 hasta ahora

        # download single scene by known product id
        #api.download(1)

        products = api.query(footprint,
                            date = (self.startDate, self.endDate),
                            platformname = 'Sentinel-2',
                            #processinglevel = 'Level-2A',
                            cloudcoverpercentage = (0, 30))

        api.download_all(products)

        products = api.to_geodataframe(products)

        self.api = api
        
        return products
    
    def download(self):
        try:
            if not os.path.exists(self.outputFolder + config.PRODUCTS_ZIPS_PATH):
                os.mkdir(self.outputFolder + config.PRODUCTS_ZIPS_PATH)
            
            if not os.path.exists(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/downloaded.txt'):
                os.open(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/downloaded.txt', os.O_CREAT)

            if not os.path.exists(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/downloads_log.txt'):
                os.open(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/downloads_log.txt',os.O_CREAT)

            
            prev_downloaded=glob.glob(self.outputFolder+ config.PRODUCTS_ZIPS_PATH+'/*.zip')
            #check the products with parcels inside
            parcels = self.__readParcels()
            products = self.__apiQuery()

            par_gs = parcels["geometry"]
            pro_gs = products["geometry"]

            products["valid"] = False

            products["valid"] = products["geometry"].apply(lambda x: True if par_gs.within(x).any() else False)
            valids = products[products["valid"]==True]
            
            #download these products
            valids = valids.drop(['valid'], axis=1)

            logging.basicConfig(filename=self.outputFolder+ config.PRODUCTS_ZIPS_PATH +"/downloads_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')

            txt = open(self.outputFolder+ config.PRODUCTS_ZIPS_PATH +"/downloaded.txt", "r")
            ids = txt.read().split('\n')
            txt.close()
            downloaded = 0
            ids_added = []

            logger.debug("Copernicus products 2B downloaded: " + str(len(valids)))
            for idx, valid in valids.iterrows():
                logging.debug("Checking product idx({})...".format(idx))

                if idx in ids:
                    logging.info("The product with id {} is downloaded already".format(idx))
                else:
                    try:
                        self.api.download(idx, directory_path=self.outputFolder + config.PRODUCTS_ZIPS_PATH )
                        with open(self.outputFolder + config.PRODUCTS_ZIPS_PATH + "/downloaded.txt", "a") as txt:
                            txt.write(idx)
                            txt.write('\n')
                        downloaded += 1
                    except Exception as e:
                        try:
                            validDesc= str({'title':valid.title,'link':valid.link,'summary':valid.summary})
                        except:
                            try:
                                validDesc= valid.title
                            except:
                                validDesc= str(valid)
                        logger.warning("Product {} has not been downloaded. Exception details: {}".format(validDesc, str(e)))
#                         try:
#                             command = config.CLI_DOWNLOAD.format(USERNAME=config.USER, PASSWORD=config.PASSWORD, ZIP_FOLDER=self.outputFolder + config.PRODUCTS_ZIPS_PATH, PRODUCT_NAME=valid['title'], ID_PRODUCT=idx)
#                             print(command)
#                             os.system(command)
#                             with open(self.outputFolder + config.PRODUCTS_ZIPS_PATH + "/downloaded.txt", "a") as txt:
#                                 txt.write(idx)
#                                 txt.write('\n')
#                             downloaded += 1
#                         except Exception as e:
#                             logging.error("ProductsDownloader.download The product {} has not been downloaded due to: {}".format(valid, traceback.format_exc()))
            last_downloaded=glob.glob(self.outputFolder+ config.PRODUCTS_ZIPS_PATH+'/*.zip')            
            self.downloaded=[ele for ele in last_downloaded if ele not in prev_downloaded]
            return "{}/{} products downloaded".format( str(downloaded), str(len(valids.index)))
        except Exception as e:
            raise Exception("ProductsDownloader.download catches exception:"+ str(e))

    def getCopernicusAccessToken(self,credentials:dict =None) -> str:

        moduleName = config.moduleName
        
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
    
    def downloadCopernicusSentinel2Products(self)->bool:
               
        moduleName= config.moduleName
        downloaded=config.downloaded
        productsFilter = config.productsFilter
        data_collection = config.data_collection
        credentials = config.credentials
        destinationFilePath = config.filePath
        cloud_coverage = config.cloud_coverage
        
        prev_downloaded=glob.glob(self.outputFolder+ config.PRODUCTS_ZIPS_PATH+'/*.zip')
        
        parcels = self.__readParcels()
        
        logging.basicConfig(filename=self.outputFolder+ config.PRODUCTS_ZIPS_PATH +"/downloads_log.txt",
            level=logging.DEBUG,
            format='%(levelname)s: %(asctime)s %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S')

        txt = open(self.outputFolder+ config.PRODUCTS_ZIPS_PATH +"/downloaded.txt", "r")
        ids = txt.read().split('\n')
        txt.close()
        
        #aoi = self.__readBoundary()['geometry'].iloc[0].convex_hull
        #aoi = str(aoi)
        aoi = config.aoi      

        if self.startDate is None or len(self.startDate)==0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter start_date is not correctly set.")
        if self.endDate is None or len(self.endDate)==0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter end_date is not correctly set.")
        
        if credentials is None or len(credentials.keys())==0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter credentials is not correctly set.")
        if destinationFilePath is None or len(destinationFilePath)==0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter destinationFilePath is not correctly set.")
        
        if cloud_coverage is None or cloud_coverage<0.0 or cloud_coverage>100.0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter cloud_coverage is not correctly set. It must be a float value in range [0.0, 100.0]")


        if aoi is None ==0:
            raise Exception(f"{moduleName}.downloadCopernicusSentinel2Products: the parameter area of interest (aoi) is not correctly set.")


        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(<productID>)/$value"

        # json = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 40.00) and ContentDate/Start gt 2022-01-01T00:00:00.000Z and ContentDate/Start lt 2022-01-03T00:00:00.000Z&$top=10").json()
        # df = pd.DataFrame.from_dict(json['value'])

        try:
            # Termino de verificar los parámetros
            start_date_obj = datetime.datetime.strptime(self.startDate, "%Y-%m-%d")
            if start_date_obj >= datetime.datetime.today():
                raise Exception(f"The parameter start_date ({self.startDate})is a future date.")
            end_date_obj = datetime.datetime.strptime(self.endDate, "%Y-%m-%d")
            if start_date_obj>= end_date_obj:
                raise Exception(f"The parameter start_date ({self.startDate}) can not be later than end_date ({self.endDate})")
            
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
            f"and ContentDate/Start gt {self.startDate}T00:00:00.000Z "+ \
            f"and ContentDate/Start lt {self.endDate}T00:00:00.000Z "  +  \
            f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_coverage})" + \
            f"&$orderby=ContentDate/Start%20asc"+ \
            f"&$top=1000"            
                      
            response = requests.get(catalogQueryURL)
            response.raise_for_status()
            products = response.json()

            keycloak_token = self.getCopernicusAccessToken(credentials=credentials)

            session = requests.Session()
            session.headers.update({'Authorization': f'Bearer {keycloak_token}'})

            downloaded_products = 0
            download_ids = []
            
            for product in products["value"]:
                id = product["Id"]              
                if id not in ids:
                    logger.debug('Id : ' + str(id))
                    name = product["Name"]
                    url2Download = url.replace("<productID>", id)
                    response = session.get(url2Download, allow_redirects=False)
                    crossed = 0
                    while response.status_code in (301, 302, 303, 307) and crossed<config.maximum_crossed:
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
                                        crossed = crossed + 1
                            else:
                                if response.reason=='Unauthorized':
                                    keycloak_token = self.getCopernicusAccessToken(credentials=credentials)
                                    session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                                    response = session.get(url2Download, allow_redirects=False)
                                else:
                                    if response.reason=='Expired signature!':
                                        keycloak_token = self.getCopernicusAccessToken(credentials=credentials)
                                        session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                                        response = session.get(url2Download, allow_redirects=False)
                                    else:
                                        logger.error(f"{moduleName}.downloadCopernicusSentinel2Products got reason: {response.reason} to HTTP error 401 when downloading product:{name}")
                                        crossed = crossed + 1

                        except Exception as e:
                            logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: catches exception e: {e}\n\t when trying to write file: {filePath2Write}")
                            crossed = crossed + 1
                    if response.status_code >=200 and response.status_code< 300:
                        logger.debug('statud_code : ' + str(200))                        
                        file = session.get(url2Download, verify=False, allow_redirects=True)
                        filePath2Write = destinationFilePath + name + '.zip'
                        logger.debug('Product download : ' + str(id) + " name " + str(name))
                        try:
                            with open(filePath2Write, 'wb') as p:
                                p.write(file.content)
                            with open(self.outputFolder + config.PRODUCTS_ZIPS_PATH + "/downloaded.txt", "a") as txt:
                                txt.write(id)
                                txt.write('\n')
                            downloaded_products += 1
                            download_ids.append(id)
                            logger.debug('Download_products : ' + str(downloaded_products))
                            logger.debug('download_ids : ' + str(download_ids))
                            if config.first_interaction_stop == True:
                                break
                        except Exception as e:
                            logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: catches exception e: {e}\n\t when trying to write file: {filePath2Write}")
                    else:
                        #logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: not able to download Sentinel-2 product {name}. When requesting to dataspace.copernicus.eu I recived the error {file.status_code} and the message {file.text}. ")
                        if response.status_code==401 and response.reason=='Unauthorized':
                            keycloak_token = self.getCopernicusAccessToken(credentials=credentials)
                            session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                        else:
                            if response.status_code==401 and response.reason=='Expired signature!':
                                keycloak_token = self.getCopernicusAccessToken(credentials=credentials)
                                session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
                            else:
                                logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: not able to download Sentinel-2 product {name}. When requesting to dataspace.copernicus.eu I recived the error {file.status_code} and the message {file.text}. ")
        
                else:
                    logger.error(f"{moduleName}.downloadCopernicusSentinel2Products: The file {id} is already processed previously. ")
        
        except Exception as e:
            raise Exception(
                f"{moduleName}.downloadCopernicusSentinel2Products: catched exception: {e}"
                )

        last_downloaded=glob.glob(self.outputFolder+ config.PRODUCTS_ZIPS_PATH+'/*.zip')            
        self.downloaded=[ele for ele in last_downloaded if ele not in prev_downloaded]
        return "{}/{} products downloaded".format( str(downloaded_products), str(len(download_ids)))

    def unzip(self):
        prev_extracted=glob.glob(self.outputFolder+ config.PRODUCTS_PATH+'/*')           
        if not os.path.exists(self.outputFolder + config.PRODUCTS_PATH ):
            os.mkdir(self.outputFolder + config.PRODUCTS_PATH)

#         onlyfiles = [f for f in listdir(self.outputFolder+ config.PRODUCTS_ZIPS_PATH + '/') if os.path.isfile(os.path.join(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/', f))]
        onlyfiles=self.downloaded
        unzipped = 0

        logging.basicConfig(filename=self.outputFolder+ config.PRODUCTS_ZIPS_PATH +"/downloads_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')

        for file in onlyfiles:
            if file not in ["downloaded.txt", "downloads_log.txt"]:
                logger.debug(file)
                try:
#                     with zipfile.ZipFile(self.outputFolder + config.PRODUCTS_ZIPS_PATH + '/' + file, 'r') as zip_ref:
                    with zipfile.ZipFile(file, 'r') as zip_ref:
                        zip_ref.extractall(self.outputFolder + config.PRODUCTS_PATH)
                    unzipped += 1
                
                except zipfile.BadZipFile as e:
                    logging.error("ProductsDownloader.download The product {} has not been downloaded due to: {}".format(file, traceback.format_exc()))
                    continue
                except Exception as e:
                    logging.error("ProductsDownloader.download The product {} has not been downloaded due to: {}".format(file, traceback.format_exc()))
                    continue
                os.remove(file)
        
            logger.debug("" + str(unzipped) + "products unzipped of " + str(len(onlyfiles)-2))
        last_extracted=glob.glob(self.outputFolder+ config.PRODUCTS_PATH+'/*')  
        return [file for file in last_extracted if file not in prev_extracted]
        