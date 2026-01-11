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
            datetime.datetime.strptime(self.startDate, '%Y%m%d')
        except ValueError:
            raise ValueError("ProductsDownloader.exist name parameters['startDate'] has an incorrect data format, should be YYYYMMDD")

        self.endDate = parameters["endDate"]
        try:
            datetime.datetime.strptime(self.endDate, '%Y%m%d')
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

        products = api.query(footprint,
                            date = (self.startDate, self.endDate),
                            platformname = 'Sentinel-2',
                            #processinglevel = 'Level-2A',
                            cloudcoverpercentage = (0, 10))
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
        