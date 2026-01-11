import os
import sys
import geopandas as gpd
import pandas as pd
import rasterio
import json
from rasterstats import zonal_stats
import shapely
import numpy as np
np.set_printoptions(threshold=sys.maxsize)
import glob
import shutil
import config as cf
from sqlalchemy import create_engine

import warnings

warnings.filterwarnings("ignore")
logger= cf.logger

import requests
from requests.auth import HTTPBasicAuth

from sqlalchemy import create_engine
#from config import *

# import warnings
# warnings.filterwarnings("ignore")

class DatasetsGenerator():

    def __init__(self, parameters:dict=None):
        
        self.__PARAMETERSKEYS = ["inputFolder", "outputFolder","parcelsGeojsonPath"]

        if parameters is None or not isinstance(parameters, dict):
            raise Exception ("DatasetsGenerator.exist name parameter parameters must be a non empty dictionary." )
        
        self.parameters = parameters

        keys = parameters.keys()

        if len(keys) == 0:
            raise Exception ("DatasetsGenerator.exist name parameter parameters must be a non empty dictionary." )
        for key in self.__PARAMETERSKEYS:
            if not key in keys:
                raise Exception ("DatasetsGenerator.exist name parameter parameters does not contain parameter: "+ str(key) + " review your code" )
            value = parameters[key]

            if value is None:
                raise Exception ("DatasetsGenerator.exist name parameter parameters[ "+ str(key) + "] dose not provide any value" )

        self.inputFolder = parameters["inputFolder"]
        if not os.path.exists(self.inputFolder) or not os.path.isdir(self.inputFolder):
            raise Exception ("DatasetsGenerator.exist name parameters['inputFolder'] does not provide the path to a valid folder:"+ self.inputFolder )

        self.outputFolder = parameters["outputFolder"]
        if not os.path.exists(self.outputFolder) or not os.path.isdir(self.outputFolder):
            os.makedirs(self.outputFolder)
            #raise Exception ("DatasetsGenerator.exist name parameters['outputFolder'] does not provide the path to a valid folder:"+ self.outputFolder )

        self.parcelsPath = parameters["parcelsGeojsonPath"]
        if not os.path.exists(self.parcelsPath):
            raise Exception ("DatasetsGenerator.exist name parameters['parcelsGeojsonPath'] does not provide the path to a geojson:"+ self.parcelsPath )    

        self.parcels = gpd.read_file(self.parcelsPath).to_crs({'init': 'epsg:4326'})
        #self.parcels["geometry"] = self.parcels.geometry.map(lambda multipolygon: shapely.ops.transform(lambda x, y: (y, x), multipolygon))
        self.parcels.crs = "EPSG:4326"


    def __pixels(self, x):
        #data,
        return x.data.tolist()

    def __get_ndvi_from_picture(self, picture):
        #TODO iterate over the parcels to get the index value
        stats = zonal_stats(
                vectors=self.parcels['geometry'], 
                raster=picture, 
                stats=['mean', 'min', 'max', 'median','std', 'count'],
                add_stats = {'pixels_array':self.__pixels},
                all_touched=True
            )
        return stats

    def generate_datasets(self, processed):
    
        if os.path.exists(self.outputFolder +"/generated.txt"):
            txt = open(self.outputFolder +"/generated.txt", "r")
            ids = txt.read().split('\n')
        else:
            ids=[]
        
        files=glob.glob(self.inputFolder +'/*.tiff')
        logger.debug('Generated datasets: {}'.format(files))
        
        processed=[file.split('/')[-1] for file in files if file.split('/')[-1] not in ids]
        try:
#             files = files = [file.split('/')[-1] for file in processed]#os.listdir(self.inputFolder)
#             if 'localId' in self.parcels.columns:
#                 p_csv = self.parcels[['localId']].copy()
#             else:
#                 p_csv = self.parcels[[self.parcels.columns[0]]].copy()

            for file in processed:
                if 'localId' in self.parcels.columns:
                    p_csv = self.parcels[['localId']].copy()
                else:
                    p_csv = self.parcels[[self.parcels.columns[0]]].copy()
                logger.debug(file[:-5])
                #TODO get the date from the picture name
                
                #tener en cuenta la parte "T31TBG" porque si no no se guarda toda la info
                id_csv = file.split('_')[0]
                date_image = file.split('_')[-1].split('T')[0]
                date_image = date_image[:4] + '-' + date_image[4:6] + '-' + date_image[6:]
                #extract the ndvi from the picture for each parcel
                stats = self.__get_ndvi_from_picture(self.inputFolder +file)
                
                #id parcela / fecha / valores anteriores CSV
                p_csv['date'] = date_image
                
                parcels_stats = pd.DataFrame(stats)
                parcels_stats = parcels_stats[parcels_stats['max'] != 0]
                parcels_stats.rename(columns = {'count': 'pixels'}, inplace = True)
                
                p_csv = pd.concat([p_csv, parcels_stats], axis=1)
#                 print(p_csv)
                p_csv = p_csv.dropna(subset=['mean', 'min', 'max', 'std'])
                
                p_csv['tesela']=id_csv
                #store the ndvis in the dataset with the best format
                p_csv.to_csv(self.outputFolder + '/' + id_csv + '_' + date_image+'.csv', sep=';')
                
                p_csv=p_csv.rename(columns={'median':'meidan'})
                p_csv=p_csv.drop(labels=['pixels'],axis=1)

                
                engine = create_engine('postgresql://' + cf.postgress_Username + ':' + cf.postgress_Password + '@' + cf.postgress_Host + ':' + str(cf.postgress_Port) + '/' + cf.postgress_Database)
                
                p_csv.to_sql('copernicus_nvdi', engine, schema='copernicus', if_exists='append',
                                                         index=False, chunksize=1000, dtype=None, method=None)
                
            for file in glob.glob(self.inputFolder +'/*.tiff'):
                os.remove(file)
            for file in glob.glob(self.inputFolder.replace('/ndvi_images','/tiffs') + '/*.tiff') :
                os.remove(file)
                
                
        
        except Exception as e:
            raise Exception("DatasetsGenerator.generate_datasets catches exception:"+ str(e))