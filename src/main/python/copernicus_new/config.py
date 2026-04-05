import logging
import os
import sys

paths2Libraries=['']
for path2Library in paths2Libraries:
    if os.path.isdir(path2Library):
      sys.path.append(path2Library)

#logging
########
logger = logging.getLogger(__name__)
if not logger.handlers:
  loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'Copernicus')
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  fh = logging.FileHandler(loggingPath, mode="w", encoding="utf-8")
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))
dataPath=os.getenv('DATAPATH', '/data')

# CREDENTIALS
Sentinel_user = '********'
Sentinel_password = '*********'
#Sentinel_URL= 'https://dataspace.copernicus.eu'
Sentinel_URL='https://apihub.copernicus.eu/apihub'


#INTERNAL PATHS
PRODUCTS_ZIPS_PATH = r'/products_zips'
PRODUCTS_PATH = r'/products'
TIFFS_PATH = r'/tiffs'
NDVIS_PATH = r'/ndvi_images'

#COMMAND LINE INTERFACE DOWNLOAD 
# CLI_DOWNLOAD = 'wget --content-disposition --continue --user={USERNAME} --password={PASSWORD} -O {ZIP_FOLDER}/{PRODUCT_NAME}.zip "https://scihub.copernicus.eu/dhus/odata/v1/Products(\'{ID_PRODUCT}\')/\$value"'

#DATABASE:
postgress_Host        = "******.******.***"
postgress_Port        = ********
postgress_Database    ='*******'
postgress_Password    = "*********"
postgress_Username    = "********"

#folder_results='/projects/grapevine/GIT/src/data/copernicus/results'
#folder_ndvis='/projects/grapevine/GIT/src/data/copernicus/ndvis'
#folder_DOSomontano='/projects/grapevine/GIT/src/data/copernicus/DO_Somontano'

folder_results='/projects/grapevine/GIT/src/data/copernicusNew/results'
folder_ndvis='/projects/grapevine/GIT/src/data/copernicusNew/ndvis'
folder_DOSomontano='/projects/grapevine/GIT/src/data/copernicusNew/DO_Somontano'

parameters_downloader = {
    "boundaryGeojsonPath": r'./aragon_polygon.geojson',
    "parcelsGeojsonPath": r'./20210602controlparcels.geojson',#r'./notebooks/20210602controlparcels.geojson', #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson',
    "outputFolder": folder_results,#r'./notebooks/modules/results',
    "startDate": "2023-01-01",
    "endDate": "2024-01-20"
}

parameters_ndvi_generator = {
    "inputFolder": folder_results + '/products/', #r'./notebooks/modules/results/products/',
    "outputFolder": folder_ndvis, #r'./notebooks/modules/ndvis',
    "boundaryGeojsonPath": r'./aragon_polygon.geojson'
}

parameters_dataset_generator = {
    "inputFolder": folder_ndvis+'/ndvi_images/',#r'./notebooks/modules/ndvis/ndvi_images/',
    "outputFolder": folder_DOSomontano,#r'./notebooks/modules/datasets/DO_Somontano/',
    "parcelsGeojsonPath": r'./20210602controlparcels.geojson' #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson'
}

moduleName= "downloadCopernicusSentinel2Products"
downloaded=True
data_collection = "SENTINEL-2"
productsFilter = "'MSIL2A'"
credentials = {
    "username" : '**************',
    "password" : '************'
}
filePath= rf"/projects/grapevine/GIT/src/data/copernicusNew/results/products_zips/"
'''aoi = "POLYGON((-0.999755859375 42.99661231842139, \
-2.3565673828124996 41.31082388091818, \
-1.5216064453125 39.76210275375139, \
-0.39001464843749994 39.87601941962116, \
1.043701171875 41.42625319507269, \
0.7800292968749999 42.85180609584705,  \
-0.999755859375 42.99661231842139))'"'''

aoi = "POLYGON((-2 42.93, \
1 42.93, \
1 39.9, \
-2 39.9, \
-2 42.93))'"

cloud_coverage = 15.00
maximum_crossed = 1000
first_interaction_stop = False