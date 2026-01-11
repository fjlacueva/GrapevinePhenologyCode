import logging
from logging.handlers import SysLogHandler
from logging.handlers import RotatingFileHandler
import os
import sys
PROJECTNAME='GRAPEVINE.dataTransformation'

dataPath=os.getenv('DATAPATH', '/data')

paths2Libraries=['']
for path2Library in paths2Libraries:
    if os.path.isdir(path2Library):
      sys.path.append(path2Library)
# Try to import ENV.$PYTHONPATHS paths
paths2Libraries=os.getenv('PYTHONPATHS')
if paths2Libraries != None:
    paths2Libraries= paths2Libraries.split(',')
    for path2Library in paths2Libraries:
        if os.path.isdir(path2Library):
          sys.path.append(path2Library)

#logging
########
logger = logging.getLogger(__name__)
if not logger.handlers:
  loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), PROJECTNAME)
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  # fh = logging.FileHandler(loggingPath, mode="a", encoding="utf-8")
  fh = RotatingFileHandler(loggingPath, maxBytes=int(os.getenv('LOGFILESIZEMAXBYTES', 5*1024*1024)), backupCount=5, encoding="utf-8", mode="a") # maxBytes=5MB
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  # SysLogHandler to store only ERRORS on it
  # Correct configuration found at https://signoz.io/blog/python-syslog/
  # How to install a rsyslog: https://www.manageengine.com/products/eventlog/logging-guide/syslog/configuring-ubuntu-lts-as-rsyslog-server.html
  # How to setup a logger to send to a rsyslog: https://stackoverflow.com/questions/38907637/quick-remote-logging-system
  remoteSysLogInfo=os.getenv('REMOTESYSLOGINFO', None) # Format [host, port]. eg: 'artemisa.******.**',514
  if remoteSysLogInfo != None:
    remoteSysLogInfo= remoteSysLogInfo.split(',')
    rSyslogHost=remoteSysLogInfo[0].strip()
    rSyslogPort=int(remoteSysLogInfo[1].strip())
    sysLogHandler= logging.handlers.SysLogHandler(address = (rSyslogHost, rSyslogPort))
    # sysLogHandler = SysLogHandler(facility=SysLogHandler.LOG_DAEMON, address='/dev/log')
    formatter= logging.Formatter("%(asctime)s:%(levelname)7s:{}.%(filename)s[%(lineno)04d]: %(message)s".format(PROJECTNAME),"%H%M%S")
    sysLogHandler.setFormatter(formatter)
    logger.addHandler( sysLogHandler)
    sysLogHandler.setLevel( logging.ERROR)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))

#########
EXECUTOR_PROPAGATE_EXCEPTIONS=False
# WebServer config
##################
serverPort=5000
future_timeAliveSgs=36000

#Common variables
postgress_Host        = "artemisa.******.**"
postgress_Port        = ******
postgress_Database    ='*****'
postgress_Password    = "*****"
postgress_Username    = "******"
FTP_SERVER = "artemisa.******.**"
FTP_PORT = "****"
FTP_USER = "*****"
FTP_PASSWORD = "*******"


cmds2CreatePythonTmpEnv1=['~cp /pythonSarga/*.py $WORKINGFOLDER -R',
                           '~cp /pythonModelos_climaticos/*.py $WORKINGFOLDER -R',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']

####################################
# vvv realClimateDataCalculus vvv  #
####################################
realClimateDataCalculus_odExcelFolder = '{}/DatosIntermedios/'.format(dataPath)
realClimateDataCalculus_SIARstationGISFile = '{}/20200604_Climatic_And_Parcel_Data.xlsx'.format(dataPath)

realClimateDataCalculus_loggingPath = '{}/realClimateDataCalculus/realClimateDataCalculus.log'.format(
  os.getenv('LOGSPATH', '/logs'))

inicio = False
inicioYear = 2023

allStations = True
stations = ['GV10']

GublerCalculation = {
   "hourlySelectedFields" : ['estacion', 'ubi', 'fecha', 'hourFrac', 'season', 'tmed'],
  #  "hourlyFields" : ['intervalTemperatureBellow_21_C', 'intervalTemperatureIn_21_30_C', 'intervalTemperatureAbove_35'],
   "hourlyFields" : [ 'intervalTemperatureIn_21_30_C' ],
   "hourlyGroupByFields" : ['estacion', 'ubi', 'fecha', 'hourFrac', 'season'],

   "dailySelectedFields" : ['indicativo', 'date'],
   "dailyFields" : ['hoursIn_21_30_C', 'tmed_min', "dailyContribution", "Gublert0Aux1", "Gublert0Aux2", "Gublert0Aux3", "Gublert0Aux4", "Gublert0Aux4", "Gublert0"],
   "dailyGroupByFields" : ['estacion', 'ubi', 'season'],
   "schema" : 'ITAINNOVA',
   "hourlytable" : 'GublerIndexHourlyData',
   "dailytable" : 'GublerIndexData'
}
####################################
# ^^^ realClimateDataCalculus ^^^  #
####################################

CVCSIndex_endPoint='/CVCSIndex'
CVCSIndex_prefix='CVCSIndex'

forecastDataUnion_endPoint='/forecastDataUnion'
forecastDataUnion_prefix='forecastDataUnion'

inicio_year=2016

DataAemet_endPoint='/dataAemet'
DataAemet_prefix='dataAemet-'
DataAemet_cmds=[ *cmds2CreatePythonTmpEnv1,
                '$PYTHONENV/bin/python $WORKINGFOLDER/aemet0.py',
                '$PYTHONENV/bin/python $WORKINGFOLDER/aemet2pg.py']
# Following vars are needed on docker to invoke the aemet0.py
DataAemet_DataFolder  ='/data/'
DataAemet_APIKeyFile  ='/pythonSarga/apikey.ita'

DataSiar_endPoint='/dataSiar'
DataSiar_prefix='dataSiar-'
DataSiar_cmds=[ *cmds2CreatePythonTmpEnv1,
                '/python/scripts/siar-download-insert.sh']


DataFaraNetwork_endPoint='/dataFaraNetwork'
DataFaraNetwork_prefix='dataFaraNetwork-'
DataFaraNetwork_cmds=[ *cmds2CreatePythonTmpEnv1,
                       'LOGLEVEL=INFO ; $PYTHONENV/bin/python $WORKINGFOLDER/load_data_ftp_server_improve.py']


predictionsAPIMeteo_endPoint='/predictionsAPIMeteo'
predictionsAPIMeteo_prefix='predictionsAPIMeteo-'
cmds2Create_predictionsAPIMeteo=['~mkdir $WORKINGFOLDER/common -p',
                           '~cp /pythonForecastAPI/* $WORKINGFOLDER -R',
                          #  '~cp /python/config.py $WORKINGFOLDER/common',
                           ]
predictionsAPIMeteo_cmds=[ *cmds2Create_predictionsAPIMeteo,
                           'LOGLEVEL=DEBUG ;cd $WORKINGFOLDER; $PYTHONENV/bin/python TestGetDataFromUrlToDatabase.py']



# climaticDataSiarWinkle parameters
odExcelFolder = '{}/DatosIntermedios/'.format(dataPath)
SIARstationGISFile = '{}/20200604_Climatic_And_Parcel_Data.xlsx'.format(dataPath)

##################################
# vvv copernicus parameters vvv  #
##################################
# CREDENTIALS
Sentinel_user = '********'
Sentinel_password = '********'
Sentinel_URL= 'https://scihub.copernicus.eu/dhus'

#INTERNAL PATHS
PRODUCTS_ZIPS_PATH = r'/products_zips'
PRODUCTS_PATH = r'/products'
TIFFS_PATH = r'/tiffs'
NDVIS_PATH = r'/ndvi_images'

#COMMAND LINE INTERFACE DOWNLOAD 
# CLI_DOWNLOAD = 'wget --content-disposition --continue --user={USERNAME} --password={PASSWORD} -O {ZIP_FOLDER}/{PRODUCT_NAME}.zip "https://scihub.copernicus.eu/dhus/odata/v1/Products(\'{ID_PRODUCT}\')/\$value"'

folder_results='/data/copernicus/results'
folder_ndvis='/data/copernicus/ndvis'
folder_DOSomontano='/data/copernicus/DO_Somontano'
parameters_downloader = {
    "boundaryGeojsonPath": r'./aragon_polygon.geojson',
    "parcelsGeojsonPath": r'./20210602controlparcels.geojson',#r'./notebooks/20210602controlparcels.geojson', #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson',
    "outputFolder": folder_results,#r'./notebooks/modules/results',
    "startDate": "20220701",
    "endDate": "20220722"
}

parameters_ndvi_generator = {
    "inputFolder": folder_results + '/products/', #r'./notebooks/modules/results/products/',
    "outputFolder": folder_ndvis, #r'./notebooks/modules/ndvis',
    "boundaryGeojsonPath": r'aragon_polygon.geojson'
}

parameters_dataset_generator = {
    "inputFolder": folder_ndvis+'/ndvi_images/',#r'./notebooks/modules/ndvis/ndvi_images/',
    "outputFolder": folder_DOSomontano,#r'./notebooks/modules/datasets/DO_Somontano/',
    "parcelsGeojsonPath": r'./20210602controlparcels.geojson' #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson'
}
################################################################
# vvv forecastDataUnion (aka createFinalSheet) parameters vvv  #
################################################################
#DATABASE:
postgress_Host        = "artemisa.******.**"
postgress_Port        = ******
postgress_Database    ='*****'
postgress_Password    = "*******"
postgress_Username    = "******"

#CREATE SABANA
full_sabana=False
year_sabana=''
inicio_year=2016
variedades=['GARNACHA', 'CHARDONNAY', 'CABERNET SAUVIGNON', 'MAZUELA', 'SYRACH', 'TEMPRANILLO']
dic_variedades_replace={'CABERNET-SAUVIGNON':'CABERNET SAUVIGNON','GARNACHA TINTA':'GARNACHA'}
variables_diarias_min=['tmed_min']
variables_diarias_max=['tmed_max']
variables_diarias_mean=['tmed_mean', 'hr_mean', 'wind_N', 'wind_NE', 'wind_E','wind_SE', 'wind_S', 'wind_SW', 
                        'wind_W', 'wind_NW']
variables_semanales=['gdd_4.5_t0_Tbase_sum',
       'gdd_4.5_t0_TbaseMax_sum', 'gdd_4.5_1_Tbase_sum',
       'gdd_4.5_1_TbaseMax_sum', 'gdd_4.5_2_Tbase_sum',
       'gdd_4.5_2_TbaseMax_sum', 'gdd_10.0_t0_Tbase_sum',
       'gdd_10.0_t0_TbaseMax_sum', 'gdd_10.0_1_Tbase_sum',
       'gdd_10.0_1_TbaseMax_sum', 'gdd_10.0_2_Tbase_sum',
       'gdd_10.0_2_TbaseMax_sum', 'chillingDD_7.0_t0_Tbase_sum',
       'chillingDD_7.0_t0_Tbasemin_sum', 'chillingDD_7.0_t0_Utah_sum',
       'chillingDD_7.0_1_Tbase_sum', 'chillingDD_7.0_1_Tbasemin_sum',
       'chillingDD_7.0_1_Utah_sum', 'chillingDD_7.0_2_Tbase_sum',
       'chillingDD_7.0_2_Tbasemin_sum', 'chillingDD_7.0_2_Utah_sum', 'rad_sum',
       'precip_sum', 'winkler_4.5_Tbase', 'winkler_4.5_TbaseMax',
       'winkler_10.0_Tbase', 'winkler_10.0_TbaseMax',
       'gdd_4.5_t0_Tbase_sum_Cumm', 'gdd_4.5_t0_TbaseMax_sum_Cumm',
       'gdd_4.5_1_Tbase_sum_Cumm', 'gdd_4.5_1_TbaseMax_sum_Cumm',
       'gdd_4.5_2_Tbase_sum_Cumm', 'gdd_4.5_2_TbaseMax_sum_Cumm',
       'gdd_10.0_t0_Tbase_sum_Cumm', 'gdd_10.0_t0_TbaseMax_sum_Cumm',
       'gdd_10.0_1_Tbase_sum_Cumm', 'gdd_10.0_1_TbaseMax_sum_Cumm',
       'gdd_10.0_2_Tbase_sum_Cumm', 'gdd_10.0_2_TbaseMax_sum_Cumm',
       'chillingDD_7.0_t0_Tbase_sum_Cumm',
       'chillingDD_7.0_t0_Tbasemin_sum_Cumm',
       'chillingDD_7.0_t0_Utah_sum_Cumm', 'chillingDD_7.0_1_Tbase_sum_Cumm',
       'chillingDD_7.0_1_Tbasemin_sum_Cumm', 'chillingDD_7.0_1_Utah_sum_Cumm',
       'chillingDD_7.0_2_Tbase_sum_Cumm', 'chillingDD_7.0_2_Tbasemin_sum_Cumm',
       'chillingDD_7.0_2_Utah_sum_Cumm', 'rad__t0__Cumm', 'rad__1__Cumm',
       'rad__2__Cumm', 'precip__t0__Cumm', 'precip__1__Cumm',
       'precip__2__Cumm', 'winkler_4.5_t0_Tbase_Cumm',
       'winkler_4.5_t0_TbaseMax_Cumm', 'winkler_4.5_1_Tbase_Cumm',
       'winkler_4.5_1_TbaseMax_Cumm', 'winkler_4.5_2_Tbase_Cumm',
       'winkler_4.5_2_TbaseMax_Cumm', 'winkler_10.0_t0_Tbase_Cumm',
       'winkler_10.0_t0_TbaseMax_Cumm', 'winkler_10.0_1_Tbase_Cumm',
       'winkler_10.0_1_TbaseMax_Cumm', 'winkler_10.0_2_Tbase_Cumm',
       'winkler_10.0_2_TbaseMax_Cumm']

n_dias_atras=14
n_dias_alante=7

dos={1.:'Borja',4.:'Calatayud',6.:'Somontano',8.:'Cariñena'}
################################################################
# ^^^ forecastDataUnion (aka createFinalSheet) parameters ^^^  #
################################################################

##################################
# vvv copernicus parameters vvv  #
##################################
##################################
# ^^^ copernicus parameters ^^^  #
##################################

####################################################
### vvv dataValidation configuration variables vvv
####################################################
artemisaNGinx_url = 'https://artemisa.******.**/api/'
artemisaNGinx_user = "*******"
artemisaNGinx_password = "********"
aragonstationstable = 'estaragon'
weatherforecasttable = 'weatherforecast'
weatherforecastdatatable = 'weatherforecastdata'
user_data = 1
data_resolution = 4
data_provider = 4
columnNameLon = 'long'
columnNameLat = 'lat'
hours = 'hourly'
time = 'time'
nocountstations = ['Z28']
windspeed = 'windspeed_10m'
winddirection = 'winddirection_10m'
dewpoint = 'dewpoint_2m'
relativehumidity = 'relativehumidity_2m'
precipitation = 'precipitation'
directradiation = 'direct_radiation'
temperature = 'temperature_2m'
past_days = 2
code=0
long=18
lat=19
# It is used by Data For Completing Database
indicativo = 'indicativo'
store_table = 'estimate_arahorario'
index_hour = 18
index_day = 4
initialYear = 2022

# loggingPath = '/projects/grapevine/GIT/src/logs/ForecastAPI/log.log'
loggingPath = '{}/ForecastAPI/log.log'.format(os.getenv("LOGSPATH", '/logs'))
file_system_prediction="{}/ForecastAPI/dates_predictions.txt".format(os.getenv("DATAPATH", '/data'))
file_system_prediction_2="{}/ForecastAPI/dates_predictions2.txt".format(os.getenv("DATAPATH", '/data'))
#######################################################
### ^^^ dataValidation configuration variables ^^^
#######################################################


#######################################################
### vvv diseaseComputation configuration variables vvv
#######################################################
#CREATE SABANA
diseaseComputation_full_sabana=True
diseaseComputation_year_sabana=2021
# Defined at forecastDataUnion variedades=['GARNACHA', 'CHARDONNAY', 'CABERNET SAUVIGNON', 'MAZUELA', 'SYRACH', 'TEMPRANILLO']
# Defined at forecastDataUnion dic_variedades_replace={'CABERNET-SAUVIGNON':'CABERNET SAUVIGNON','GARNACHA TINTA':'GARNACHA'}
# Defined at forecastDataUnion variables_diarias_min=['tmed_min', 'rad_min']
# Defined at forecastDataUnion variables_diarias_max=['tmed_max', 'rad_max']
# Defined at forecastDataUnion variables_diarias_mean=['tmed_mean', 'rad_mean', 'hr_mean', 'wind_N', 'wind_NE', 'wind_E','wind_SE', 'wind_S', 'wind_SW', 
#                        'wind_W', 'wind_NW']
# Defined at forecastDataUnion variables_semanales=['gdd_4.5_t0_Tbase_sum',
#        'gdd_4.5_t0_TbaseMax_sum', 'gdd_4.5_1_Tbase_sum',
#        'gdd_4.5_1_TbaseMax_sum', 'gdd_4.5_2_Tbase_sum',
#        'gdd_4.5_2_TbaseMax_sum', 'gdd_10.0_t0_Tbase_sum',
#        'gdd_10.0_t0_TbaseMax_sum', 'gdd_10.0_1_Tbase_sum',
#        'gdd_10.0_1_TbaseMax_sum', 'gdd_10.0_2_Tbase_sum',
#        'gdd_10.0_2_TbaseMax_sum', 'chillingDD_7.0_t0_Tbase_sum',
#        'chillingDD_7.0_t0_Tbasemin_sum', 'chillingDD_7.0_t0_Utah_sum',
#        'chillingDD_7.0_1_Tbase_sum', 'chillingDD_7.0_1_Tbasemin_sum',
#        'chillingDD_7.0_1_Utah_sum', 'chillingDD_7.0_2_Tbase_sum',
#        'chillingDD_7.0_2_Tbasemin_sum', 'chillingDD_7.0_2_Utah_sum', 'rad_sum',
#        'precip_sum', 'winkler_4.5_Tbase', 'winkler_4.5_TbaseMax',
#        'winkler_10.0_Tbase', 'winkler_10.0_TbaseMax',
#        'gdd_4.5_t0_Tbase_sum_Cumm', 'gdd_4.5_t0_TbaseMax_sum_Cumm',
#        'gdd_4.5_1_Tbase_sum_Cumm', 'gdd_4.5_1_TbaseMax_sum_Cumm',
#        'gdd_4.5_2_Tbase_sum_Cumm', 'gdd_4.5_2_TbaseMax_sum_Cumm',
#        'gdd_10.0_t0_Tbase_sum_Cumm', 'gdd_10.0_t0_TbaseMax_sum_Cumm',
#        'gdd_10.0_1_Tbase_sum_Cumm', 'gdd_10.0_1_TbaseMax_sum_Cumm',
#        'gdd_10.0_2_Tbase_sum_Cumm', 'gdd_10.0_2_TbaseMax_sum_Cumm',
#        'chillingDD_7.0_t0_Tbase_sum_Cumm',
#        'chillingDD_7.0_t0_Tbasemin_sum_Cumm',
#        'chillingDD_7.0_t0_Utah_sum_Cumm', 'chillingDD_7.0_1_Tbase_sum_Cumm',
#        'chillingDD_7.0_1_Tbasemin_sum_Cumm', 'chillingDD_7.0_1_Utah_sum_Cumm',
#        'chillingDD_7.0_2_Tbase_sum_Cumm', 'chillingDD_7.0_2_Tbasemin_sum_Cumm',
#        'chillingDD_7.0_2_Utah_sum_Cumm', 'rad__t0__Cumm', 'rad__1__Cumm',
#        'rad__2__Cumm', 'precip__t0__Cumm', 'precip__1__Cumm',
#        'precip__2__Cumm', 'winkler_4.5_t0_Tbase_Cumm',
#        'winkler_4.5_t0_TbaseMax_Cumm', 'winkler_4.5_1_Tbase_Cumm',
#        'winkler_4.5_1_TbaseMax_Cumm', 'winkler_4.5_2_Tbase_Cumm',
#        'winkler_4.5_2_TbaseMax_Cumm', 'winkler_10.0_t0_Tbase_Cumm',
#        'winkler_10.0_t0_TbaseMax_Cumm', 'winkler_10.0_1_Tbase_Cumm',
#        'winkler_10.0_1_TbaseMax_Cumm', 'winkler_10.0_2_Tbase_Cumm',
#        'winkler_10.0_2_TbaseMax_Cumm']

# Defined at forecastDataUnion n_dias_atras=14
# Defined at forecastDataUnion n_dias_alante=7

# Defined at forecastDataUnion dos={1.:'Borja',4.:'Calatayud',6.:'Somontano',8.:'Cariñena'}

## TO LAUNCH THE PREDICTIONS:
codigos_enfermedades_predichas={'Oidio':5, 'Piral':7,'Lobesia':3} #OJO, estos nombres deben coincidir con los de los 
#archivos (sin la extensión) ubicados en la carpeta definida en la variable 'path_to_models'
all_preds=False #Change here to 'False' to launch just last 7 days predictions
path_to_models='/createModels/models/' ##Beware, it has to finish with /. Poner aquí la ruta de descompresión de la carpeta
#de NEXUS. En NEXUS está en https://argon-docker.******.**/repository/war/PRO19_0383_GRAPEVINE/models/diseases.zip
#######################################################
### ^^^ diseaseComputation configuration variables ^^^
#######################################################


########################################
# vvv unizarForecastingGeneration vvv  #
########################################
unizarForecastingGeneration_dos={1.:'Borja',4.:'Calatayud',6.:'Somontano',8.:'Cariñena'}

## TO LAUNCH THE PREDICTIONS:
#codigos_enfermedades_predichas={'Lobesia':3, 'Oidio':5} #OJO, estos nombres deben coincidir con los de los 
#archivos (sin la extensión) ubicados en la carpeta definida en la variable 'path_to_models'
unizarForecastingGeneration_all_preds=True #Change here to 'False' to launch just last 7 days predictions
#######################################
# ^^^ nizarForecastingGeneration ^^^  #
#######################################


# Swagger config
################
swagger_config = {
    "headers": [
    ],
    "specs": [
        {
            "endpoint": 'swagger info',
            "route": '/swagger/Info.json',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "static_url_path": "/flasgger_static",
    # "static_folder": "static",  # must be set by user
    "swagger_ui": True,
    "specs_route": "/swagger/"
}
swagger_template = {
  "swagger": "2.0",
  "info": {
    "title": "Grapevine API",
    "description": "API for model management",
    "contact": {
      "responsibleOrganization": "ITAInnova",
      "responsibleDeveloper": "grapevineTeam",
      "email": "info@******.**",
      "url": "www.******.**",
    },
    # "termsOfService": "na",
    "version": "0.0.1"
  },
  "host": "artemisa.******.**",  # overrides localhost:500
  # "basePath": "/api",  # base bash for blueprint registration
  "schemes": [
    "https"
  ],
}