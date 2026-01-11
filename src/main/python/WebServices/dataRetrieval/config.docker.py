import logging
from logging.handlers import SysLogHandler
from logging.handlers import RotatingFileHandler
import os
import sys
PROJECTNAME='GRAPEVINE.dataRetrieval'

workingFolder=os.getenv('WORKINGFOLDER', '/tmp')
dataFolder=os.getenv('DATAPATH', '/data')
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
  formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  # SysLogHandler to store only ERRORS on it
  # Correct configuration found at https://signoz.io/blog/python-syslog/
  # How to install a rsyslog: https://www.manageengine.com/products/eventlog/logging-guide/syslog/configuring-ubuntu-lts-as-rsyslog-server.html
  # How to setup a logger to send to a rsyslog: https://stackoverflow.com/questions/38907637/quick-remote-logging-system
  remoteSysLogInfo=os.getenv('REMOTESYSLOGINFO', None) # Format [host, port]. eg: '*********.******.**',514
  if remoteSysLogInfo != None:
    remoteSysLogInfo= remoteSysLogInfo.split(',')
    rSyslogHost=remoteSysLogInfo[0].strip()
    rSyslogPort=int(remoteSysLogInfo[1].strip())
    sysLogHandler= logging.handlers.SysLogHandler(address = (rSyslogHost, rSyslogPort))
    # sysLogHandler = SysLogHandler(facility=SysLogHandler.LOG_DAEMON, address='/dev/log')
    formatter= logging.Formatter("%(asctime)s:%(levelname)7s:{}.%(filename)s[%(lineno)04d]: %(message)s".format(PROJECTNAME),"%H%M%S")
    sysLogHandler.setFormatter(formatter)
    logger.addHandler( sysLogHandler)
  if remoteSysLogInfo != None:
    sysLogHandler.setLevel( logging.ERROR)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))


#########
EXECUTOR_PROPAGATE_EXCEPTIONS=False
# WebServer config
##################
serverPort=5000
future_timeAliveSgs=36000

#Common variables
postgress_Host        = "******.*******.***"
postgress_Port        = ******
postgress_Database    ='******'
postgress_Password    = "*******"
postgress_Username    = "******"
FTP_SERVER = "********.******.***"
FTP_PORT = "***"
FTP_USER = "******"
FTP_PASSWORD = "***********"


cmds2CreatePythonTmpEnv1=['~cp /pythonSarga/*.py $WORKINGFOLDER',
                           '~cp /pythonModelos_climaticos/*.py $WORKINGFOLDER',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
DataAemet_endPoint='/dataAemet'
DataAemet_prefix='dataAemet-'
DataAemet_cmds=[ *cmds2CreatePythonTmpEnv1,
                '$PYTHONENV/bin/python $WORKINGFOLDER/aemet0.py',
                '$PYTHONENV/bin/python $WORKINGFOLDER/aemet2pg.py']


############################################
### vvv dataSiar configuration variables vvv
############################################
DataAemet_DataFolder  ='/data/'
DataAemet_APIKeyFile  ='/pythonSarga/apikey.ita'
max_days = 7
############################################
### ^^^ dataSiar configuration variables ^^^
############################################



####################################################
### vvv dataFaraNetwork configuration variables vvv
####################################################
pathTreatedFiles = '{}/dataSiarTreated/'.format(dataFolder)
####################################################
### ^^^ dataFaraNetwork configuration variables  ^^^
####################################################


####################################################
### vvv forecastAgroApps configuration variables vvv
####################################################
expectedParameters = ["startdate", "endate", "resolution"]
fileDownload = {
    "verify_cerificates" : False,
    "dataBaseQueries": {
        "user": "******",
        "password": "*******",
        "climaticStationsDataRequest":"https://*********.********.****/api/gen_climaticstations?offset=0&pocid=not.is.null",
        "forecastResolutionIDRequest":"https://*********.******.*****/api/weatherforecast_resolution?select=id&name=eq.<resolution>&offset=0"
        
    },
    # "urlTemplate":"<server>:<port>/thredds/ncss/agroapps_folder/<date>/GREECE/00/UPP_00/<resolution>/<filename>?",
    "urlTemplate":"<server>:<port>/thredds/ncss/agroapps_folder/<date>/SPAIN/00/UPP_00/<resolution>/<filename>?",
    "urlPointRequestTemplate":"&latitude=<lat>&longitude=<long>",
    "urlMomentRequestTemplate":"&time_start=<time_start>&time_end=<time_end>&vertCoord=&accept=<file_format>",
    "availableResolutionsNumberOfFiles": {
        "18km": 192,
        # "18km": 20,
        "6km": 146,
        "2km":72
    },
    "availableResolutionsIds": {
        "18km": "01",
        "6km": "02",
        "2km": "03"
    },
    "server":"http://193.144.42.171",
    "port":"8080",
    "baseDownloadFolder": "{}/WeatherForecast/".format(dataFolder),
    "downloadedFormat": "xml",
    "fileTypes": ["WRFPRS","WindDir", "WindSpeed"],
    "WRFPRS" : {
        "fileNameTemplate" : "WRFPRS_d<resolution_id>.<number>",
        "vars":  ["Dew_point_temperature_height_above_ground","Relative_humidity_height_above_ground", "Temperature_height_above_ground",
            "Total_precipitation_surface_<number>_Hour_Accumulation", "downward_short_wave_rad_flux_surface"],
    },
    "WindDir" : {
        "vars":  ["Wind_direction_from_which_blowing_height_above_ground"],
        "fileNameTemplate" : "WindDir_d<resolution_id>.<number>",
    },
    "WindSpeed" : {
        "vars":  ["Wind_speed_height_above_ground"],
        "fileNameTemplate" : "WindSpeed_d<resolution_id>.<number>.grb",
    },
    'databaseInsertUpdates' :{
        "user": "************",
        "password": "*****************",
        'forecastTable': 'https://*********.******.**/api/weatherforecast',
        'forecastTableSearchTemplate':'?and=(idestacion.eq.<idStation>,weatherforecastproviderid.eq.<idProvider>,' + 
            'forecastdate.eq.<forecastDate>,resolutionid.eq.<resolutionid>)',
        'forecastDataTable': 'https://*********.******.**/api/weatherforecastdata',
        'forecastDataTableDeleteTemplate': '?idweatherforecast=eq.<idweatherforecast>',
    },
    "columnsToRename" : {
            "Dew_point_temperature_height_above_ground": "dewpoint",
            "Relative_humidity_height_above_ground": "relativehumidity",
            "Temperature_height_above_ground": "temperature",
            "Wind_direction_from_which_blowing_height_above_ground": "winddirection",
            "Wind_speed_height_above_ground": "windspeed",
            "forecastDate": "forecasttimestamp",
            # "Total_precipitation_surface_0_Hour_Accumulation": "precipitation",
            "Total_precipitation_surface_<number>_Hour_Accumulation": "precipitation",
            "downward_short_wave_rad_flux_surface": "radiation"
    },
    "requestHeaders" : {
        'User-Agent': 'ITAInnova User Agent',
        'From': '*******@********.****'
    }
}
####################################################
### ^^^ forecastAgroApps configuration variables ^^^
####################################################


weatherforecasttable = 'weatherforecast'


####################################################
### vvv predictionsAPIMeteo configuration variables vvv
####################################################
artemisaNGinx_url = 'https://*********.******.**/api/'
artemisaNGinx_user = "********"
artemisaNGinx_password = "*************"
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
initialYear = 2010

max_days = 7

number_days_agroapps = 3

# loggingPath = '/projects/grapevine/GIT/src/logs/ForecastAPI/log.log'
loggingPath = '{}/ForecastAPI/log.log'.format(os.getenv("LOGSPATH", '/logs'))
file_system_prediction="{}/ForecastAPI/dates_predictions.txt".format(os.getenv("DATAPATH", '/data'))
file_system_prediction2="{}/ForecastAPI/dates_predictions2.txt".format(os.getenv("DATAPATH", '/data'))
#######################################################
### ^^^ predictionsAPIMeteo configuration variables ^^^
#######################################################

#forecastAgroapps_endPoint='/agroapps'
#forecastAgroapps_prefix='agroapps-'
#forecastAgroapps_cmds=[ 'python /argon/src/main/python/modelos_climaticos/load_data_ftp_server_improve.py']

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
  "host": "*********.******.**",  # overrides localhost:500
  # "basePath": "/api",  # base bash for blueprint registration
  "schemes": [
    "https"
  ],
}