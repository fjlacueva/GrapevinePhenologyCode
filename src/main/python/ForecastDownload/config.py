import os
import logging
from sys import stdout
import sys

paths2Libraries=['/projects/grapevine/GIT/src/src/main/python', '/projects/grapevine/GIT/src/src/main/python/Tools']
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
  loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'DownloadFiles')
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  fh = logging.FileHandler(loggingPath, mode="w", encoding="utf-8")
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler(stdout)
#   formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))

# loggerConfiguration={
#     "level":logging.ERROR,
#     "fileName": 'logs/DownloadFiles.log',
#     "filemode": 'a', 
#     "format":'%(asctime)s:  %(levelname)s - %(message)s'
# }
expectedParameters = ["startdate", "endate", "resolution"]
fileDownload = {
    "verify_cerificates" : False,
    "dataBaseQueries": {
        "user": "*********",
        "password": "**************",
        "climaticStationsDataRequest":"https://*********.*******.***/api/gen_climaticstations?offset=0&pocid=not.is.null",
        "forecastResolutionIDRequest":"https://*******.**********.****/api/weatherforecast_resolution?select=id&name=eq.<resolution>&offset=0"
        
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
    "server":"http://*****.*****.***.*****",
    "port":"*****",
    "baseDownloadFolder":"/projects/grapevine/GIT/src/data/WeatherForecast/",
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
        "user": "*******",
        "password": "**********",
        'forecastTable': 'https://*******.******.****/api/weatherforecast',
        'forecastTableSearchTemplate':'?and=(idestacion.eq.<idStation>,weatherforecastproviderid.eq.<idProvider>,' + 
            'forecastdate.eq.<forecastDate>,resolutionid.eq.<resolutionid>)',
        'forecastDataTable': 'https://*********.*******.***/api/weatherforecastdata',
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
        'From': '*****@****.***'
    }
}

number_days_agroapps = 3

artemisaNGinx_url = 'https://*****.*****.***/api/'
artemisaNGinx_user = "*******"
artemisaNGinx_password = "********"
weatherforecasttable = '*********'
