import logging
import os
import sys

artemisaNGinx_url = 'https://******.*******.****/api/'
artemisaNGinx_user = "*********"
artemisaNGinx_password = "*********"
aragonstationstable = 'estaragon'
weatherforecasttable = 'weatherforecast'
weatherforecastdatatable = 'weatherforecastdata'
user_data = 1
data_resolution = 4
data_provider = 4
columnNameLon = 'long'
columnNameLat = 'lat'
hours = 'hourly'
hour = 'hora'
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
operatingSystem = sys.platform
if operatingSystem=='win32':
  loggingPath = r'c:\temp\Grapevine'
  paths2Libraries=[r'C:\proyectos\PRO19_0383_Grapevine_ITAINNOVA\03_Desarrollo\Modelos\src\src\main\python', 
                   r'C:\proyectos\PRO19_0383_Grapevine_ITAINNOVA\03_Desarrollo\Modelos\src\src\main\Tools']
else:
  loggingPath = '/projects/grapevine/GIT/src/logs/ForecastAPI/log.log'
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

if operatingSystem=='win32':
  realClimateDataCalculus_loggingPath='{}/{}.log'.format(loggingPath+ r'\logs', 'climaticDataSiarWinkle')
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

#logger=None #Other injeced configs can define one
#DATABASE:
postgress_Database='******'
postgress_Username='**********'
postgress_Password='********'
postgress_Host='********.*******.**'
postgress_Port=*****
code=0
long=18
lat=19
if operatingSystem=='win32': #No está creada la ruta en el git
  file_system_prediction = r'C:\proyectos\PRO19_0383_Grapevine_ITAINNOVA\03_Desarrollo\Modelos\src\data\ForecastAPI\dates_predictions.txt'
  file_system_prediction_2 = r'C:\proyectos\PRO19_0383_Grapevine_ITAINNOVA\03_Desarrollo\Modelos\src\data\ForecastAPI\dates_predictions2.txt'
else:
  file_system_prediction = '/projects/grapevine/GIT/src/data/ForecastAPI/dates_predictions.txt'
  file_system_prediction_2 = '/projects/grapevine/GIT/src/data/ForecastAPI/dates_predictions2.txt'

# It is used by Data For Completing Database
import datetime
today = datetime.date.today()
year = today.year


indicativo = 'indicativo'
store_table = 'estimate_arahorario'
index_hour = 18
index_day = 4
# Para forzar poner el año que se desea
# initialYear = 2022
initialYear = year