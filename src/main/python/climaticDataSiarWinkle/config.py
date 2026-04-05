import logging
import os
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



#DATABASE:
postgress_Database='meteo'
postgress_Username='********' 
postgress_Password='*********'
postgress_Host='****.******.**'
postgress_Port=****

realClimateDataCalculus_odExcelFolder = '/projects/grapevine/GIT/src/data/DatosIntermedios/'
realClimateDataCalculus_SIARstationGISFile = '/projects/grapevine/GIT/src/data/20200604_Climatic_And_Parcel_Data.xlsx'

realClimateDataCalculus_loggingPath = '/projects/grapevine/GIT/src/logs/realClimateDataCalculus/realClimateDataCalculus.log'

inicio = True
inicioYear = 2023

allStations = False
stations = ['gv02', 'Z01']
stations = ['gv02']

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
