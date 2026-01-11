import logging
import os
import sys

paths2Libraries=['/projects/grapevine/GIT/src/src/main/python/Tools']
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
  loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'dataPresentation')
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  fh = logging.FileHandler(loggingPath, mode="w", encoding="utf-8")
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))


# WebServer config
##################
serverPort=4500
future_timeAliveSgs=24*60*60
# loggingPath='/logs/phenology.log'


# urlPhenology='http://phenology:8000/phenology/easy/'

vineyardsDiseasePrediction_endPoint='/vineyardsDiseasePrediction'
vineyardsDiseasePrediction_prefix='vineyardsDiseasePrediction-'
outputCesga_DiseaseFileName='vineyardsDiseasePredict.csv'
dataPath=os.getenv('DATAPATH', '/data/dataPresentation')



# vineyardsPhenologyPrediction_endPoint='/vineyardsPhenologyPrediction'
# vineyardsPhenologyPrediction_prefix='vineyardsPhenologyPrediction-'
# outputCesga_PhenologyFileName='vineyardsPhenologyPredict.csv'

# basePath='/projects/grapevine/GIT/src/python/'
# VPN Config
############
vpn_privateSVNPEMPathbasePath='/certificates/vpn/2Share/privateVpn.pem'
# vpn_scriptFile='/data/vpnConfig2.json'

# Configuration of SARGA's part metheorological data
####################################################
# vvv Used on fake_code_csv_generator
# meteorological_past_database='meteo'
# meteorological_past_user='grapevine'
# meteorological_past_password='.grapevine'
# meteorological_past_host='bux65.linkpc.net'
# meteorological_past_port=65432
# path_fenología_vid_inicio_CSV='/data/fenology_vid-inicio.csv'
# author='UNIZAR'
# outputCesgaFileName='output_cesga.csv'
# configoutputPath='/data/outputs/'
# tmpPath='/tmp/'
# arrayProvinces='{22, 44, 50}'
# ^^^Used on fake_code_csv_generator
#DATABASE:
meteorological_past_database=os.getenv('METEOROLOGICAL_DATABASE', 'meteo')
meteorological_past_user=os.getenv('METEOROLOGICAL_USER', '*****')
meteorological_past_password=os.getenv('METEOROLOGICAL_PASSWORD', '*********$%&?')
meteorological_past_host=os.getenv('METEOROLOGICAL_HOST', 'artemisa.******.****')
meteorological_past_port=os.getenv('METEOROLOGICAL_PORT', *****)
tmpPath=os.getenv('TMP_PATH', '/tmp/')

outputCesgaFileName=os.getenv('OUTPUTCESGAFILENAME', 'output_cesga.csv')
outputCesgaFileNameDisease=os.getenv('OUTPUTCESGAFILENAMEDISEASE', 'output_disease_cesga.csv')
outputCesgaFileNamePhenology=os.getenv('OUTPUTCESGAFILENAMEPHENOLOGY', 'output_phenology_cesga.csv')
configoutputPath=os.getenv('CONFIGOUTPUTPATH', '/data/dataPresentation/')

changeColumnName=os.getenv('CHANGECOLUMNNAME', {'variedad' : 'variety'})
copyFile=os.getenv('COPYFILE', 'False')
# configoutputPath='/data/outputs/'
# configoutputPath='$DATAPATH'

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
      "email": "info@itainnova.es",
      "url": "www.itainnova.es",
    },
    # "termsOfService": "na",
    "version": "0.0.1"
  },
  "host": "grapevine.itainnova.es",  # overrides localhost:500
  # "basePath": "/api",  # base bash for blueprint registration
  "schemes": [
    "https"
  ],
#   "operationId": "getmyData"
}