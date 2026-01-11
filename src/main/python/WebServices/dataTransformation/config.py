import logging
from logging.handlers import SysLogHandler
from logging.handlers import RotatingFileHandler
import os
import sys
PROJECTNAME='GRAPEVINE.dataTransformation'

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
  loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), PROJECTNAME)
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  # fh = logging.FileHandler(loggingPath, mode="w", encoding="utf-8")
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
  remoteSysLogInfo=os.getenv('REMOTESYSLOGINFO', None) # Format [host, port]. eg: 'artemisa.******.**',514
  if remoteSysLogInfo != None:
    remoteSysLogInfo= remoteSysLogInfo.split(',')
    rSyslogHost=remoteSysLogInfo[0].strip()
    rSyslogPort=int(remoteSysLogInfo[1].strip())
    logger.debug('SysLog sent to host {},{}'.format(rSyslogHost, str(rSyslogPort)))
    sysLogHandler= logging.handlers.SysLogHandler(address = (rSyslogHost, rSyslogPort))
    # sysLogHandler = SysLogHandler(facility=SysLogHandler.LOG_DAEMON, address='/dev/log')
    formatter= logging.Formatter("%(asctime)s:%(levelname)7s:{}.%(filename)s[%(lineno)04d]: %(message)s".format(PROJECTNAME),"%H%M%S")
    sysLogHandler.setFormatter(formatter)
    logger.addHandler( sysLogHandler)
  logger.info('Server {} starting right now'.format(PROJECTNAME))
  if remoteSysLogInfo != None:
    sysLogHandler.setLevel( logging.ERROR)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))


#########
EXECUTOR_PROPAGATE_EXCEPTIONS=False
# WebServer config
##################
serverPort=5000
future_timeAliveSgs=24*60*60

#Common variables
postgress_Host        = "artemisa.******.**"
postgress_Port        = ******
postgress_Database    ='*******'
postgress_Password    = "*******"
postgress_Username    = "******"
FTP_SERVER = "artemisa.******.**"
FTP_PORT = "21"
FTP_USER = "user"
FTP_PASSWORD = "userPassword"


cmds2CreatePythonTmpEnv1=['~cp /pythonSarga/*.py $WORKINGFOLDER',
                           '~cp /pythonModelos_climaticos/*.py $WORKINGFOLDER',
                           '~cp /python/config.py $WORKINGFOLDER']
cmds2CreatePythonTmpEnv2=['~mkdir $WORKINGFOLDER/common -p',
                           '~cp /pythonForecastAPI/* $WORKINGFOLDER -R',
                          #  '~cp /python/config.py $WORKINGFOLDER/common',
                           ]



####################################
# vvv realClimateDataCalculus vvv  #
####################################
realClimateDataCalculus_endPoint='/realClimateDataCalculus'
realClimateDataCalculus_prefix='dataTransformation.realClimateDataCalculus'
realClimateDataCalculus_cmds2Create=['~cp /climaticDataSiarWinkle/* $WORKINGFOLDER -R',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
realClimateDataCalculus_cmds=[ *realClimateDataCalculus_cmds2Create,
                           'LOGLEVEL=DEBUG ;cd $WORKINGFOLDER; $PYTHONENV/bin/python TransferSiarDataNew.py']
####################################
# ^^^ realClimateDataCalculus ^^^  #
####################################

CVCSIndex_endPoint='/CVCSIndex'
CVCSIndex_prefix='CVCSIndex'

forecastDataUnion_endPoint='/forecastDataUnion'
forecastDataUnion_prefix='dataTransformation.forecastDataUnion'


copernicus_endPoint='/copernicus'
copernicus_prefix='dataTransformation.copernicus-'
cmds2Create_copernicus=['~cp /copernicus/* $WORKINGFOLDER -R',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
copernicus_cmds=[ *cmds2Create_copernicus,
                           'LOGLEVEL=INFO ;cd $WORKINGFOLDER; $PYTHONENV/bin/python mainDates.py']

#####################################################
# vvv forecastDataUnion (aka createFinalSheet) vvv  #
#####################################################
forecastDataUnion_endPoint='/forecastDataUnion'
forecastDataUnion_prefix='dataTransformation.forecastDataUnion-'
forecastDataUnion_cmds2Create=['~cp /createFinalSheet/* $WORKINGFOLDER -R',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
forecastDataUnion_cmds=[ *forecastDataUnion_cmds2Create,
                           'LOGLEVEL=INFO ;cd $WORKINGFOLDER; $PYTHONENV/bin/python Tejedor_de_Parras_Structured.py']
#####################################################
# ^^^ forecastDataUnion (aka createFinalSheet) ^^^  #
#####################################################

########################################################
### vvv DataValidation REST API execution variables vvv
########################################################
dataValidation_endPoint='/dataValidation'
dataValidation_prefix='dataTransformation.dataValidation-'
cmds2CreatePythonTmpEnv_dataValidation=['~cp /pythonForecastAPI/* $WORKINGFOLDER -R',
                                          '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
dataValidation_cmds=[ *cmds2CreatePythonTmpEnv_dataValidation, 'LOGLEVEL=INFO ; cd $WORKINGFOLDER; $PYTHONENV/bin/python OpenMeteoRecoverPrevoiusDataForCompletingDatabase.py']
########################################################
### ^^^ DataValidation REST API execution variables ^^^
########################################################


###########################################################
### vvv diseaseComputation REST API execution variables vvv
###########################################################
diseaseComputation_endPoint='/diseaseComputation'
diseaseComputation_prefix='dataTransformation.diseaseComputation-'
cmds2CreatePythonTmpEnv_diseaseComputation=['~cp /createModels/* $WORKINGFOLDER -R',
                                          '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
diseaseComputation_cmds=[ *cmds2CreatePythonTmpEnv_diseaseComputation, 'LOGLEVEL=DEBUG ; cd $WORKINGFOLDER; $PYTHONENV/bin/python Launch_models.py']
###########################################################
### ^^^ diseaseComputation REST API execution variables ^^^
###########################################################


########################################
# vvv unizarForecastingGeneration vvv  #
########################################
unizarForecastingGeneration_endPoint='/unizarForecastingGeneration'
unizarForecastingGeneration_prefix='dataTransformation.unizarForecastingGeneration'
unizarForecastingGeneration_cmds2Create=['~cp /unizarForecastingGeneration/* $WORKINGFOLDER -R',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
unizarForecastingGeneration_cmds=[ *unizarForecastingGeneration_cmds2Create,
                           'LOGLEVEL=DEBUG ;cd $WORKINGFOLDER; $PYTHONENV/bin/python Launch_Unizar.py']
#######################################
# ^^^ nizarForecastingGeneration ^^^  #
#######################################

# predictionsAPIMeteo_endPoint='/predictionsAPIMeteo'
# predictionsAPIMeteo_prefix='predictionsAPIMeteo-'
# predictionsAPIMeteo_cmds=[ *cmds2CreatePythonTmpEnv2,
#                            'LOGLEVEL=DEBUG ;cd $WORKINGFOLDER; $PYTHONENV/bin/python TestGetDataFromUrlToDatabase.py']

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