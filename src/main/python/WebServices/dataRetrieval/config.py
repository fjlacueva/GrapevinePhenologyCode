import logging
from logging.handlers import SysLogHandler
from logging.handlers import RotatingFileHandler
import os
import sys
PROJECTNAME='GRAPEVINE.dataRetrieval'

workingFolder=os.getenv('WORKINGFOLDER', '/tmp')
dataFolder=os.getenv('DATAPATH', '/data')

paths2Libraries=['/projects/grapevine/GIT/src/src/main/python/Tools', # For host environment
                 '/pythonLibs']                                       # For docker environment
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
  remoteSysLogInfo=os.getenv('REMOTESYSLOGINFO', None) # Format [host, port]. eg: 'artemisa.*******.***',514
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
future_timeAliveSgs=-1

DataAemet_endPoint='/dataAemet'
DataAemet_prefix='dataRetrieval.dataAemet-'
cmds2CreatePythonTmpEnv_aemet=['~cp /pythonSarga/*.py $WORKINGFOLDER',
                           '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
DataAemet_cmds=[ *cmds2CreatePythonTmpEnv_aemet,
  'LOGLEVEL=INFO; cd $WORKINGFOLDER; $PYTHONENV/bin/python aemet0.py',
  'LOGLEVEL=INFO; cd $WORKINGFOLDER; $PYTHONENV/bin/python aemet2pg.py']



# Output example
# {
#   cmd: "/argon/src/main/script/siar-download-insert.sh",
#   output: "INFO:root: connect using  20220607 to 20220607 
#   INFO:root:insertado : 2304 registros 
#   INFO:root:fin
#   ",
#   rc: 0
# }

########################################################
### vvv dataSiar REST API execution variables vvv
########################################################
DataSiar_endPoint='/dataSiar'
DataSiar_prefix='dataRetrieval.dataSiar-'
cmds2CreatePythonTmpEnv_dataSiar=['~mkdir -p $WORKINGFOLDER/dataSiar',
                                  '~cp /pythonSarga/*.py $WORKINGFOLDER/dataSiar',
                                  '~cp /python/config.docker.py $WORKINGFOLDER/dataSiar/config.py']
DataSiar_cmds=[ *cmds2CreatePythonTmpEnv_dataSiar,
  'LOGLEVEL=INFO; cd $WORKINGFOLDER/dataSiar; $PYTHONENV/bin/python mainDataSiar.py']
########################################################
### vvv dataSiar REST API execution variables vvv
########################################################



########################################################
### vvv dataFaraNetwork REST API execution variables vvv
########################################################
DataFaraNetwork_endPoint='/dataFaraNet'
DataFaraNetwork_prefix='dataRetrieval.dataFaraNet-'
cmds2CreatePythonTmpEnv_DataFaraNetwork=['~cp /pythonModelos_climaticos/* $WORKINGFOLDER -R',
                                          '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
DataFaraNetwork_cmds=[ *cmds2CreatePythonTmpEnv_DataFaraNetwork, 
'LOGLEVEL=INFO; cd $WORKINGFOLDER; $PYTHONENV/bin/python load_data_ftp_server_improve.py']
########################################################
### ^^^ dataFaraNetwork REST API execution variables ^^^
########################################################


forecastAgroapps_endPoint='/forecastAgroapps'
forecastAgroapps_prefix='dataRetrieval.forecastAgroapps-'
forecastAgroapps_cmds=[ '']
forecastAgroapps_endPoint='/forecastAgroapps'
cmds2CreatePythonTmpEnv_forecastAgroapps=['~mkdir -p $WORKINGFOLDER/forecastAgroApps',
                                          '~cp /ForecastDownload/* $WORKINGFOLDER/forecastAgroApps -R',
                                          '~cp /python/config.docker.py $WORKINGFOLDER/forecastAgroApps/config.py']
forecastAgroapps_cmds=[ *cmds2CreatePythonTmpEnv_forecastAgroapps, 'LOGLEVEL=INFO; cd $WORKINGFOLDER/forecastAgroApps; $PYTHONENV/bin/python functionGetLastDateResolution.py']




########################################################
### vvv predictionsAPIMeteo REST API execution variables vvv
########################################################
predictionsAPIMeteo_endPoint='/predictionsAPIMeteo'
predictionsAPIMeteo_prefix='dataRetrieval.predictionsAPIMeteo-'
cmds2CreatePythonTmpEnv_predictionsAPIMeteo=['~cp /pythonForecastAPI/* $WORKINGFOLDER -R',
                                          '~cp /python/config.docker.py $WORKINGFOLDER/config.py']
predictionsAPIMeteo_cmds=[ *cmds2CreatePythonTmpEnv_predictionsAPIMeteo, 'LOGLEVEL=INFO ; cd $WORKINGFOLDER; $PYTHONENV/bin/python TestGetDataFromUrlToDatabase.py']
########################################################
### ^^^ predictionsAPIMeteo REST API execution variables ^^^
########################################################



# predictionsThredss_endPoint='/predictionsThredss'
# predictionsThredss_prefix='predictionsThredss-'
# predictionsThredss_cmds=[ 'python /argon/src/main/python/modelos_climaticos/load_data_ftp_server_improve.py']

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
      "email": "info@*******.***",
      "url": "www.*******.***",
    },
    # "termsOfService": "na",
    "version": "0.0.1"
  },
  "host": "artemisa.*******.***",  # overrides localhost:500
  # "basePath": "/api",  # base bash for blueprint registration
  "schemes": [
    "https"
  ],
}