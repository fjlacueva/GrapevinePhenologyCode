import logging
import os

#DATABASE:
meteorological_past_database='meteo'
meteorological_past_user='*******' 
meteorological_past_password='*******'
meteorological_past_host='artemisa.********.****'
meteorological_past_port=*****

tmpPath='/tmp/'
outputCesgaFileName='output_cesga.csv'
outputCesgaFileNameDisease='output_disease_cesga.csv'
outputCesgaFileNamePhenology='output_phenology_cesga.csv'
configoutputPath='/data/dataPresentation/'

# changeColumnName={'variedad' : 'variety'}

#logging
########
logger = logging.getLogger(__name__)
if not logger.handlers:
#   loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'dbManager')
#   loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
#   logger.setLevel(loggingLevel)
#   formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
#   fh = logging.FileHandler(loggingPath, mode="w", encoding="utf-8")
#   fh.setFormatter(formatter)
#   # fh.setLevel(logging.DEBUG)
#   logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, loggingPath))
