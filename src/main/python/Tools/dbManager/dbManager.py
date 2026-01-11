import pandas as pd
from datetime import date, timedelta, datetime
import os
import sys

from sqlalchemy import create_engine

if 'config' in sys.modules:
  config= sys.modules['config']
else:
  import config as config

logger= config.logger
logger.debug('{} using config from {}'.format(os.path.basename(__file__), config.__file__))

import psycopg2

def generateCSVFromDB( query, outputFolder, outputFileName):
    conexion = psycopg2.connect(database  =config.meteorological_past_database, 
                            user          =config.meteorological_past_user, 
                            password      =config.meteorological_past_password, 
                            host          =config.meteorological_past_host, 
                            port          =config.meteorological_past_port)

    # date_1y_ago= str(date.today()-timedelta(365))
    # query="select * from " + '"ITAINNOVA"' + ".diseases_predictions where time = '" +  date_1y_ago + "'"    preds = pd.read_sql_query(query, con=conexion).drop_duplicates()
    # query="select * from ITAINNOVA.diseases_predictions where time = {}".format(date_1y_ago)
    logger.debug("Executing query {}...".format(query))
    preds = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
    conexion.commit()
    conexion.close
    logger.debug("\tReturned {} records".format(preds.shape[0]))


    now = datetime.now()
    prefix='{}{:02d}{:02d}_{:02d}{:02d}{:02d}'.format(now.year, now.month, now.day, now.hour, now.minute, now.second)
    tmpFile='{}/{}-{}'.format( config.tmpPath, prefix, outputFileName)
    # preds = preds.rename(columns=config.changeColumnName)
    # preds['created'] = prefix
    preds.reset_index(drop=True).to_csv(tmpFile,sep=';')

    # outputFile=config.configoutputPath + prefix + config.outputCesgaFileNameDisease
    fileName='{}-{}'.format( prefix, outputFileName)
    outputFile='{}/{}'.format( outputFolder, fileName)
    logger.debug("\tLeaving records at file {}...".format(outputFile))

    cmd='mv {} {}'.format(tmpFile, outputFile)
    rc= os.system(cmd)

    msg={ "outputCSVFile": outputFile, "csvFileName": fileName, "count": preds.shape[0]}
    return msg



# vvv DEBUG VVV #
# date_1y_ago= str(date.today()-timedelta(365))
# query="select * from public.phenological_predictions where time = '" + date_1y_ago + "'"

# file2CopyPath= generateCSVFromDB(query, config.outputCesgaFileNamePhenology)
# print(file2CopyPath)
# ^^^ DEBUG ^^^ #
