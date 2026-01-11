import sys
import os
import time
import logging
import json

from flask.json import jsonify
import config as config
logger= config.logger
import sys
import uuid
from datetime import date, timedelta, datetime

from gevent.pywsgi import WSGIServer
import vpn.vpnConnect as vpnManager
from flask_futureExecutor.futureException import FutureException
from flask_futureExecutor import Executor
from fastapi.encoders import jsonable_encoder
from dbManager import dbManager
from webServicesCommon import appCommon
from flask import request
from flask import Flask
from flask import abort
from datetime import date, datetime
from flasgger import Swagger

app=appCommon.app
executor=appCommon.executor
logger=appCommon.logger
swagger = Swagger(app, template=config.swagger_template, config=config.swagger_config, merge=True)


@app.route(config.vineyardsDiseasePredictionNoFutures_endPoint,methods = ['GET'])
def executeDiseasePredictionGeneratorWaittingTilFinished():
    """ '/vineyardsDiseasePredictionWaitting'?weeksAgo=0
    Executes the generation of disease prediction generator uploading the final output to CESGA servers
    This invocation waits till process is finished (see /vineyardsDiseasePrediction to use the future infrastructure)
    weeksAgo (Optional parameter; def=0). Retrieves the number of weeks ago the prediction is retrieved- > date.today() - weeksAgo weeks
    ---
    tags:
      - dataPresentation
    responses:
      200:
        description: Summary of the execution of the job.
    """
    weeksAgo= int(request.args.get('weeksAgo')) if 'weeksAgo' in request.args else 0;
    return vineyardsDiseasePrediction([weeksAgo,weeksAgo], None)
    


@app.route(config.vineyardsDiseasePrediction_endPoint,methods = ['GET'])
def executeDiseasePredictionGenerator():
    """ '/vineyardsDiseasePrediction'?weeksAgo=0
    Launches the generation of disease prediction generator uploading the final output to CESGA servers
    weeksAgo (Optional parameter; def=0). Retrieves the number of weeks ago the prediction is retrieved- > date.today() - weeksAgo weeks
    ---
    tags:
      - dataPresentation
    responses:
      200:
        description: Status of launched job.
    """
    weeksAgo= int(request.args.get('weeksAgo')) if 'weeksAgo' in request.args else 0;
    return executor.launchFuture(logger, config.vineyardsDiseasePrediction_prefix, vineyardsDiseasePrediction, weeksAgo)
    
    # try:
    #     jobPrefix=config.vineyardsDiseasePrediction_prefix
    #     jobs=executor.jobsStatus()
    #     logger.debug(type(jobs))
    #     for existingJob in list(executor.jobsStatus()): 
    #         if existingJob["id"].startswith(jobPrefix) \
    #             and existingJob["done"] == False:
    #             return getJobStatus(existingJob["id"])
    # except:
    #     logger.error("Unexpected error:", str(sys.exc_info()).replace('"', '~')) # To avoid errors formatting jsons

    # try:
    #     id=jobPrefix + str(uuid.uuid1())
    #     executor.submit_stored(id, vineyardsDiseasePrediction)
    #     # executor.submit_stored(id, time.sleep, 5)
    #     return jsonify( executor.jobStatus(id, removeIfDone=False))
    # except:
    #     logger.error("Unexpected error:", str(sys.exc_info()).replace('"', '~'))
    #     return jsonify("error: " + str(sys.exc_info()).replace('"', '~')) # To avoid errors formatting jsons

def vineyardsDiseasePrediction(*args, **kwargs):
    """
    Arguments:
        *args, **kwargs: Are mandatory arguments as this method is designed for being invoked as a future method (executor.launchFuture)
            Depending on the method, these args will held none, or several arguments that have to be passed as an extra parameter of the executor.launchFuture:
                > executor.launchFuture(logger, config.DataSiar_prefix, commandLauncherManager, ['/argon/src/main/script/aemet-download.sh', 
                                                                                            '/argon/src/main/script/aemet-insert.sh'])
                or using name arguments:
                > executor.launchFuture(logger, config.DataSiar_prefix, commandLauncherManager, cmds= ['/argon/src/main/script/aemet-download.sh', 
                                                                                                    '/argon/src/main/script/aemet-insert.sh'])
    Returns:
        Futureable methods should return jsonable objects as the returned object is managed by the Future library to extract valuable info.
        In the event of an exception, the futureable methods should raise a FutureException with the object that describes the exception.
        Both the content returned back on successful finalization or on a FutureException should be a jsonable object: a list, a dictionary, ...
        as it will later be shown as an atribute of the future "result" field. eg:
        This method is returning a list of commands executed with their execution code and its output:
    """
    # Following two lines fix the passing of python parameters
    kwargs= args[1]
    args= args[0]
    weeksAgo= 0 if len(args) == 0 else args[0]
    try:
        dateFrom = str(date.today()- timedelta(weeks=weeksAgo))
        now = datetime.now()
        created='{}{:02d}{:02d}_{:02d}{:02d}{:02d}'.format(now.year, now.month, now.day, now.hour, now.minute, now.second)
        fields=('longitude AS longitude, latitude AS latitude, time AS time, pac_code AS pac_code, variedad AS variety, value AS value' + 
                ', magnitude AS magnitude, model AS model, \'{}\' AS created'.format(created))
        query = "SELECT {} FROM \"ITAINNOVA\".predictions WHERE time >= '{}' ORDER BY time".format(fields, dateFrom)

        logInfo="Retrieving vineyardsDiseasePrediction from date {} ({} weeks ago) onwards...".format(dateFrom, weeksAgo)
        logger.info(logInfo)
        rGenerateCSVFromDB= dbManager.generateCSVFromDB( query, config.dataPath,  config.outputCesga_DiseaseFileName)        
        file2CopyPath= rGenerateCSVFromDB["outputCSVFile"]
        nRecords= rGenerateCSVFromDB["count"]
        logInfo='{}\nNumber of records: {}'.format(logInfo, nRecords)
        if config.copyFile == 'True' and nRecords>0:
            rWaitTillFileMoved2CESGA= waitTillFileMoved2CESGA(file2CopyPath)
            rWaitTillFileMoved2CESGA["details"]= '{}\n{}'.format( logInfo, rWaitTillFileMoved2CESGA["details"])
            response= rWaitTillFileMoved2CESGA
        else:
            # Moves the file to the local storage path
            outputFolderEmpty= config.dataPathEmpty
            outputFileEmpty='{}/{}'.format( outputFolderEmpty, rGenerateCSVFromDB["csvFileName"])
            os.rename( file2CopyPath, outputFileEmpty)
            response=rGenerateCSVFromDB
            if nRecords==0:
                logInfo='{}\nRestiction NotUploadEmptyFiles applied. File kept at {} for audit purposes'.format( logInfo, outputFolderEmpty)
            else:
                logInfo='{}\nRestiction NotUploadAnyFile applied. File kept at {} for audit purposes'.format( logInfo, outputFolderEmpty)
            response['details'] = logInfo
        
        logger.debug(str(response))
        return response
    except FutureException as e:
        raise e
    except Exception as e:
        err="error: " + str(sys.exc_info()).replace('"', '~')
        logger.error('{} exception at vineyardsDiseasePrediction: {}'.format(str(type(e)), err))
        msg={ "error": jsonable_encoder(err)}
        raise FutureException(msg)

# def vineyardsPhenologyPrediction(*args, **kwargs):
#     """
#     Arguments:
#         *args, **kwargs: Are mandatory arguments as this method is designed for being invoked as a future method (executor.launchFuture)
#             Depending on the method, these args will held none, or several arguments that have to be passed as an extra parameter of the executor.launchFuture:
#                 > executor.launchFuture(logger, config.DataSiar_prefix, commandLauncherManager, ['/argon/src/main/script/aemet-download.sh', 
#                                                                                             '/argon/src/main/script/aemet-insert.sh'])
#                 or using name arguments:
#                 > executor.launchFuture(logger, config.DataSiar_prefix, commandLauncherManager, cmds= ['/argon/src/main/script/aemet-download.sh', 
#                                                                                                     '/argon/src/main/script/aemet-insert.sh'])
#     Returns:
#         Futureable methods should return jsonable objects as the returned object is managed by the Future library to extract valuable info.
#         In the event of an exception, the futureable methods should raise a FutureException with the object that describes the exception.
#         Both the content returned back on successful finalization or on a FutureException should be a jsonable object: a list, a dictionary, ...
#         as it will later be shown as an atribute of the future "result" field. eg:
#         This method is returning a list of commands executed with their execution code and its output:
#     """
#     # Following two lines fix the passing of python parameters
#     kwargs= args[1]
#     args= args[0]
#     try:
#         date_1y_ago= str(date.today()-timedelta(365))
#         query="select * from public.phenological_predictions where time = '{}'".format(date_1y_ago)

#         file2CopyPath= dbManager.generateCSVFromDB(query, config.outputCesga_PhenologyFileName)
#         return waitTillFileMoved2CESGE(file2CopyPath)
#     except FutureException as e:
#         raise e
#     except Exception as e:
#         err="error: " + str(sys.exc_info()).replace('"', '~')
#         logger.error('{} exception at vineyardsDiseasePrediction: {}'.format(str(type(e)), err))
#         msg={ "error": jsonable_encoder(err)}
#         raise FutureException(msg)


def waitTillFileMoved2CESGA( file2CopyPath):
    try:        
        msg={ "file": file2CopyPath, "details": "File {} Processed".format(file2CopyPath)}
        file2CopyPath=os.path.expandvars(file2CopyPath)
        if ( not os.path.exists(file2CopyPath)):
            return msg
        else:
            while( os.path.exists(file2CopyPath)):
                time.sleep(1)
        # Sleep till file2CopyPath has been deleted by the crontab process
        return msg
        # cmd='scp {} /logs'.format(file2CopyPath)
        # os.system(cmd)
        # return cmd
        # Copy 2 CESGA
        # encryptedFileWithCommand=config.vpn_scriptFile
        # privateKeyPath=config.vpn_privateSVNPEMPathbasePath
        # x=vpnManager.executeEncryptedScript(encryptedFileWithCommand, privateKeyPath, file2CopyPath)
        # return x
    except Exception as e:
        err="error: " + str(sys.exc_info()).replace('"', '~')
        logger.error('{} exception at waitTillFileMoved2CESGE: {}'.format(str(type(e)), err))
        msg["details"]=jsonable_encoder(err)
        raise FutureException(msg)

if __name__ == "__main__":
    appCommon.launchServer()
