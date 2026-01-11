from asyncio.log import logger
import tempfile
import subprocess 

import sys
import os
import logging
from flask.json import jsonify
import config as config
from flask_futureExecutor.futureException import FutureException
from flask_futureExecutor import Executor
from fastapi.encoders import jsonable_encoder
import uuid
from datetime import date, timedelta, datetime
import webServicesCommon.appCommon as appCommon

from flask import Flask
from flasgger import Swagger

app=appCommon.app
executor=appCommon.executor
logger=appCommon.logger

swagger = Swagger(app, template=config.swagger_template, config=config.swagger_config, merge=True)

@app.route(config.DataAemet_endPoint,methods = ['GET'])
def executeDataAemetLauncher():
    """
    Executes the retrieval of Aemet data 
    ---
    tags:
      - dataRetrieval
    """
    return executor.launchFuture(logger, config.DataAemet_prefix, commandLauncherManager, config.DataAemet_cmds)


@app.route(config.DataSiar_endPoint,methods = ['GET'])
def executeDataSiarLauncher():
    """
    Executes the retrieval of Siar data 
    ---
    tags:
      - dataRetrieval
    """
    return executor.launchFuture(logger, config.DataSiar_prefix, commandLauncherManager, config.DataSiar_cmds)

@app.route(config.DataFaraNetwork_endPoint,methods = ['GET'])
def executeDataFaraNetworkLauncher():
    """
    Executes the predictions based on of Fara Network data 
    ---
    tags:
      - dataRetrieval
    """
    return executor.launchFuture(logger, config.DataFaraNetwork_prefix, commandLauncherManager, config.DataFaraNetwork_cmds)

@app.route(config.forecastAgroapps_endPoint,methods = ['GET'])
def executePrediccionesAgroappsLauncher():
    """
    Executes the retrieval of Agroapps data to Artemisa
    ---
    tags:
      - dataRetrieval
    """
    return executor.launchFuture(logger, config.forecastAgroapps_prefix, commandLauncherManager, config.forecastAgroapps_cmds)

@app.route(config.predictionsAPIMeteo_endPoint,methods = ['GET'])
def executePrediccionesAPIMeteoLauncher():
    """
    Executes the retrieval of API Meteo data 
    ---
    tags:
      - dataRetrieval
    """
    return executor.launchFuture(logger, config.predictionsAPIMeteo_prefix, commandLauncherManager, config.predictionsAPIMeteo_cmds)


def commandLauncherManager(*args, **kwargs):
    """
    Manages the invocation of a list of CLI commands.
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
                [
                    {
                        cmd: "/argon/src/main/script/aemet-download.sh",
                        output: "b'Start Aemet\nhttps://opendata.aemet.es/opendata/api/observacion/convencional/todas'",
                        rc: 0
                    },
                    {
                        cmd: "/argon/src/main/script/aemet-insert.sh.x",
                        output: "b''",
                        rc: 0
                    }
                ]
        In the event of an error, the method raises a FutureException with the error or the list of commands showing the one generating the error.
                [
                    {
                        cmd: "/argon/src/main/script/aemet-insert.sh.x",
                        output: "b'/bin/sh: 1: /argon/src/main/script/aemet-insert.sh.x: not found\n'",
                        rc: 127
                    }
                ]

        It is very important to finish the method raising a FutureException as it will be shown as an error in the future description when requested:
            [
                {
                    createdTime: "Sat, 04 Jun 2022 00:32:42 GMT",
                    done: true,
                    durationSgs: 4.314109,
                    error: true,
                    finishedTime: "Sat, 04 Jun 2022 00:32:46 GMT",
                    id: "dataSiar-14fbcb7e-e38d-11ec-880a-df17a11b1d59",
                    result: [
                        {
                            cmd: "/argon/src/main/script/aemet-download.sh",
                            output: "b'Start Aemet\nhttps://opendata.aemet.es/opendata/api/observacion/convencional/todas/\nhttps://opendata.aemet.es/opendata/sh/f95be79a\n/home/worker/aemetdata/03062022H22.json hecho\nEnd Aemet\n'",
                            rc: 0
                        },
                        {
                            cmd: "/argon/src/main/script/aemet-insert.sh.x",
                            output: "b'/bin/sh: 1: /argon/src/main/script/aemet-insert.sh.x: not found\n'",
                            rc: 127
                        }
                    ],
                    status: "FINISHED"
                }
            ]

    """
    # PYTHON ERROR as it converts args and kwargs into args. Crazy but true: Look at the captured trace
    # VVV functionWithExceptionManagementWrapper ARGUMENTS: 0
    # kwarts *{'cmds': ['/argon/src/main/script/siar-download-insert.sh']} (<class 'dict'>)
    # 193.144.228.191 - - [2022-06-07 08:02:07] "GET /dataSiar HTTP/1.1" 200 311 0.003608
    # ^^^ functionWithExceptionManagementWrapper ARGUMENTS: 0
    # VVV DATASIARMANAGER ARGUMENTS: 2
    # -() (<class 'tuple'>)
    # -{'cmds': ['/argon/src/main/script/siar-download-insert.sh']} (<class 'dict'>)
    # kwarts *{} (<class 'dict'>)
    # ^^^ DATASIARMANAGER ARGUMENTS: 2
    
    # Following two lines fix the passing of python parameters
    kwargs= args[1]
    args= args[0]

    # print('VVV DATASIARMANAGER ARGUMENTS: ' + str(len(args)))
    # for arg in args:
    #     print('-{} ({})'.format(str(arg), str(type(arg))))
    # print('kwarts *{} ({})'.format(str(kwargs), str(type(kwargs))))
    # print('^^^ DATASIARMANAGER ARGUMENTS: ' + str(len(args)))


    lstCmds=kwargs.get('cmds',None)
    # print(str(lstCmds))
    if lstCmds == None:
        # print ('No cmds argument')
        if len(args) < 1:
            raise FutureException({'error':'Required list of commands or cmds named argument missing!'})
        else:
            lstCmds= args[0]

    fullTrace=[]
    trace=None
    try:
        nCmd=1
        for cmd in lstCmds:
            trace={ "idx": nCmd, "cmd":cmd, "rc":0, "output":""}
            nCmd= nCmd+1
            if not cmd.startswith('~'):
                fullTrace.append(trace)
            else:
                cmd= cmd[1:]

            msg= '- Calling cmd: \'{}\''.format(cmd)
            logger.debug(msg)
            completedProcess= subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding= 'utf8', check=True)
            rc=completedProcess.returncode

            trace["rc"]=rc
            trace["output"]=completedProcess.stdout.strip()
            # logger.debug('MSG type: ' + str(type(msg)))
            # output='{}\n{}\n'.format(output, msg)
            logger.debug('- Subprocess finished with rc={}'.format(str(rc)))
            if rc!=0:
                raise FutureException(fullTrace)
    except  subprocess.CalledProcessError as e:
        logger.error('{} exception at commandLauncherManager: {}'.format(str(type(e)), str(e.stdout)))
        if not trace in fullTrace:
            fullTrace.append(trace)
        trace["rc"]=e.returncode
        trace["output"]=e.stdout.strip()
        raise FutureException(fullTrace)
        
    except FutureException as e:
        logger.error('{} exception at commandLauncherManager: {}'.format(str(type(e)), str(e)))
        raise e
    except Exception as e:
        err="error: " + str(sys.exc_info()).replace('"', '~')
        logger.error('{} exception at commandLauncherManager: {}'.format(str(type(e)), err))
        if trace != None:
            trace['output']= err
        else:
            trace={ "cmd":'', "rc":-1, "output": jsonable_encoder(err)}
            fullTrace.append(trace)
        raise FutureException(fullTrace)

    return fullTrace

if __name__ == "__main__":
    appCommon.launchServer()
