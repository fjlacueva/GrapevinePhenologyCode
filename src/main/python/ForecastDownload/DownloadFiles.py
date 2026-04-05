
from distutils.command.config import config
from wsgiref import validate
# from ForecastDownload import config as config
from config import fileDownload as fileDownloadConfig
import config
# from config import loggerConfiguration as loggerConfiguration

from http import server
import re
import requests
from requests.auth import HTTPBasicAuth
import validators
import os
import datetime
from datetime import date
import json
import xmltodict
import xml.etree.ElementTree as elementTree
from typing import Tuple
import pandas as pd

logging= config.logger

def isXml(value):
    try:
        elementTree.fromstring(value)
    except elementTree.ParseError:
        return False
    return True

invalidThreddsURLS=[]
threddsURLContents=[]

def validateExistenceOfThreddsData( requestHeaders, server, port, dateString, resolution, requestFileName):
    # CGM: Validation Method 1 (Deprecated): Accessing the URL http://193.144.42.171:8080/thredds/ncss/agroapps_folder/2023-02-01/dataset.html
    # A 404 is expected, but the body for existing days returns a text similar to this one:
    #       /data/AGROAPPS-CLIMATE-MODEL/2021-04-25 (Is a directory)
    #  Validation Method 2: Accessing the URL http://193.144.42.171:8080/thredds/catalog/agroapps_folder/2023-02-01/catalog.html
    # A 200 is expected for existing days and a 404 if the day does not exist
    errorOnValidation= False
    rc=404
    URL2Test= fileDownloadConfig['urlTemplate'].replace('<server>', server).replace('<port>', port).replace('/thredds/ncss/','/thredds/catalog/').replace('<date>', dateString)
    if( resolution is None):
        URL2Test=URL2Test.replace('/SPAIN/00/UPP_00/<resolution>/<filename>?', '/catalog.html')
    else:
        URL2Test=URL2Test.replace('<resolution>', resolution).replace('/<filename>?', '/catalog.html')

    invalidThreddsURL= next((invalidThreddsURL for invalidThreddsURL in invalidThreddsURLS if invalidThreddsURL == URL2Test), None)
    if invalidThreddsURL is not None:
        errorOnValidation=True
    else:
        try:
            threddsURLContent= next((threddsURLContent['content'] for threddsURLContent in threddsURLContents if threddsURLContent['url'] == URL2Test), None)
            if threddsURLContent is not None:
                rBody=threddsURLContent
                rc= 200
            else:
                response= requests.get( URL2Test, headers=requestHeaders)
                rc= response.status_code
                if rc != 200:
                    errorOnValidation=True
                    invalidThreddsURLS.append(URL2Test)
                else:
                    # The catalog exists. For the case in which no resolution nor filename are provided, this will sufice to assume it is valid, 
                    # For the case in which both resolution and filename are provided, the content is analyzed to check that the file is present
                    rBody= response.text
                    threddsURLContent= {'url': URL2Test, 'content': rBody}
                    threddsURLContents.append(threddsURLContent)

            if errorOnValidation==False:
                if (requestFileName is not None) and (not requestFileName in rBody):
                    errorOnValidation=True
        except Exception as e:
            logging.error(f"Exception validating data existence {{'date':'{dateString}','URL':'{URL2Test}','error':'{str(e)}'}}")
            raise e

        if errorOnValidation:
            threddsDate2ValidateTemplate= f'{dateString}{"" if resolution is None else "-"+resolution}{"" if requestFileName is None else "-"+requestFileName}'
            if rc != 200:
                logging.info(f"DownloadData for '{threddsDate2ValidateTemplate}' will fail as RC={rc} for URL '{URL2Test}'")
            else:
                logging.info(f"DownloadData for '{threddsDate2ValidateTemplate}' will fail as URL '{URL2Test}' did not contain any ref to file '{requestFileName}'")
    if errorOnValidation:
        return False
    else:
        return True


def downloadDataFromURL(coordinates:dict=None, currentDate:datetime=None,
        typeOfFile:str=None, fileParameters:dict=None, resolution:str = None)->dict:
    forecastValues=[]

    try: 
        requestHeaders=fileDownloadConfig["requestHeaders"] if "requestHeaders" in fileDownloadConfig.keys() else None

        logging.debug("downloadDataFromURL started")

        if coordinates is None :
            raise Exception("downloadDataFromURL parameter coordinates is not correctly set." ) 

        if typeOfFile is None  :
            raise Exception("downloadDataFromURL parameter currentDate is not correctly set." ) 
        dateString = currentDate.strftime('%Y%m%d')
        dateString = currentDate.strftime('%Y-%m-%d')


        if typeOfFile is None or len(typeOfFile)==0 :
            raise Exception("downloadDataFromURL parameter typeOfFile is not correctly set." ) 

        if fileParameters is None :
            raise Exception("downloadDataFromURL parameter fileParameters is not correctly set." ) 

        if "urlTemplate" not in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['urlTemplate'] does not exist." ) 
        URL = fileDownloadConfig["urlTemplate"]
        if URL is None or len(URL)==0:
            raise Exception("downloadDataFromURL fileDownloadConfig['urlTemplate'] is not correctly set." ) 

        if "downloadedFormat" not in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['downloadedFormat'] does not exist." ) 
        downloadedFormat = fileDownloadConfig["downloadedFormat"]

        if not "baseDownloadFolder"  in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['baseDownloadFolder'] does not exist." ) 
        baseDownloadFolder = fileDownloadConfig["baseDownloadFolder"]
        if baseDownloadFolder is None or len(baseDownloadFolder)==0:
            raise Exception("downloadDataFromURL fileDownloadConfig['baseDownloadFolder'] is not correctly set." )  

        if not "server"  in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['server'] does not exist." ) 
        server = fileDownloadConfig["server"]
        if server is None or len(server)==0 or not validators.url(server):
            raise Exception("downloadDataFromURL fileDownloadConfig['server'] is not correctly set." ) 

        if not "port"  in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['port'] does not exist." ) 
        port = fileDownloadConfig["port"]
        if port is None :
            raise Exception("downloadDataFromURL fileDownloadConfig['port'] is not correctly set." )   

        if "availableResolutionsNumberOfFiles" not in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['availableResolutionsNumberOfFiles'] is not correctly set." ) 
        availableResolutionsNumberOfFiles = fileDownloadConfig['availableResolutionsNumberOfFiles']  

        if "availableResolutionsIds" not in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['availableResolutionsIds'] is not correctly set." ) 
        availableResolutionsIds = fileDownloadConfig['availableResolutionsIds']  

            
        if resolution is None or len(resolution)==0:
             raise Exception("downloadDataFromURL resolution parameter is not correctly set." )  
        if not resolution in availableResolutionsNumberOfFiles.keys():
            raise Exception("downloadDataFromURL the"+ resolution + 
                " is not defined in fileDownloadConfig['availableResolutionsNumberOfFiles']." ) 

        resolution_id = availableResolutionsIds[resolution]
        if resolution_id is None or not isinstance(resolution_id, str):
            raise Exception("downloadDataFromURL resolution_id['resolution'] is not correctly set." ) 

        resolutionNumerOfFiles = availableResolutionsNumberOfFiles[resolution]
        if resolutionNumerOfFiles is None or not isinstance(resolutionNumerOfFiles, int):
            raise Exception("downloadDataFromURL availableResolutionsNumberOfFiles['resolution' is not correctly set]." ) 

        if not "urlPointRequestTemplate" in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['urlPointRequestTemplate'] does not exist." ) 
        urlPointRequestTemplate = fileDownloadConfig["urlPointRequestTemplate"]
        if urlPointRequestTemplate is None or len(urlPointRequestTemplate)==0 :
            raise Exception("downloadDataFromURL fileDownloadConfig['urlPointRequestTemplate'] is not correctly set." )  
        if "long" not in coordinates.keys():
            raise Exception("downloadDataFromURL parameter coordinates does not define long." ) 
        long = coordinates["long"]
        if long is None or not isinstance(long, float):
            raise Exception("downloadDataFromURL parameter coordinates['long'] does not contain a valid value." ) 
        urlPointRequest = urlPointRequestTemplate.replace("<long>", str(long))
        if "lat" not in coordinates.keys():
            raise Exception("downloadDataFromURL parameter coordinates does not define lat." ) 
        lat = coordinates["lat"]
        if lat is None or not isinstance(lat, float):
            raise Exception("downloadDataFromURL parameter coordinates['lat'] does not contain a valid value." ) 
        urlPointRequest = urlPointRequest.replace("<lat>", str(lat))


        if "urlMomentRequestTemplate" not in fileDownloadConfig.keys():
            raise Exception("downloadDataFromURL fileDownloadConfig['urlMomentRequestTemplate'] does not exist." ) 
        urlMomentRequestTemplate = fileDownloadConfig["urlMomentRequestTemplate"]
        urlMomentRequestTemplate = urlMomentRequestTemplate.replace('<file_format>', downloadedFormat)

        if "fileNameTemplate" not in fileParameters.keys():
            raise Exception("downloadDataFromURL fileParameters['fileNameTemplate'] does not exist." ) 
        fileNameTemplate = fileParameters["fileNameTemplate"]
        if fileNameTemplate is None or len(fileNameTemplate)==0:
            raise Exception("downloadDataFromURL fileParameters['fileNameTemplate'] is not correctly set." ) 

        if "vars" not in fileParameters.keys():
            raise Exception("downloadDataFromURL fileParameters['fileNameTemplate'] does not exist." ) 
        fileVars = fileParameters["vars"]
        if fileVars is None or len(fileVars)==0:
            raise Exception("downloadDataFromURL fileParameters['vars'] is not correctly set.  You must include at least one valid variable." ) 


        if not validateExistenceOfThreddsData( requestHeaders, server, port, dateString, None, None):
            return forecastValues




        URL = URL.replace("<server>", server)
        URL = URL.replace("<port>", port)
        URL = URL.replace("<date>", dateString)
        URL = URL.replace("<resolution>", resolution)

        baseDownloadFolder = baseDownloadFolder.replace('<date>', dateString)
        if not os.path.isdir(baseDownloadFolder):
            raise Exception("downloadDataFromURL fileDownloadConfig['baseDownloadFolder'] is not a path to a folder: " + 
                baseDownloadFolder )  
        
        requestedVars = ""
        for var in fileVars:
            if len(requestedVars)>0:
                requestedVars = requestedVars + "&"
            requestedVars = requestedVars + "var="+var

        nErrorsDownloadDataFromUrl=0
        nOkDownloadDataFromUrl=0
        nDownloadDataFromUrl=0
        for fileNumber in range(0, resolutionNumerOfFiles+1):
            forecastValue={}
            responseData = None
            fileNumberString = str(fileNumber)
            if fileNumber<100:
                fileNumberString = '{0:02d}'.format(fileNumber) 
            requestFileName = fileNameTemplate.replace('<number>', fileNumberString)
            requestFileName = requestFileName.replace('<resolution_id>', resolution_id)

            desideredForecastDate = currentDate + datetime.timedelta(hours=fileNumber)
            desideredForecastDateString = desideredForecastDate.isoformat()
            urlMomentRequest = urlMomentRequestTemplate.replace('<time_start>', desideredForecastDateString)
            urlMomentRequest = urlMomentRequest.replace('<time_end>', desideredForecastDateString)

            requestURLRoot = URL.replace("<filename>", requestFileName)
            varNumberString = str(fileNumber)
            varsToRequest = requestedVars.replace('<number>', varNumberString)
            requestURL = requestURLRoot + varsToRequest + urlPointRequest + urlMomentRequest
            if not validators.url(requestURL):
                raise Exception("downloadDataFromURL not possible to create a valid url: " + requestURL ) 

            if not validateExistenceOfThreddsData( requestHeaders, server, port, dateString, resolution, requestFileName):
                continue


            response = requests.get(requestURL, headers=requestHeaders)
            rc= response.status_code
            nDownloadDataFromUrl= nDownloadDataFromUrl + 1
            if rc != 200:
                if(nErrorsDownloadDataFromUrl<5):
                    logging.warning(f'RC:{response.status_code:4d} calling URL {requestURL}')
                    if not response.text is None and len(response.text)>0:
                        logging.warning("\t next message is received: "  + response.text)
                elif nErrorsDownloadDataFromUrl==5:
                    logging.warning("As there seems to be too many warnings of type \"downloadDataFromURL received an status_code bla, bla\", " \
                        " no more will be printed, so please review the number of them at the final summary")
                nErrorsDownloadDataFromUrl=nErrorsDownloadDataFromUrl+1
            else:
                logging.debug(f'RC:{response.status_code:4d} calling URL {requestURL}')
                if not isXml(response.text):
                    logging.warning( f"downloadDataFromURL !XML response received for request: {requestURL}: {response.text}")
                    # raise Exception("downloadDataFromURL recived response is not a valid xml: " +  response.text)  
                    logging.warning("downloadDataFromURL  UNABLE TO RECOVER DATA FOR FILE: " + requestFileName )
                else:
                    nOkDownloadDataFromUrl=nOkDownloadDataFromUrl+1
                    responseData = json.loads(json.dumps(xmltodict.parse(response.text)))
                    # if responseData is None:
                    #     logging.warning("downloadDataFromURL  NOT ABLE TO TO RECOVER DATA FOR FILE: " + requestFileName + " from URL: " +  requestURL)
                        # raise Exception("downloadDataFromURL recived response is not a valid xml: " +  requestURL)
                    forecastValue = {
                        'long': long,
                        'lat': lat,
                        'forecastDate': desideredForecastDate,
                    }
                    if not responseData is None:
                        # responseDataValues =  responseData['grid']['point']['data']
                        # for responseDataValue in responseDataValues:
                        #     variableName = responseDataValue['@name'] 
                        #     if variableName in fileVars:
                        #         valueString = responseDataValue['#text']
                        #         if valueString is None or len(valueString)==0:
                        #             logging.error("downloadDataFromURL response received for request: " +  variableName)
                        #             logging.error("downloadDataFromURL  there is not values for variable : " +  response.text)
                        #             raise Exception("downloadDataFromURL there is not values for variable : " +  response.text)
                        #         try:
                        #             variableValue = float(valueString)
                        #         except Exception as e:
                        #             logging.error("downloadDataFromURL response received for request: " +  variableName)
                        #             logging.error("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                        #             raise Exception("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                        if typeOfFile == "WRFPRS":
                            responsePointsDataValues =  responseData['grid']['point']
                            for point in responsePointsDataValues:
                                responseDataValues=point['data']
                                for responseDataValue in responseDataValues:
                                    variableName = responseDataValue['@name'] 
                                    if variableName.startswith("Total_precipitation_"):
                                        variableName = "Total_precipitation_surface_<number>_Hour_Accumulation"
                                    if variableName in fileVars:
                                        valueString = responseDataValue['#text']
                                        if valueString is None or len(valueString)==0:
                                            logging.error("downloadDataFromURL response received for request: " +  variableName)
                                            logging.error("downloadDataFromURL  there is not values for variable : " +  response.text)
                                            raise Exception("downloadDataFromURL there is not values for variable : " +  response.text)
                                        try:
                                            variableValue = float(valueString)
                                            forecastValue[variableName]=variableValue
                                        except Exception as e:
                                            logging.error("downloadDataFromURL response received for request: " +  variableName)
                                            logging.error("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                                            raise Exception("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                        if typeOfFile in ( "WindDir", "WindSpeed") :
                            responsePointDataValues =  responseData['grid']['point']
                            responseDataValues=responsePointDataValues['data']
                            for responseDataValue in responseDataValues:
                                variableName = responseDataValue['@name'] 
                                if variableName.startswith("Total_precipitation_"):
                                    variableName = "Total_precipitation_surface_<number>_Hour_Accumulation"
                                if variableName in fileVars:
                                    valueString = responseDataValue['#text']
                                    if valueString is None or len(valueString)==0:
                                        logging.error("downloadDataFromURL response received for request: " +  variableName)
                                        logging.error("downloadDataFromURL  there is not values for variable : " +  response.text)
                                        raise Exception("downloadDataFromURL there is not values for variable : " +  response.text)
                                    try:
                                        variableValue = float(valueString)
                                        forecastValue[variableName]=variableValue
                                    except Exception as e:
                                        logging.error("downloadDataFromURL response received for request: " +  variableName)
                                        logging.error("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                                        raise Exception("downloadDataFromURL  contains a not valid float value for variable : " +  response.text)
                        forecastValues.append(forecastValue)

        logging.debug(f"downloadDataFromURL finished having retrieved {len(forecastValues)} forecastValues")
        if(nErrorsDownloadDataFromUrl>=5):
            logging.warning(f"(err:{nErrorsDownloadDataFromUrl:d})/(Ok:{nDownloadDataFromUrl-nErrorsDownloadDataFromUrl:d}) found on downloadDataFromURL")
    except Exception as e:
        logging.error("downloadDataFromURL catches Exception: "+ str(e))
        #raise Exception("downloadDataFromURL catches expection: " + str(e))
    return forecastValues

def downloadDataForDate(coordinates:dict=None,currentDate:datetime=None, resolution:str = None)->object:
    values = {}

    try: 
        logging.debug("downloadDataForDate started")

        if coordinates is None :
            raise Exception("downloadDataForDate parameter coordinates is not correctly set." ) 

        if currentDate is None :
            raise Exception("downloadDataForDate parameter dateString is not correctly set." ) 

        if "fileTypes" not in fileDownloadConfig.keys():
            raise Exception("downloadDataForDate fileDownloadConfig['fileTypes'] does not exist." ) 
        fileTypes = fileDownloadConfig["fileTypes"]
        if fileTypes is None or len(fileTypes)==0 :
            raise Exception("downloadDataForDate fileDownloadConfig['fileTypes'] is not correctly set." ) 

        if resolution is None or len(resolution)==0:
             raise Exception("downloadDataForDate resolution parameter is not correctly set." )  


        for typeOfFile in fileTypes:
            if typeOfFile is None or len(typeOfFile)==0:
                raise Exception("downloadDataForDate fileDownloadConfig['fileTypes'] contain a null or empty value." ) 
            if typeOfFile not in fileDownloadConfig.keys():
                raise Exception("downloadDataForDate: " + typeOfFile + " is not defined in config.fileDownloadConfig." ) 
            fileParameters = fileDownloadConfig[typeOfFile]
            fileValues = downloadDataFromURL(coordinates = coordinates, currentDate=currentDate, 
                typeOfFile=typeOfFile, fileParameters=fileParameters, resolution= resolution)
            if not fileValues is None:
                values[typeOfFile]= fileValues
        logging.debug("downloadDataForDate finished")

    except Exception as e:
        logging.error("downloadDataForDate catches Exception: "+ str(e))
        raise Exception("downloadDataForDate catches expection: " + str(e))
    return values


# def downloadDataForCoordinates(coordinates:dict=None, startDate:datetime=None, endDate:datetime=None,
def downloadDataForCoordinates(coordinates:dict=None, startDate:datetime=None, 
    resolution:str = None)->object:
    values = []
    try: 
        logging.debug("downloadDataForCoordinates started")
        if coordinates is None :
            raise Exception("downloadDataForCoordinates parameter coordinates is not correctly set." ) 
        if startDate is None :
            raise Exception("downloadDataForCoordinates parameter startDate is not correctly set." ) 
        # if endDate is None :
        #     raise Exception("downloadDataForCoordinates parameter endDate is not correctly set." ) 
        if resolution is None or len(resolution)==0:
             raise Exception("downloadDataForCoordinates resolution parameter is not correctly set." )  


        delta = datetime.timedelta(days=1)
        # currentDate = startDate
        # while (currentDate <= endDate):
            # dateValues =downloadDataForDate(coordinates=coordinates, currentDate=currentDate, resolution = resolution)
            # values.append({'forecastDate':  currentDate, 'values' :dateValues})
            # currentDate += delta
        dateValues =downloadDataForDate(coordinates=coordinates, currentDate=startDate, resolution = resolution)
        # values.append({'forecastDate':  currentDate, 'values' :dateValues})
        if len(dateValues.keys())!=0:
            values.append({'forecastDate':  startDate, 'values' :dateValues})
            logging.debug("downloadDataForCoordinates finished")

    except Exception as e:
        logging.error("downloadDataForCoordinates catches Exception: "+ str(e))
        raise Exception("downloadDataForCoordinates catches expection: " + str(e))

    return values

# def downloadDataForStations(startDate:datetime=None, endDate:datetime=None, resolution:str = None)->object:

def downloadDataForStations(startDate:datetime=None, resolution:str = None)->object:
    values = []
    try: 
        logging.debug("downloadDataForStations started")
        if startDate is None :
            raise Exception("downloadDataForStations parameter startDate is not correctly set." ) 
        # if endDate is None :
        #     raise Exception("downloadDataForStations parameter endDate is not correctly set." ) 
        if resolution is None or len(resolution)==0:
             raise Exception("downloadDataForStations resolution parameter is not correctly set." )  

        if "dataBaseQueries" not in fileDownloadConfig.keys():
            raise Exception("downloadDataForStations fileDownloadConfig does not contain key dataBaseQueries." ) 
        dataBaseQueries = fileDownloadConfig["dataBaseQueries"]
        if dataBaseQueries is None or  not type(dataBaseQueries) is dict :
            raise Exception("downloadDataForStations fileDownloadConfig['dataBaseQueries'] does not have a valid value." ) 

        if "climaticStationsDataRequest" not in dataBaseQueries.keys():
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries does not contain key climaticStationsDataRequest." ) 
        climaticstationsDataRequestURL = dataBaseQueries["climaticStationsDataRequest"]
        if climaticstationsDataRequestURL is None or len(climaticstationsDataRequestURL) ==0:
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries['climaticStationsDataRequest'] does not have a valid value." ) 
        if not validators.url(climaticstationsDataRequestURL):
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries['climaticStationsDataRequest'] is not a valid url: " + climaticstationsDataRequestURL ) 

        if "user" not in dataBaseQueries.keys():
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries does not contain key user." ) 
        user = dataBaseQueries["user"]
        if user is None or len(user) ==0:
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries['user'] does not have a valid value." ) 

        if "password" not in dataBaseQueries.keys():
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries does not contain key user." ) 
        password = dataBaseQueries["password"]
        if password is None or len(password) ==0:
            raise Exception("downloadDataForStations fileDownloadConfig.dataBaseQueries['password'] does not have a valid value." ) 

        basic = HTTPBasicAuth(username=user, password=password)
        response = requests.get(climaticstationsDataRequestURL, auth = basic, verify= fileDownloadConfig["verify_cerificates"])   
        if response.status_code!=200:
            logging.error("downloadDataForStations received an status_code: " + str(response.status_code)+
                " to request: " + climaticstationsDataRequestURL)
            if not response.text is None and len(response.text)>0:
                logging.error("\t next message is received: "  + response.text)
            raise Exception("downloadDataForStations received error: " + str(response.status_code)+ " when requesting climatic station data." )
        climaticstationsDataJson = response.json() 
        # climaticstationsData = json.loads(climaticstationsDataJson)
        for climaticstationData in climaticstationsDataJson:
            coordinates = {
                "long": climaticstationData["long"],
                "lat": climaticstationData["lat"]
            }
            # newValues = downloadDataForCoordinates(coordinates=coordinates, startDate=startDate, endDate=endDate, resolution=resolution)
            newValues = downloadDataForCoordinates(coordinates=coordinates, startDate=startDate, resolution=resolution)
            if not newValues is None and len(newValues)!=0:
                values.append({ "stationId": climaticstationData["id"], "coordinates": coordinates, "values": newValues})
        logging.debug("downloadDataForStations finished")
    except Exception as e:
        logging.error("downloadDataForStations catches Exception: "+ str(e))
        raise Exception("downloadDataForStations catches expection: " + str(e))
    return values    

class ObjectEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            return obj.__dict__

def getStationForecastID(predictionDate:datetime=None, stationid:int=None, providerid:int=None, resolutionid:int=None,
        databaseInsertUpdates:dict = None, long:float=None, lat:float=None )-> Tuple[int, object]:
    forecastID = None

    try: 
        logging.debug("getStationForecastID started")
        if predictionDate is None :
            raise Exception("getStationForecastID parameter predictionDate is not correctly set." ) 
        if stationid is None :
            raise Exception("getStationForecastID parameter stationid is not correctly set." ) 
        if resolutionid is None :
            raise Exception("getStationForecastID parameter resolutionid is not correctly set." )    
        if databaseInsertUpdates is None:
            raise Exception("getStationForecastID parameter databaseInsertUpdates is not correctly set." )     
        if long is None:
            raise Exception("getStationForecastID parameter long is not correctly set." )     
        if lat is None:
            raise Exception("getStationForecastID parameter lat is not correctly set." )     
            
        if not 'forecastTable' in  databaseInsertUpdates.keys():   
            raise Exception("getStationForecastID parameter databaseInsertUpdates does not contain value for forecastTable key." )   
        forecastTable = databaseInsertUpdates['forecastTable']  
        if  forecastTable is None and len(forecastTable)==0:
             raise Exception("getStationForecastID databaseInsertUpdates['forecastTable'] is not correctly set." )   

        if not 'forecastTableSearchTemplate' in  databaseInsertUpdates.keys():   
            raise Exception("getStationForecastID parameter databaseInsertUpdates does not contain value for forecastTableSearchTemplate key." )   
        forecastTableSearchTemplate = databaseInsertUpdates['forecastTableSearchTemplate']  
        if  forecastTableSearchTemplate is None and len(forecastTableSearchTemplate)==0:
             raise Exception("getStationForecastID databaseInsertUpdates['forecastTableSearchTemplate'] is not correctly set." )   

        if "user" not in databaseInsertUpdates.keys():
            raise Exception("getStationForecastID fileDownloadConfig.databaseInsertUpdates does not contain key user." ) 
        user = databaseInsertUpdates["user"]
        if user is None or len(user) ==0:
            raise Exception("getStationForecastID fileDownloadConfig.databaseInsertUpdates['user'] does not have a valid value." ) 

        if "password" not in databaseInsertUpdates.keys():
            raise Exception("getStationForecastID fileDownloadConfig.databaseInsertUpdates does not contain key user." ) 
        password = databaseInsertUpdates["password"]
        

        forecastTableSearchTemplate = forecastTableSearchTemplate.replace('<idStation>', str(stationid))    
        forecastTableSearchTemplate = forecastTableSearchTemplate.replace('<idProvider>', str(providerid))   
        forecastDate  = datetime.datetime.strftime(predictionDate, "%Y-%m-%d")
        forecastTableSearchTemplate = forecastTableSearchTemplate.replace('<forecastDate>', forecastDate)    
        forecastTableSearchTemplate = forecastTableSearchTemplate.replace('<resolutionid>', str(resolutionid))

        requestURL = forecastTable +forecastTableSearchTemplate
        if not validators.url(requestURL):
            raise Exception("getStationForecastID not possible to create a valid url: " + requestURL ) 
        # httpBasicAuth = HTTPBasicAuth(username=user, password=password)
        httpSession = requests.Session()
        httpSession.auth = (user, password)
        responseData =None
        insertValues = {
            "idestacion" : stationid,
            "forecastdate" : forecastDate,
            "weatherforecastproviderid" : providerid,
            "resolutionid" : resolutionid,
            "long": long,
            "lat": lat

        }
        # response = requests.get(requestURL, auth = httpBasicAuth)   
        response = httpSession.get(requestURL, verify= fileDownloadConfig["verify_cerificates"])
        if response.status_code != 200:
            logging.error("getStationForecastID received an status_code: " + str(response.status_code)+
                " to request: " + requestURL)
            raise Exception('getStationForecastID an error happened when accesing database: '+ str(response.status_code)+
                ' ' + response.reason + ' ' + response.request)
        else:
            responseData = json.loads(response.content)
            if len(responseData) ==0 :
                # response = requests.post(url=forecastTable, json=insertValues, auth = httpBasicAuth)
                
                response = httpSession.post(url=forecastTable, json=insertValues)
                if response.status_code!=201:
                    logging.error("getStationForecastID received an status_code: " + str(response.status_code)+
                        " when trying to insert into : " + forecastTable)
                    logging.error("\t\t values " + json.dumps(insertValues))
                    raise Exception('getStationForecastID an error happened when accesing database: '+ str(response.status_code)+
                        ' ' + response.reason + ' ' + forecastTable)

                requestURL = forecastTable +forecastTableSearchTemplate
                if not validators.url(requestURL):
                    raise Exception("getStationForecastID not possible to create a valid url: " + requestURL ) 
                response = httpSession.get(requestURL)
                if response.status_code != 200:
                    logging.error("getStationForecastID received an status_code: " + str(response.status_code)+
                        " to request: " + requestURL)
                    raise Exception('getStationForecastID an error happened when accesing database: '+ str(response.status_code)+
                        ' ' + response.reason + ' ' + response.request)
                else:
                    responseData = json.loads(response.content)
                    if len(responseData) > 1:
                        raise Exception("getStationForecastID more than 1 value returned for url: " + str(requestURL) )    
            else:
                if len(responseData) > 1:
                    raise Exception("getStationForecastID more than 1 value returned for url: " + str(requestURL) ) 
            if responseData is None:
                raise Exception("getStationForecastID there is an error somewhere.  It was not possible to find id for station data" + 
                    json.dumps(insertValues) ) 
            stationData = responseData[0]
            forecastID = stationData['id']        
        logging.debug("getStationForecastID finished")

    except Exception as e:
        logging.error("getStationForecastID catches Exception: "+ str(e))
        raise Exception("getStationForecastID catches expection: " + str(e))
 
    return forecastID, httpSession

def saveStationForecastToDataBase(predictionDate:datetime=None, stationForecastId:int=None, 
        stationForecastDataValues:object=None, httpSession:object=None,
        databaseInsertUpdates:dict = None, stationID:int=None )->bool:
    saved = True

    try: 
        if predictionDate is None :
            raise Exception("saveStationForecastToDataBase parameter predictionDate is not correctly set." ) 
        if stationForecastId is None :
            raise Exception("saveStationForecastToDataBase parameter stationForecastId is not correctly set." ) 
        if stationForecastDataValues is None  or len(stationForecastDataValues)==0:
            raise Exception("saveStationForecastToDataBase parameter stationForecastDataValues is not correctly set." ) 
        if stationID is None  :
            raise Exception("saveStationForecastToDataBase parameter stationID is not correctly set." ) 

        if httpSession is None :
            raise Exception("saveStationForecastToDataBase parameter httpSession is not correctly set." ) 
        if databaseInsertUpdates is None:
            raise Exception("saveStationForecastToDataBase parameter databaseInsertUpdates is not correctly set." )     
        
        if not 'forecastDataTable' in  databaseInsertUpdates.keys():   
            raise Exception("saveStationForecastToDataBase parameter databaseInsertUpdates does not contain value for forecastTable key." )   
        forecastDataTable = databaseInsertUpdates['forecastDataTable']  
        if  forecastDataTable is None and len(forecastDataTable)==0:
            raise Exception("saveStationForecastToDataBase databaseInsertUpdates['forecastDataTable'] is not correctly set." )   

        if not 'forecastDataTableDeleteTemplate' in  databaseInsertUpdates.keys():   
            raise Exception("saveStationForecastToDataBase parameter databaseInsertUpdates does not contain value for forecastDataTableDeleteTemplate key." )   
        forecastDataTableDeleteTemplate = databaseInsertUpdates['forecastDataTableDeleteTemplate']  
        if  forecastDataTableDeleteTemplate is None and len(forecastDataTableDeleteTemplate)==0:
            raise Exception("saveStationForecastToDataBase databaseInsertUpdates['forecastDataTableDeleteTemplate'] is not correctly set." )

        forecastDataTableDeleteTemplate = forecastDataTableDeleteTemplate.replace('<idweatherforecast>', str(stationForecastId))  
        requestedURL = forecastDataTable + forecastDataTableDeleteTemplate 
        if not validators.url(requestedURL):
            raise Exception("saveStationForecastToDataBase not possible to create a valid url: " + requestedURL ) 
        response = httpSession.delete(url=requestedURL)
        if response.status_code > 299:
            logging.error("saveStationForecastToDataBase received an status_code: " + str(response.status_code)+
                " to request: " + forecastDataTableDeleteTemplate)
            raise Exception('saveStationForecastToDataBase an error happened when accesing database: '+ str(response.status_code)+
                ' ' + response.reason + ' ' + response.request)
        
        dateStationForecastData = stationForecastDataValues[0]

        if not 'forecastDate' in dateStationForecastData.keys():
            raise Exception("saveStationForecastToDataBase dateStationForecastData does not contain forecastDate key." )
        forecastDate = dateStationForecastData['forecastDate']
        if predictionDate != forecastDate:
            raise Exception("saveStationForecastToDataBase parameter predictionDate: "+ datetime.datetime.strftime(predictionDate, 'd%-m%-Y%') +
                " does not match forecastDate: "+ datetime.datetime.strftime(forecastDate, 'd%-m%-Y%') +  
                " for station with id: " + str(stationID) + "." )

        if not 'values' in dateStationForecastData.keys():
            raise Exception("saveStationForecastToDataBase dateStationForecastData does not contain values key." )
        dateStationForecastDataValues = dateStationForecastData['values']

        completeStationForecastDataDF = None
        for file in dateStationForecastDataValues.keys():
            logging.debug(f'saveStationForecast2DB for dateStationForecastDataValue {file}')
            fileValues = dateStationForecastDataValues[file]
            # filesValuesDF = pd.DataFrame.from_dict(fileValues)
            # filesValuesDF.drop_duplicates(inplace=True)
            # filesValuesDF['ConvertedDate']=filesValuesDF['forecastDate'].dt.strftime('%Y-%m-%d H%:M%')
            # filesValuesDF['ConvertedDate'] = filesValuesDF['ConvertedDate'].astype('|S')
            filesValuesJson = json.dumps(fileValues, indent=4, sort_keys=True, cls=ObjectEncoder)
            filesValuesDF = pd.read_json(filesValuesJson)
            # filesValuesDF['forecastDate'] = pd.to_datetime(filesValuesDF['forecastDate'],  format = '%Y-%m-%d %H:%M')
            logging.debug(f'\tfilesValuesDF.shape: {json.dumps(filesValuesDF.shape)}')
            if completeStationForecastDataDF is None:
                completeStationForecastDataDF = filesValuesDF
            else:
                if(len(filesValuesDF)>0):
                    completeStationForecastDataDF = pd.merge(completeStationForecastDataDF,filesValuesDF, on='forecastDate', how='outer')

        if 'lat' in completeStationForecastDataDF.columns:
            completeStationForecastDataDF.drop(columns=['lat', 'long'], inplace=True)
        if 'lat_x' in completeStationForecastDataDF.columns:
            completeStationForecastDataDF.drop(columns=['lat_x', 'long_x'], inplace=True)
        if 'lat_y' in completeStationForecastDataDF.columns:
            completeStationForecastDataDF.drop(columns=['lat_y', 'long_y'], inplace=True)
        completeStationForecastDataDF['idweatherforecast']=stationForecastId
        if not fileDownloadConfig is None and "databaseInsertUpdates" in fileDownloadConfig.keys():
            columnsToRename = fileDownloadConfig['columnsToRename']
        # columnsToRename = {
        #     "Dew_point_temperature_height_above_ground": "dewpoint",
        #     "Relative_humidity_height_above_ground": "relativehumidity",
        #     "Temperature_height_above_ground": "temperature",
        #     "Wind_direction_from_which_blowing_height_above_ground": "winddirection",
        #     "Wind_speed_height_above_ground": "windspeed",
        #     "forecastDate": "forecasttimestamp",
        #     "Total_precipitation_surface_0_Hour_Accumulation": "precipitation"
        # }
        completeStationForecastDataDF.rename(columns = columnsToRename, inplace = True)
        completeStationForecastDataJSON = completeStationForecastDataDF.to_json(orient = 'records')
        if not validators.url(forecastDataTable):
            raise Exception("saveStationForecastToDataBase not possible to create a valid url: " + forecastDataTable ) 
        response = httpSession.post(url=requestedURL, data=completeStationForecastDataJSON)
        if response.status_code > 299:
            logging.error("saveStationForecastToDataBase received an status_code: " + str(response.status_code)+
                " to request: " + requestedURL)
            logging.error("\t when posting data: " + completeStationForecastDataJSON  )          
            raise Exception('saveStationForecastToDataBase an error happened when accesing database: '+ str(response.status_code)+
                ' ' + response.reason + ' ' + response.request)

        logging.debug(f'saveStationForecast2DB summary: {completeStationForecastDataJSON}')


    except Exception as e:
        logging.error("saveStationForecastToDataBase catches Exception: "+ str(e))
        raise Exception("saveStationForecastToDataBase catches expection: " + str(e))

def saveStationsForecastDataToDatabase(predictionDate:datetime=None, resolutionid:int=None, 
        stationsForecastData:object=None, providerid:int=None, databaseInsertUpdates:dict = None)->bool:
    saved = True

    try: 
        if predictionDate is None :
            raise Exception("saveStationsForecastDataToDatabase parameter predictionDate is not correctly set." ) 
        if resolutionid is None :
            raise Exception("saveStationsForecastDataToDatabase parameter resolutionid is not correctly set." ) 
        if stationsForecastData is None :
            raise Exception("saveStationsForecastDataToDatabase parameter stationsForecastData is not correctly set." ) 
        if stationsForecastData is None :
            raise Exception("saveStationsForecastDataToDatabase parameter stationsForecastData is not correctly set." ) 
        if databaseInsertUpdates is None:
            raise Exception("saveStationsForecastDataToDatabase parameter databaseInsertUpdates is not correctly set." ) 
        
        for stationForecastData in stationsForecastData:
            if not "stationId" in stationForecastData.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'stationId' key in stationForecastData." ) 
            stationID = stationForecastData['stationId']
            if not "coordinates" in stationForecastData.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'coordinates' key in stationForecastData." ) 
            coordinates = stationForecastData['coordinates']
            if not "lat" in coordinates.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'lat' key in coordinates." ) 
            stationLat = coordinates['lat']
            if not "long" in coordinates.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'long' key in coordinates." ) 
            stationLong = coordinates['long']
            if not "values" in stationForecastData.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'values' key in stationForecastData." ) 
            stationForecastDataValues  = stationForecastData['values']


            stationForecastId, httpSession = getStationForecastID(predictionDate=predictionDate, providerid=providerid, 
                stationid=stationID, resolutionid=resolutionid, databaseInsertUpdates=databaseInsertUpdates,
                long=stationLong, lat=stationLat)
            if not "values" in stationForecastData.keys():
                raise Exception("saveStationsForecastDataToDatabase it was not possible to find 'values' key in stationForecastData." ) 
            stationForecastDataValues  = stationForecastData['values']

            saved = saveStationForecastToDataBase(predictionDate=predictionDate,stationForecastId=stationForecastId, 
                stationForecastDataValues=stationForecastDataValues, httpSession=httpSession, 
                databaseInsertUpdates= databaseInsertUpdates, stationID=stationID)
            logging.debug(f'stationForecastId: {stationForecastId}')

    except Exception as e:
        logging.error("saveStationsForecastDataToDatabase catches Exception: "+ str(e))
        raise Exception("saveStationsForecastDataToDatabase catches expection: " + str(e))
 
    return saved

def getResolutionId(resolution:str=None)-> int:
    resolutionid = None

    if resolution is None:
        resolution = '18km'
    
    if "dataBaseQueries" not in fileDownloadConfig.keys():
            raise Exception("getResolutionId fileDownloadConfig does not contain key dataBaseQueries." ) 
    dataBaseQueries = fileDownloadConfig["dataBaseQueries"]
    if "forecastResolutionIDRequest" not in dataBaseQueries.keys():
            raise Exception("getResolutionId fileDownloadConfig.dataBaseQueries does not contain key forecastResolutionIDRequest." ) 
    forecastResolutionIDRequestURL = dataBaseQueries["forecastResolutionIDRequest"]
    forecastResolutionIDRequestURL = forecastResolutionIDRequestURL.replace("<resolution>", resolution)

    if "user" not in dataBaseQueries.keys():
        raise Exception("getResolutionId fileDownloadConfig.dataBaseQueries does not contain key user." ) 
    user = dataBaseQueries["user"]
    if user is None or len(user) ==0:
        raise Exception("getResolutionId fileDownloadConfig.dataBaseQueries['user'] does not have a valid value." ) 

    if "password" not in dataBaseQueries.keys():
        raise Exception("getResolutionId fileDownloadConfig.dataBaseQueries does not contain key user." ) 
    password = dataBaseQueries["password"]
    if password is None or len(password) ==0:
        raise Exception("getResolutionId fileDownloadConfig.dataBaseQueries['password'] does not have a valid value." ) 

    basic = HTTPBasicAuth(username=user, password=password)
    response = requests.get(forecastResolutionIDRequestURL, auth = basic, verify= fileDownloadConfig["verify_cerificates"])   
    if response.status_code!=200:
        logging.error("getResolutionId received an status_code: " + str(response.status_code)+
            " to request: " + forecastResolutionIDRequestURL)
        if not response.text is None and len(response.text)>0:
            logging.error("\t next message is received: "  + response.text)
        raise Exception("getResolutionId received error: " + str(response.status_code)+ " when requesting climatic station data." )
    weatherForecastIdDataJson = response.json() 

    if len(weatherForecastIdDataJson)>1:
        raise Exception("getResolutionId more than one id recovered for resolutiont: " + resolution+ " :"  + response.text)
    for climaticstationData in weatherForecastIdDataJson:
        resolutionid = climaticstationData["id"]



    return resolutionid


def downloadFromDate( dateFromStr:str=None, endDateStr:str=None, resolution: str = '18km')-> bool:

    downloaded = True
    logMessage= "downloadFromDate"
    # logging.basicConfig(filename=loggerConfiguration["fileName"], 
    # filemode=loggerConfiguration["filemode"], 
    # format=loggerConfiguration["format"],
    # level=loggerConfiguration["level"])
    logging.debug('This will get logged to a file')

    today = date.today()

    if  not dateFromStr is None and len(dateFromStr)> 0:
        try:
            startDate = datetime.datetime.strptime(dateFromStr, '%Y%m%d')
        except Exception as e: 
            logMessage= logMessage + " catches exception: " + str(e) + "\n"
            logMessage= logMessage + "  when transformin date: "+  dateFromStr
            logging.error(logMessage)
            raise Exception(logMessage)
    else:
        startDate = today

    if endDateStr is None or len(endDateStr)==0:
        endDate = today
    else:
        try:
            endDate = datetime.datetime.strptime(endDateStr, '%Y%m%d')
        except Exception as e: 
            logMessage= logMessage + " catches exception: " + str(e) + "\n"
            logMessage= logMessage + "  when transformin date: "+  endDateStr
            logging.error(logMessage)
            raise Exception(logMessage)

    resolutionid = getResolutionId(resolution)
    providerid = 1 #Agroapps


    delta = datetime.timedelta(days=1)
    currentDate = startDate # -2*delta # To force revisit previous days e.g. 03/02/23 does not have data and the 04th contains data. TODO TODELETE
    while (currentDate <= endDate):
        startDateString = currentDate.strftime('%Y%m%d') 

        logging.debug( "=========> Starting donwload forescast Data for date: "+startDateString )

        # stationsForecastData = downloadDataForStations( startDate=startDate, endDate=endDate, resolution=resolution)
        stationsForecastData = downloadDataForStations( startDate=currentDate, resolution=resolution)
        logging.debug( "=========> Ending donwload forescast Data for date: "+startDateString )

        if not stationsForecastData is None and len(stationsForecastData)!=0:
            logging.debug( "=========> Starting saving forescast Data for date: "+startDateString )
            saved = saveStationsForecastDataToDatabase(predictionDate=currentDate, resolutionid=resolutionid, providerid=providerid, 
                stationsForecastData=stationsForecastData, databaseInsertUpdates= fileDownloadConfig['databaseInsertUpdates'])
            logging.debug( "=========> Ending saving forescast Data for date: "+startDateString )
        currentDate += delta

    return downloaded
