import pandas as pd
from datetime import datetime as dtDT
import datetime 
import math

import os
import re
import copy
from HandlingDataFrames.DataFrameHandler import DataFrameHandler
import requests
import json
from requests.auth import HTTPBasicAuth
import urllib3

class PhenologyDataFrame(DataFrameHandler):

    pathAndPatternsSeparator=";"
    mandatoryParameters=["dataDriver", "controlParcelFile", "redFaraPhenologyFile", "phenologicalStateMappingFile" ] 
    supportedODs = ["Calatayud", "Campo de Borja", "Cariñena", "Somontano"]


    cadastralParcelDataBaseService = {
        "serverURL" : "*****.******.******.****",
        "port" : "*******",
        "service" : "agrolake/advancedSearch",
        "method": "post",
        "headers" : {'content-type': 'application/json'},
        "serviceUser" : "**************",
        "servicePassword" : "***************"
    }


    def __init__(self, parameters:dict=None):
        super().__init__(parameters)

        self.controlParcelsDF = None
        self.phenologyRawData = None
        self.phenologicalStateMappingDF = None
        self.parcelPhenologicalDataTimeSeriesDF= None
        self.cadastralDataBaseQuery={
            "bbdd":"DatosGeograficos",
            "coleccion":"ParcelaCatastral",
            "query":{"codigo":"<codigo>"},
            "fields": {"codigo": 1, "codigo":1, "coordenadas_epsgWGS84":1, "rectangleWGS84":1, "coordenadasParcela":1}, 
            "orderField":"descripcion",
            "order":"DESCENDING"
        }


    # For getting the JSON of complex objects.
    def obj_dict(self, obj):
        return obj.__dict__ 

    def readControlParcelsData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.controlParcelFile
        """
        read = True
        filePath = self.parameters.getParameter("controlParcelFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readControlParcelsData: controlParcelFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readControlParcelsData: controlParcelFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readControlParcelsData: controlParcelFile path paremeter does not point to an accesible file:"+ filePath)
        
        df =  self.readDFFromExcelFile(filePath=filePath) 
        self.controlParcelsDF= df
        return read

    def readPhenologicalStateMapping(self, filterColumnNames:list=["Interest"], sortingColumnNames:list=["GrapevineID"])-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.controlParcelFile
        """
        read = True
        filePath = self.parameters.getParameter("phenologicalStateMappingFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readPhenologicalStateMapping: phenologicalStateMappingFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readPhenologicalStateMapping: phenologicalStateMappingFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readPhenologicalStateMapping: phenologicalStateMappingFile path paremeter does not point to an accesible file:"+ filePath)
        
        df =  self.readDFFromExcelFile(filePath=filePath) 
        df =  self.convertColumnsToInteger(dataframe=df, columns=[sortingColumnNames[0]])
        mask = True
        if not (filterColumnNames is None) and len(filterColumnNames)>0:
            mask = mask  & (df[filterColumnNames[0]]=="Yes")
        if not (sortingColumnNames is None) and len(sortingColumnNames) > 0:
            df = df.loc[mask].sort_values(sortingColumnNames[0], ascending=(True))    
        else:
            df = df.loc[mask].sort_values(mask)
        self.phenologicalStateMappingDF= df

        return read


    def readPhenologyRawData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        read = True
        filePath = self.parameters.getParameter("redFaraPhenologyFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readPhenologyRawData: redFaraPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readPhenologyRawData: redFaraPhenologyFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readPhenologyRawData: redFaraPhenologyFile path paremeter does not point to an accesible file:"+ filePath)
        
        df =  self.readDFFromCSVFile(filePath=filePath) 
        self.phenologyRawData= df
        self.phenologyRawData.sort_values(by=['codigo', 'greatest'], inplace=False)
        return read

    
    def readUnizarPhenologyRawData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        read = True
        filePath = self.parameters.getParameter("unizarHistoricPhenologyFile")
        csvSeparator = self.parameters.getParameter("csvSeparator")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readUnizarPhenologyRawData: unizarHistoricPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readUnizarPhenologyRawData: unizarHistoricPhenologyFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readUnizarPhenologyRawData: unizarHistoricPhenologyFile path paremeter does not point to an accesible file:"+ filePath)

        if (csvSeparator is None) or (len(csvSeparator)==0):
            csvSeparator =";"
        
        df =  self.readDFFromCSVFile(filePath=filePath, separator=csvSeparator) 
        self.unizarPhenologyRawData= df
        self.unizarPhenologyRawData.sort_values(by=['codigo', 'greatest'], inplace=False)
        return read


    def readPhenologyProcessedData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        read = True
        filePath = self.parameters.getParameter("redFaraPhenologyFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readPhenologyProcessedData: redFaraPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readPhenologyProcessedData: redFaraPhenologyFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readPhenologyProcessedData: redFaraPhenologyFile path paremeter does not point to an accesible file:"+ filePath)
        
        df =  self.readDFFromCSVFile(filePath=filePath) 
        self.phenologyRawData= df
        self.phenologyRawData.sort_values(by=['codigo', 'greatest'], inplace=False)
        return read


    def savePhenologyRawData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        saved = True
        filePath = self.parameters.getParameter("redFaraPhenologyFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.savePhenologyRawData: redFaraPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.savePhenologyRawData: redFaraPhenologyFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.savePhenologyRawData: redFaraPhenologyFile path paremeter does not point to an accesible file:"+ filePath)
        
        self.saveDataFrameToCsv(self.phenologyRawData, filePath=filePath, fileSeparator=";") 

        return saved

    def readPreprocessedParcelPhenologicalDataTimeSeriesDF(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        read = True
        filePath = self.parameters.getParameter("redFaraPhenologyFile")
        filePath = filePath.replace(".csv", "_procesado.csv")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readPreprocessedParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readPreprocessedParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile is no a valid string.")
        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readPreprocessedParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile path paremeter does not point to an accesible file:"+ filePath)
        
        df = self.readDFFromCSVFile(filePath=filePath, separator=";") 
        self.parcelPhenologicalDataTimeSeriesDF = df
        return read



    def saveParcelPhenologicalDataTimeSeriesDF(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.redFaraPhenologyFile
        """
        saved = True
        filePath = self.parameters.getParameter("redFaraPhenologyFile")
        filePath = filePath.replace(".csv", "_procesado.csv")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.saveParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.saveParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile is no a valid string.")

        # if not(os.path.isfile(filePath)):
        #     raise Exception("PhenologyDataFrame.saveParcelPhenologicalDataTimeSeriesDF: redFaraPhenologyFile path paremeter does not point to an accesible file:"+ filePath)
        
        self.saveDataFrameToCsv( self.parcelPhenologicalDataTimeSeriesDF, filePath=filePath, fileSeparator=";") 

        return saved

    def readProcessedControlParcelsData(self)-> bool:
        read = True
        filePath = self.parameters.getParameter("controlParcelFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.readProcessedControlParcelsData: controlParcelFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.readProcessedControlParcelsData: controlParcelFile is no a valid string.")

        filename, file_extension = os.path.splitext(filePath)
        fileToRead = filename + ".csv"
        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.readProcessedControlParcelsData: controlParcelFile path paremeter does not point to an accesible file:"+ filePath)

        df = self.readDFFromCSVFile(fileToRead,";")
        self.controlParcelsDF = df

        return read


    def saveControlParcelsData(self)-> bool:
        save = True
        filePath = self.parameters.getParameter("controlParcelFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.saveControlParcelsData: controlParcelFile is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("PhenologyDataFrame.saveControlParcelsData: controlParcelFile is no a valid string.")

        if not(os.path.isfile(filePath)):
            raise Exception("PhenologyDataFrame.saveControlParcelsData: controlParcelFile path paremeter does not point to an accesible file:"+ filePath)

        filename, file_extension = os.path.splitext(filePath)
        # dateTimeObj = datetime.datetime.now()
        # currentDate= dateTimeObj.strftime("%Y%m%d_%H%M")
        # fileToSave=filename + "_" + currentDate + ".csv"
        fileToSave = filename + ".csv"
        self.saveDataFrameToCsv(self.controlParcelsDF,fileToSave,";")

        return save
    
    def createSpanishCadastralCode(self, parcelCode:dict={
                "Country":"Country",
                "ProvID":"ProvID",
                "MunID":"MunID",
                "Poligono":"Poligono",
                "Parcela":"Parcela",
            }):

        cadastralCode=""
        keys= parcelCode.keys()
        if not ("Country" in keys):
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode parameter does not contain key Country.")
        if not ("ProvID" in keys):
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode parameter does not contain key ProvID.")
        if not ("MunID" in keys):
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode parameter does not contain key MunID.")
        if not ("Poligono" in keys):
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode parameter does not contain key Poligono.")
        if not ("Parcela" in keys):
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode parameter does not contain key Parcela.")

        country=str(parcelCode["Country"])
        provID=str(parcelCode["ProvID"])
        munID=str(parcelCode["MunID"])
        poligono=str(parcelCode["Poligono"])
        parcela=str(parcelCode["Parcela"])

        if country is None or len(country)==0:
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['Country'] parameter does not have a value.")
        if provID is None or len(provID)==0:
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['ProvID'] parameter does not have a value.")
        if munID is None or len(munID)==0:
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['MunID'] parameter does not have a value.")
        if poligono is None or len(poligono)==0:
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['Poligono'] parameter does not have a value.")
        if parcela is None or len(parcela)==0:
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['Parcela'] parameter does not have a value.")
    
        if country.upper()!="ES":
            raise Exception("PhenologyDataFrame.createSpanishCadastralCode: parcelCode['Country'] is not valid.")

        while (len(provID)<2):
            provID="0"+provID
        while (len(munID)<3):
            munID="0"+munID
        while (len(poligono)<3):
            poligono="0"+poligono
        while (len(parcela)<5):
            parcela="0"+parcela
        
        cadastralCode = provID + munID + "A" + poligono + parcela

        return cadastralCode
    
    def getParcelCoordiantes (self,spanishCadastralParcelCode:str=None)->dict:

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        coordinatesRecord={}

        if (spanishCadastralParcelCode is None) or (len(spanishCadastralParcelCode)==0):
            raise Exception("PhenologyDataFrame.getParcelCoordiantes: spanishCadastralParcelCode parameter is None.")

        query= self.cadastralDataBaseQuery
        query["query"]["codigo"]=spanishCadastralParcelCode

        queryJSON= json.dumps(query, default=self.obj_dict)
        serviceURl="https"+"://"+self.cadastralParcelDataBaseService["serverURL"]+":"+\
            self.cadastralParcelDataBaseService["port"]+"/"+\
            self.cadastralParcelDataBaseService["service"]

        serviceUSer=self.cadastralParcelDataBaseService["serviceUser"]
        servicePassword=self.cadastralParcelDataBaseService["servicePassword"]

        response = requests.post(serviceURl, data=queryJSON, auth=HTTPBasicAuth(serviceUSer, servicePassword), 
            headers=self.cadastralParcelDataBaseService["headers"], verify=False)
            
        coordinatesRecord = (json.loads(response.text))

        return coordinatesRecord
    
    def verifySpanishFilterParcels (self, objectMethod:str="getParcelsCoordinates",
            filterParcels:dict={"ProvID":"50", "OD":"Cariñena", "MunID":"268", "Poligono":"1", "Parcela":"50"})->bool:
        correct = True

        if objectMethod is None or len(objectMethod)==0:
            raise Exception("PhenologyDataFrame.verifySpanishFilterParcels: objectMethod parameter must be especified.")
        if filterParcels is None:
            raise Exception("PhenologyDataFrame.verifySpanishFilterParcels: filterParcels parameter must be especified.")

        # if not "ProvID" in filterParcels.keys() and not "OD" in filterParcels.keys():
        #     raise Exception("PhenologyDataFrame."+ objectMethod +": filterParcels paremeter must contain at least values for 'ProvID' and 'OD'.")

        for key in filterParcels.keys():
            value = filterParcels[key]
            if value is None or len(value)==0:
                raise Exception("PhenologyDataFrame."+ objectMethod +": filterParcels['" +key + "'] paremeter must have a valid value.")
            if key.upper() == 'OD':
                if value not in self.supportedODs:
                    raise Exception("PhenologyDataFrame."+ objectMethod + ": filterParcels['" +key + "'] paremeter must have a valid value once of:" + 
                        self.supportedODs)
            else:
                try:
                    intValue = int(value)
                except Exception as e:
                    raise Exception("PhenologyDataFrame."+ objectMethod + ": filterParcels['" +key + "'] paremeter must be a valid integer:" + value)
                if key == "ProvID":
                    if (intValue<=0) and (intValue>52):
                        raise Exception("PhenologyDataFrame."+ objectMethod + ": filterParcels['" +key + "'] paremeter value: "+ value + " must be in [1, 52] interval")
                if key == "MunID" or key == "Poligono":
                    if (intValue<=0) and (intValue>999):
                        raise Exception("PhenologyDataFrame."+ objectMethod + ": filterParcels['" +key + "'] paremeter value: "+ value + " must be in [1, 999] interval")
                if key == "Parcela":
                    if (intValue<=0) and (intValue>9999):
                        raise Exception("PhenologyDataFrame."+ objectMethod + ": filterParcels['" +key + "'] paremeter value: "+ value + " must be in [1, 9999] interval")

        return  correct

    def createSpanishFilterParcels(self, objectMethod:str="getParcelsCoordinates", 
            filterParcels:dict={"ProvID":"50", "OD":"Cariñena", "MunID":"268", "Poligono":"1", "Parcela":"50"}):
        mask = True

        mask = (self.controlParcelsDF['Considered']=='Yes')
        if "ProvID" in filterParcels.keys():
            ProvID = int(filterParcels["ProvID"])
            mask= mask & \
                (self.controlParcelsDF['ProvID']==ProvID)
        if "OD" in filterParcels.keys():
            OD = filterParcels["OD"]
            mask= mask & \
                (self.controlParcelsDF['OD']==OD)
        if "MunID" in filterParcels.keys():
            munId = int(filterParcels["MunID"])
            mask= mask & \
                (self.controlParcelsDF['MunID']==munId)
        if "Poligono" in filterParcels.keys():
            polId = int(filterParcels["Poligono"])
            mask= mask & \
                (self.controlParcelsDF['Poligono']==polId)
        if "Parcela" in filterParcels.keys():
            parcelId = int(filterParcels["Parcela"])
            mask= mask & \
                (self.controlParcelsDF['Parcela']==parcelId)
            
        return mask

    def getParcelsCoordinates(self, force = False,
        filterParcels:dict={"ProvID":"50", "OD":"Cariñena", "MunID":"268", "Poligono":"1", "Parcela":"50"},
        keyParcelColumns:list=["Country", "ProvID", "MunID", "Poligono", "Parcela"],
        keyDBColumn:list={"codigo"},
        sortingColumnName:str="Localidad")-> bool:
        """
        """
        got = True

        if force:

            self.verifySpanishFilterParcels(objectMethod="getParcelsCoordinates", filterParcels=filterParcels)

            mask = self.createSpanishFilterParcels(objectMethod="getParcelsCoordinates",
                filterParcels=filterParcels)

            parcelsDF = self.controlParcelsDF.loc[mask].sort_values(sortingColumnName, ascending=(True))

            for key in keyParcelColumns:
                if key != "Country":
                    self.controlParcelsDF[key] = self.controlParcelsDF[key].astype(int)

            self.controlParcelsDF["cadastralCode"]="UNKNOWN"
            self.controlParcelsDF["longitude"]=0.0
            self.controlParcelsDF["latitude"]=0.0
            self.controlParcelsDF["altitude"]=0.0
            self.controlParcelsDF["cadastralDefinition"]="UNKNOWN"
                    
            for index,row in parcelsDF.iterrows():
                codProvince=row[keyParcelColumns[1]]
                codMunicipality=row[keyParcelColumns[2]]
                codPolygon=row[keyParcelColumns[3]]
                codParcel=row[keyParcelColumns[4]]
                parcelCode = {
                    keyParcelColumns[0]:"ES",
                    keyParcelColumns[1]:codProvince,
                    keyParcelColumns[2]:codMunicipality,
                    keyParcelColumns[3]:codPolygon,
                    keyParcelColumns[4]:codParcel
                }
                cadastralCode = self.createSpanishCadastralCode(parcelCode=parcelCode)
                coordinates = self.getParcelCoordiantes(spanishCadastralParcelCode=cadastralCode)

                mask = (self.controlParcelsDF['ProvID']==codProvince) & \
                    (self.controlParcelsDF['MunID'] == codMunicipality) & \
                    (self.controlParcelsDF['Poligono'] == codPolygon) & \
                    (self.controlParcelsDF['Parcela'] == codParcel) 

                if len(coordinates["data"])>0:
                    currenCoordinates=coordinates["data"][0]
                    self.controlParcelsDF["cadastralCode"][mask] = cadastralCode
                    self.controlParcelsDF["longitude"][mask] = currenCoordinates["coordenadasParcela"]["longitud"]
                    self.controlParcelsDF["latitude"][mask] = currenCoordinates["coordenadasParcela"]["latitud"]
                    self.controlParcelsDF["altitude"][mask] = currenCoordinates["coordenadasParcela"]["altitud"]
                    jsonCoordinates = json.dumps(currenCoordinates, default=self.obj_dict)
                    self.controlParcelsDF["cadastralDefinition"][mask] = jsonCoordinates

        else:
            self.readProcessedControlParcelsData()

        return got

    def getRedFaraCadastralCode(self, ProvID:int=None, MunID:int=None, Poligono:int=None, Parcela:int=None)->str:
        redFaraCadastralCode1=""
        redFaraCadastralCode2=""

        if ProvID is None :
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: ProvID paremeter does not have any value.")
        if MunID is None :
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: MunID paremeter does not have any value.")
        if Poligono is None :
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: Poligono paremeter does not have any value.")
        if Parcela is None :
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: Parcela paremeter does not have any value.")

        if ProvID <0  and  ProvID>52:
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: ProvID paremeter must have values in [1, 52] integer interval.")
        if MunID <0  and  MunID>999:
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: ProvID paremeter must have values in [1, 999] integer interval.")
        if Poligono <0  and  Poligono>999:
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: Poligono paremeter must have values in [1, 999] integer interval.")
        if Parcela <0  and  Parcela>9999:
            raise Exception("PhenologyDataFrame.getRedFaraCadastralCode: Parcela paremeter must have values in [1, 9999] integer interval.")
    
        redFaraCadastralCode1 = str(ProvID)+":"+ str(MunID)+ ":0:0:"+ str(Poligono)+":"+str(Parcela)
        redFaraCadastralCode2 = redFaraCadastralCode1.replace(":", "-")
        
        return redFaraCadastralCode1, redFaraCadastralCode2
    

    def getMinParcelDate(self, dataframe=None, groupby:list=['codigo', 'especie', 'variedad','plaga_nombre'], 
        dateColumn = "greatest", redFaraCadastralCode1:str="50:6:0:0:40:55",
        redFaraCadastralCode2:str="50-6-0-0-40-55"):
        mask = (dataframe[groupby[0]].str.startswith(redFaraCadastralCode1)) |\
            (dataframe[groupby[0]].str.startswith(redFaraCadastralCode2)) 
        
        selectedDataFrame=dataframe[mask]
        minDF = selectedDataFrame.groupby(groupby, as_index=False).agg({
              dateColumn:['min']
            })
        minDF = self.renameDataFrameColumns(dataFrame=minDF)
        dateColumn=dateColumn+"_min"
        minDate = dtDT.now()
        for index, row in minDF.iterrows():
            currentDate=row[dateColumn]
            if currentDate < minDate:
                minDate =currentDate
        return minDate


    def getSeasonFromDate(self, seasonsDatesDF=None, date=None, 
                dataFrameDateColumns:list=["fecha_min", "fecha_max"],
                dataFrameSeasonColumn:list=["season"]):
        season = "UNKNOWN"
        mask= (seasonsDatesDF[dataFrameDateColumns[0]]<= date) & \
             (seasonsDatesDF[dataFrameDateColumns[1]]>= date)
                
        seasonDF = seasonsDatesDF.loc[mask]

        for index,row in seasonDF.iterrows():
            season= row[dataFrameSeasonColumn[0]]
            break
        
        return season
    
    def getGrapevineAndRedFARAPhenologyMapping(self, 
        stateColumns:list=["GrapevineID","RedFaraDescriptions"])->dict:
        grapeVineStates = {}

        for index, row in self.phenologicalStateMappingDF.iterrows():
            grapevineState=row[stateColumns[0]]
            redFaraStatesJSON =row[stateColumns[1]]
            redFaraStates= json.loads(redFaraStatesJSON)
            grapeVineStates[grapevineState]=[]
            for redFaraState in redFaraStates["texts"]:
                redFaraState = self.cleanRedFaraState(redFaraState)
                grapeVineStates[grapevineState].append(redFaraState)
        return grapeVineStates
    
    def cleanRedFaraState (self, redFaraState:str=None )->str:
        cleanRefFaraState="UNKNOWN" 

        whiteSpacePatter=re.compile(r'[\s\t]*')
        redFaraState = re.sub(whiteSpacePatter, '', redFaraState)
        cleanRefFaraState = redFaraState.upper()

        return cleanRefFaraState
    
    def getGrapeVineFromRedFaraState(self, grapeVineAndRedFaraStates:dict=None, redFaraState:str=None )->str:

        grapeVineState="UNKNOWN" 

        redFaraState = self.cleanRedFaraState(redFaraState)
        for key in grapeVineAndRedFaraStates.keys():
            redFARAStates = grapeVineAndRedFaraStates[key]
            for redFaraMappingState in redFARAStates:
                if redFaraState==redFaraMappingState:
                    grapeVineState=str(key)
                    break
            if grapeVineState!="UNKNOWN":
                break
        return grapeVineState

    def determineParcelPhenologySeasonAndGrapevineState(self, parcelPhenologyDataFrame=None,
            seasonsDatesDF=None, 
            newColumnNames:list=["season", "grapevineState", "od", "climaticStation", "date", 
                                "longitude", "latitude", "altitude"],
            cadastralCode:str ="50", od:str='Campo de Borja',
            longitude:float=41.81401579882942, latitude:float=-1.5301328328763597, altitude:float=451.0,
            climeticStation:str="Z14")->bool:

        determined = True
        unkonwnDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')
        #Initialize new colums
        for key in newColumnNames:
            if key == "date":
                parcelPhenologyDataFrame[key]=unkonwnDate
            else:
                if key.endswith("itude"):
                    parcelPhenologyDataFrame[key]=0.0
                else:
                    parcelPhenologyDataFrame[key]="UNKNOWN"


        grapeVineAndRedFaraStates=self.getGrapevineAndRedFARAPhenologyMapping(stateColumns=["GrapevineID","RedFaraDescriptions"])
        dataFrameSeasonColumn=["season"]
        parcelPhenologyDataFrame.dropna(subset=["texto"], inplace=True) 

        if not (parcelPhenologyDataFrame is  None) and (parcelPhenologyDataFrame.size>0):
            for index, row in parcelPhenologyDataFrame.iterrows():
                redFaraState=row["texto"]
                grapeVineState=self.getGrapeVineFromRedFaraState(grapeVineAndRedFaraStates=grapeVineAndRedFaraStates,
                    redFaraState=redFaraState)
                date=row["greatest"]
                dayDate=date.date()
                parcelCode=row["codigo"]
                variety=row["variedad"]
                season = self.getSeasonFromDate(seasonsDatesDF=seasonsDatesDF, date=date, 
                    dataFrameDateColumns=["fecha_min", "fecha_max"],
                    dataFrameSeasonColumn=dataFrameSeasonColumn)
                self.phenologyRawData.at[index,newColumnNames[0]]=season
                self.phenologyRawData.at[index,newColumnNames[1]]=grapeVineState
                self.phenologyRawData.at[index,newColumnNames[2]]=od
                self.phenologyRawData.at[index,newColumnNames[3]]=climeticStation
                self.phenologyRawData.at[index,newColumnNames[4]]=dayDate
                self.phenologyRawData.at[index,newColumnNames[5]]=longitude
                self.phenologyRawData.at[index,newColumnNames[6]]=latitude
                self.phenologyRawData.at[index,newColumnNames[7]]=altitude
        return determined
    

    def convertRedFaraCodeToSpanishCadastralCode(self, row, originColumn:str="codigo")->str:
        spanishCadastralCode="UNKNOWN"

        try:
            redFaraCode=row[originColumn]
            splitRedFaraCode = re.split('[\:\-]', redFaraCode)
            if len(splitRedFaraCode)>=6:
                province = splitRedFaraCode[0]
                municipality = splitRedFaraCode[1]
                polygon = splitRedFaraCode[4]
                parcel = splitRedFaraCode[5]
                
                if len(province)< 2:
                    province="0"+province
                while len(municipality)<3:
                    municipality="0"+municipality
                while len(polygon)<3:
                    polygon="0"+polygon
                while len(parcel)<5:
                    parcel="0"+parcel
                
                cadastralCode = province + municipality +"A"+\
                    polygon + parcel
                
                if len(cadastralCode)==14:
                    spanishCadastralCode=cadastralCode
        except Exception as e:
            pass

        return spanishCadastralCode

    def setSpanishCadastralCode(self, originColumn:str = "codigo", newColumnName:str="grapevineParcelID")-> bool:

        set = True
        self.phenologyRawData[newColumnName]="UNKNOWN"
        self.phenologyRawData[newColumnName]=self.phenologyRawData.apply (lambda row: self.convertRedFaraCodeToSpanishCadastralCode(row, originColumn), axis=1)
        return set

    def updateDailyPhenologyClimaticDF (self, dailyPhenologyClimaticDF=None, mask=None, 
            selectedPhenologyRawDataFrameColums:list=["grapevineParcelID", "codigo", "especie", "variedad", "texto", "season", 
                "grapevineState", "od", "date","longitude", "latitude", "altitude"],
            sortparcelPhenologyDataFrame: list=["variedad", "date"],
            row=None, previuosGrapevineState:str="0"):
        
        

        for column in selectedPhenologyRawDataFrameColums:
            dailyPhenologycolumn= column
            if column.endswith("tude"):
                dailyPhenologycolumn= "Parcel_"+dailyPhenologycolumn
            if column!="grapevineState":
                dailyPhenologyClimaticDF[dailyPhenologycolumn][mask] = \
                    row[column]
            else:
                #Phenologigal states can not return to a previous one
                try:
                    if not math.isnan(float(previuosGrapevineState)):
                        intPreviuosGrapevineState = int(previuosGrapevineState)
                    else:
                        intPreviuosGrapevineState = 0
                except Exception as e:
                    intPreviuosGrapevineState = 0
                
                try:
                    if not math.isnan(float(row[column])):
                        currentPreviuosGrapevineState = int(previuosGrapevineState)
                    else:
                        currentPreviuosGrapevineState = 0
                except  Exception as e:
                    currentPreviuosGrapevineState = 0
                if (intPreviuosGrapevineState > currentPreviuosGrapevineState):
                    currentPreviuosGrapevineState = intPreviuosGrapevineState
                dailyPhenologyClimaticDF[dailyPhenologycolumn][mask] = \
                    currentPreviuosGrapevineState
                

        return dailyPhenologyClimaticDF
        
    
    def createDailyParcelPhenologyClimaticObservationDF(self, cadastralCode:str="", dailyPhenologyClimaticDF=None,
        selectedPhenologyRawDataFrameColums:list = ["grapevineParcelID", "codigo", "especie", "variedad", "texto", "climeticStation",
            "season", "grapevineState", "od", "date","longitude", "latitude", "altitude"],
        sortparcelPhenologyDataFrame: list=["variedad", "date"],
        sortingOrder =(True, True),
        dailyPhenologyClimaticDFColumsToDelete:list=[],
        longitude: float =0.0, latitude:float=0.0, altitude:float=0.0, species:str="Vitis Vinicola", 
        variety:str="Cabernet-Sauvignon", od:str="50", 
        redFaraCadastralCode:str="50:3:0:0:3:37",
        climeticStation:str="Z14" ):

        if dailyPhenologyClimaticDF is None :
            raise Exception("PhenologyDataFrame.createDailyParcelPhenologyClimaticObservationDF: dailyPhenologyClimaticDF paremeter does not have any value.")

        if cadastralCode is None or len(cadastralCode)==0 :
            raise Exception("PhenologyDataFrame.createDailyParcelPhenologyClimaticObservationDF: cadastralCode paremeter does not have any value.")

       

        if (dailyPhenologyClimaticDF.size>0):
            #Initialize new dailyPhenologyClimaticDF colums
            unkonwnDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')
            for newDailyColumn in selectedPhenologyRawDataFrameColums:
                if newDailyColumn == "grapevineParcelID":
                    dailyPhenologyClimaticDF[newDailyColumn]=cadastralCode
                if newDailyColumn=="codigo":
                    dailyPhenologyClimaticDF[newDailyColumn]=redFaraCadastralCode
                if newDailyColumn == "especie":
                    dailyPhenologyClimaticDF[newDailyColumn]=species
                if newDailyColumn == "variedad":
                    dailyPhenologyClimaticDF[newDailyColumn]=variety
                if newDailyColumn == "texto":
                    dailyPhenologyClimaticDF[newDailyColumn]="Dormancy"
                if newDailyColumn == "climeticStation":
                    dailyPhenologyClimaticDF[newDailyColumn]=climeticStation
                # if newDailyColumn != "season":
                #     dailyPhenologyClimaticDF[newDailyColumn]="UNKNOWN"
                if newDailyColumn=="grapevineState":
                    dailyPhenologyClimaticDF[newDailyColumn]="0"
                if newDailyColumn == "od":
                    dailyPhenologyClimaticDF[newDailyColumn]=od
                if newDailyColumn == "date":
                    dailyPhenologyClimaticDF[newDailyColumn]=unkonwnDate
                if newDailyColumn==("longitude"):
                    dailyPhenologyClimaticDF["Parcel_" + newDailyColumn]=longitude
                if newDailyColumn==("latitude"):
                    dailyPhenologyClimaticDF["Parcel_" + newDailyColumn]=latitude
                if newDailyColumn==("altitude"):
                    dailyPhenologyClimaticDF["Parcel_"+newDailyColumn]=altitude
                            
                                

            parcelPhenologyMask= (self.phenologyRawData["grapevineParcelID"]==cadastralCode)
            parcelPhenologyDataFrame = self.phenologyRawData.loc[parcelPhenologyMask, \
                selectedPhenologyRawDataFrameColums].drop_duplicates().\
                sort_values(by=sortparcelPhenologyDataFrame, ascending=sortingOrder)
            
            fromDate= unkonwnDate
            toDate= unkonwnDate
            oldRow = None
            self.convertColumnsToDatetime(dataframe=parcelPhenologyDataFrame,columns=[selectedPhenologyRawDataFrameColums[9]])
            previuosGrapevineState = "0"
            for index, row in parcelPhenologyDataFrame.iterrows():
                if not oldRow is None:
                    # I have read at least two rows

                    season= oldRow[selectedPhenologyRawDataFrameColums[6]]
                    fromDate= oldRow[selectedPhenologyRawDataFrameColums[9]]

                    toDate=row[selectedPhenologyRawDataFrameColums[9]]

                    dailyMask=(dailyPhenologyClimaticDF["fecha"]>= fromDate) &\
                        (dailyPhenologyClimaticDF["fecha"]< toDate) &\
                        (dailyPhenologyClimaticDF["season"]== season)
                    
                    dailyPhenologyClimaticDF=self.updateDailyPhenologyClimaticDF(dailyPhenologyClimaticDF=dailyPhenologyClimaticDF,
                        mask=dailyMask,
                        selectedPhenologyRawDataFrameColums=selectedPhenologyRawDataFrameColums,
                        row=oldRow, previuosGrapevineState=previuosGrapevineState)
                
                oldRow = row
                previuosGrapevineState = oldRow ["grapevineState"]
            
            # Updating last season records to last phenological state
            season= oldRow[selectedPhenologyRawDataFrameColums[6]]
            fromDate= oldRow[selectedPhenologyRawDataFrameColums[9]]
            dailyMask=(dailyPhenologyClimaticDF["fecha"]>= fromDate) &\
                (dailyPhenologyClimaticDF["season"]== season)
            dailyPhenologyClimaticDF=self.updateDailyPhenologyClimaticDF(dailyPhenologyClimaticDF=dailyPhenologyClimaticDF,
                mask=dailyMask,
                selectedPhenologyRawDataFrameColums=selectedPhenologyRawDataFrameColums,
                row=oldRow, previuosGrapevineState=previuosGrapevineState)

        return dailyPhenologyClimaticDF

    def getParcelClimaticData(self, force:bool=False,  
        parcelFilter:dict={"ProvID":"50", "OD":"Cariñena", "MunID":"268", "Poligono":"1", "Parcela":"50"},
        keyParcelColumns:list=["Country", "OD", "ProvID", "MunID", "Poligono", "Parcela", "cadastralCode",
            "longitude","latitude","altitude", "Variedad", "Especie"],
        keyPhenologyColumns:list=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"],
        sortingColumnName:str="cadastralCode",
        climaticStationsDataFrame=None)->bool: 
        got=True


        if force is None:
            force = False
        if parcelFilter is None :
            raise Exception("PhenologyDataFrame.getParcelClimaticData: parcelFilter paremeter does not have any value.")

        self.verifySpanishFilterParcels(objectMethod="getParcelClimaticData", filterParcels=parcelFilter)

        if force:
            # Reading Phenology File
            self.readPhenologyRawData()
            self.phenologyRawData[keyPhenologyColumns[1]]= pd.to_datetime(self.phenologyRawData[keyPhenologyColumns[1]]) 

            parcelMask = self.createSpanishFilterParcels(objectMethod="getParcelClimaticData",filterParcels=parcelFilter)
            parcelsDF = self.controlParcelsDF.loc[parcelMask].sort_values(sortingColumnName, ascending=(True))

            #For reading data only in case of changes.
            oldOD=None
            oldStation=None
            hourlyPhenologicalParcelDataFrames=[]
            dailyPhenologicalParcelDataFrames=[]

            # For each control parcel in PhenologyRawDataDataframe
            for index,row in parcelsDF.iterrows():
                od=row[keyParcelColumns[1]]
                ProvID= row[keyParcelColumns[2]]
                MunID= row[keyParcelColumns[3]]
                Poligono= row[keyParcelColumns[4]]
                Parcela= row[keyParcelColumns[5]]
                cadastralCode=row[keyParcelColumns[6]]
                longitude=row[keyParcelColumns[7]]
                latitude=row[keyParcelColumns[8]]
                altitude=row[keyParcelColumns[9]]
                variety=row[keyParcelColumns[10]]
                species=row[keyParcelColumns[11]]
                # RedFaraCadastralCodex can contain enclosure !!!!!
                redFaraCadastralCode1, redFaraCadastralCode2 = self.getRedFaraCadastralCode(ProvID=ProvID, MunID=MunID,
                    Poligono=Poligono, Parcela=Parcela)
                self.setSpanishCadastralCode(originColumn="codigo", newColumnName="grapevineParcelID")
                # Get available phenological data for the parcel.
                phenologyMask = (self.phenologyRawData[keyPhenologyColumns[0]].str.startswith(redFaraCadastralCode1)) | \
                    (self.phenologyRawData[keyPhenologyColumns[0]].str.startswith(redFaraCadastralCode2)) 
                parcelPhenologyDataFrame = self.phenologyRawData[phenologyMask]
                if (parcelPhenologyDataFrame.size>0):
                    # Process phenological data
                    # For each parcel get the minimum date for which there is data
                    minDate= self.getMinParcelDate(dataframe=parcelPhenologyDataFrame,
                        groupby=[keyPhenologyColumns[0], keyPhenologyColumns[2], 
                            keyPhenologyColumns[3], keyPhenologyColumns[4]],
                        dateColumn = keyPhenologyColumns[1], redFaraCadastralCode1=redFaraCadastralCode1, 
                        redFaraCadastralCode2 = redFaraCadastralCode2)
                    # Just in case od change read climatic data
                    if (od!=oldOD):
                        climaticStationsDataFrame.readDFsFromCVSFiles(od=od, fileSeparator=";")
                        oldOD=od
                    # Getting climaticStation (currently one for each od) hourly and daily observations
                    hourlyDF, dailyDF, climeticStation= climaticStationsDataFrame.getClimaticDataForParcels(od=od, 
                        longitude=longitude, latitude=latitude, altitude=altitude,
                        dateFrom=minDate)
                    # Creating Season and grapevinePhenological state columns based on mapping file
                    if climeticStation!=oldStation:
                        seasonsDatesDF = climaticStationsDataFrame.getSeasonForStation(station=climeticStation, dateFrom=minDate,
                            filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"])

                    self.determineParcelPhenologySeasonAndGrapevineState( parcelPhenologyDataFrame=parcelPhenologyDataFrame,
                            seasonsDatesDF=seasonsDatesDF, 
                            newColumnNames=["season", "grapevineState", "od", "climaticStation", "date", 
                                "longitude", "latitude", "altitude"],
                            cadastralCode =cadastralCode, od=od, longitude=longitude, latitude=latitude, altitude=altitude,
                            climeticStation=climeticStation)
                    
                    # For each parcel in
                    parcelPhenololyClimaticDF = self.createDailyParcelPhenologyClimaticObservationDF(cadastralCode=cadastralCode,
                        dailyPhenologyClimaticDF=dailyDF.copy(), 
                        selectedPhenologyRawDataFrameColums = ["grapevineParcelID", "codigo", "especie", "variedad", "texto", "climeticStation",
                            "season", "grapevineState", "od", "date","longitude", "latitude", "altitude"],
                        sortparcelPhenologyDataFrame=["variedad", "date"], sortingOrder =(True, True), 
                        dailyPhenologyClimaticDFColumsToDelete=[],
                        longitude=longitude, latitude=latitude, altitude=altitude, 
                        species=species, variety=variety, od=od, redFaraCadastralCode=redFaraCadastralCode1,
                        climeticStation=climeticStation)
                    
                    dailyPhenologicalParcelDataFrames.append(parcelPhenololyClimaticDF)
            self.parcelPhenologicalDataTimeSeriesDF=pd.concat(dailyPhenologicalParcelDataFrames)

        else:
            self.readPhenologyRawData()
            self.readPreprocessedParcelPhenologicalDataTimeSeriesDF()      
        return got
    
    def getProcessedPhenologyCorrelationMatrix(self, 
            selectedFields:list=["tmed_min", "tmed_max", "tmed_mean", "rad_min", "rad_max", "rad_mean", 
                "season", "gdd_4.5_t0_Tbase_sum_Cumm", "gdd_4.5_t0_TbaseMax_sum_Cumm", "gdd_4.5_1_Tbase_sum_Cumm", 
                "gdd_4.5_1_TbaseMax_sum_Cumm", "gdd_4.5_2_Tbase_sum_Cumm", "gdd_4.5_2_TbaseMax_sum_Cumm", "gdd_10.0_t0_Tbase_sum_Cumm", 
                "gdd_10.0_t0_TbaseMax_sum_Cumm", "gdd_10.0_1_Tbase_sum_Cumm", "gdd_10.0_1_TbaseMax_sum_Cumm", "gdd_10.0_2_Tbase_sum_Cumm", 
                "gdd_10.0_2_TbaseMax_sum_Cumm", "chillingDD_7.0_t0_Tbase_sum_Cumm", "chillingDD_7.0_t0_Tbasemin_sum_Cumm", 
                "chillingDD_7.0_t0_Utah_sum_Cumm", "chillingDD_7.0_1_Tbase_sum_Cumm", "chillingDD_7.0_1_Tbasemin_sum_Cumm", 
                "chillingDD_7.0_1_Utah_sum_Cumm", "chillingDD_7.0_2_Tbase_sum_Cumm", "chillingDD_7.0_2_Tbasemin_sum_Cumm", 
                "chillingDD_7.0_2_Utah_sum_Cumm", "rad__t0__Cumm", "rad__1__Cumm", "rad__2__Cumm", "precip__t0__Cumm", 
                "precip__1__Cumm", "precip__2__Cumm", "winkler_4.5_t0_Tbase_Cumm", "winkler_4.5_t0_TbaseMax_Cumm", 
                "winkler_4.5_1_Tbase_Cumm", "winkler_4.5_1_TbaseMax_Cumm", "winkler_4.5_2_Tbase_Cumm", "winkler_4.5_2_TbaseMax_Cumm", 
                "winkler_10.0_t0_Tbase_Cumm", "winkler_10.0_t0_TbaseMax_Cumm", "winkler_10.0_1_Tbase_Cumm", "winkler_10.0_1_TbaseMax_Cumm", 
                "winkler_10.0_2_Tbase_Cumm", "winkler_10.0_2_TbaseMax_Cumm", "wind_N", "wind_NE", "wind_E", "wind_SE", "wind_S", 
                "wind_SW", "wind_W", "wind_NW", "SeasonDay_t0", "SeasonDay_1", "SeasonDay_2", "stationLongitude", "stationLatitude", 
                "stationAltitude", "grapevineParcelID", "variedad", "grapevineState", "Parcel_longitude", "Parcel_latitude", "Parcel_altitude"],
            consideredSeasons: list = ["2016_2017", "2017_2018", "2018_2029"],
            consideredPhenologicalStates: list =[0, 1, 2, 3, 4],
            normalizationMethod:str = "min_max_scaler",
            notNormalizedColumns:list = []):

        correlationMatrix = None

    
        if not (consideredSeasons is None) and (len(consideredSeasons)!=0):
            desiredSeasonsMasks = []
            for season in consideredSeasons:
                mask = (self.parcelPhenologicalDataTimeSeriesDF["season"]== season)
                desiredSeasonsMasks.append(mask)
            orMask = desiredSeasonsMasks[0]
            for mask in desiredSeasonsMasks[1:]:
                orMask = orMask | mask
            selectedParcelPhenologicalDataTimeSeriesDF = self.parcelPhenologicalDataTimeSeriesDF.loc[orMask]
        else:
            selectedParcelPhenologicalDataTimeSeriesDF = self.parcelPhenologicalDataTimeSeriesDF

        if not (consideredPhenologicalStates is None) and (len(consideredPhenologicalStates)!=0):
            desideredStatesMasks = []
            for state in consideredPhenologicalStates:
                mask = (self.parcelPhenologicalDataTimeSeriesDF["grapevineState"]== state)
                desideredStatesMasks.append(mask)
            orMask = desideredStatesMasks[0]
            for mask in desideredStatesMasks[1:]:
                orMask = orMask | mask
            selectedParcelPhenologicalDataTimeSeriesDF = selectedParcelPhenologicalDataTimeSeriesDF.loc[orMask]
        
        selectedParcelPhenologicalDataTimeSeriesDF = self.getSelectedColumnsDF(dataframe=selectedParcelPhenologicalDataTimeSeriesDF,
            selectedColumns=selectedFields)

        if not (normalizationMethod is None) and len(normalizationMethod)>0:
            selectedParcelPhenologicalDataTimeSeriesDF = self.normalizeDataFrame(selectedParcelPhenologicalDataTimeSeriesDF,
                notNormalizedColumns=notNormalizedColumns, normalizationMethod=normalizationMethod)


        correlationMatrix = self.getColumnsCorrelation(selectedParcelPhenologicalDataTimeSeriesDF)

        return correlationMatrix 
    
    def selectUnizarControlParcelsData(self, selectingFields:dict={"Comentario": "Unizar", "Considered": "Yes"})-> bool:
        selected = True
        filePath = self.parameters.getParameter("controlParcelFile")
        if not isinstance(filePath, str):
            raise Exception("PhenologyDataFrame.selectUnizarControlParcelsData: controlParcelFile is no a valid string.")
        if (self.controlParcelsDF is None) or (len(self.controlParcelsDF.index)==0):
            self.readControlParcelsData()

        df =  self.controlParcelsDF

        if (selectingFields is None) or (len(selectingFields)==0):
            selectingFields:dict={"Comentario": "Unizar", "Considered": "Yes"}
        
        mask = True
        for key in selectingFields.keys():
            mask = mask & (df[key]==selectingFields[key])
        
        df = df.loc[mask]

        self.unizarControlParcelsDF= df
        return selected

    def reviewUnizarPhenologyRawData(self,
            keyPhenologyColumns:list=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"])->bool: 
        reviewed = True
        if self.unizarPhenologyRawData is None:
            raise Exception("PhenologyDataFrame.reviewUnizarPhenologyRawData: reviewUnizarPhenologyRawData dataframe is empty.")
        if self.phenologyRawData is None:
            raise Exception("PhenologyDataFrame.reviewUnizarPhenologyRawData: phenologyRawData dataframe is empty.")
        phenologyRawDataColumns = self.phenologyRawData.columns
        unizarPhenologyRawDataColumns = self.unizarPhenologyRawData.columns
        for column in phenologyRawDataColumns:
            if not column in unizarPhenologyRawDataColumns:
                raise Exception("PhenologyDataFrame.reviewUnizarPhenologyRawData: reviewUnizarPhenologyRawData does not contain column:"+
                    column+".")
                
        for column in unizarPhenologyRawDataColumns:
            if not column in phenologyRawDataColumns:
                self.unizarPhenologyRawData.drop(column,inplace=True, axis=1)
            
        phenologyRawDataColumns = self.phenologyRawData.columns
        unizarPhenologyRawDataColumns = self.unizarPhenologyRawData.columns

        if len(phenologyRawDataColumns)!= len(unizarPhenologyRawDataColumns):
            raise Exception("PhenologyDataFrame.reviewUnizarPhenologyRawData: reviewUnizarPhenologyRawData  and phenologyRawData does not have the same columns.")
    
        for columns in keyPhenologyColumns:
            if not column in unizarPhenologyRawDataColumns:
                raise Exception("PhenologyDataFrame.reviewUnizarPhenologyRawData: reviewUnizarPhenologyRawData does not contain column:"+
                    column+".")
        
        self.unizarPhenologyRawData[keyPhenologyColumns[1]]= pd.to_datetime(self.unizarPhenologyRawData[keyPhenologyColumns[1]]) 
    
        return reviewed
    
    def mergeRedFaraUnizarDataFrames(self)->bool:
        merged = True
        
        dataFrames=[]
        redFaraDF= self.phenologyRawData
        dataFrames.append(redFaraDF)
        unizarDF = self.unizarPhenologyRawData
        dataFrames.append(unizarDF)

        self.phenologyRawData = pd.concat(dataFrames)
        
        return merged