import pandas as pd
import numpy as np
import datetime 
from datetime import datetime as dtDT
from datetime import date
import os
import re
import copy
from HandlingDataFrames.DataFrameHandler import DataFrameHandler
import psycopg2
from sqlalchemy import create_engine
import config as cf
import math


def add_hours(fecha, minutes_sconds):
    return fecha + pd.DateOffset(hours=float(minutes_sconds//100), minutes= float(minutes_sconds%100))


def getStartDate (startYear:int=None)->datetime: 
    if startYear is None:
        raise Exception("ClimaticStationDataFrame.getStartDate: invoqued with and invalid value for startYear parameter.")
    return datetime.datetime(startYear-1, 9, 1)

def get_past_meteorological(estaciones:list=None, startYear:int=None, last=False):
    conexion = psycopg2.connect(database=cf.postgress_Database, #meteorological_past_database, 
                                user=cf.postgress_Username,#meteorological_past_user, 
                                password=cf.postgress_Password,#meteorological_past_password, 
                                host=cf.postgress_Host,#meteorological_past_host, 
                                port=cf.postgress_Port)#meteorological_past_port)
    '''
    last: Turned True if we only want the last values of each station (for prediction). If the full database is 
    desired use last='False'.
    '''
    data = None

    current_year = date.today().year
    if startYear is None:
        startYear = date.today().year
    
    if startYear < 2008 or startYear > current_year:
        raise Exception("ClimaticStationDataFrame.get_past_meteorological: invoqued with an invalid value for startYear parameter: " + str(startYear))
    
    if estaciones is None:
        raise Exception("ClimaticStationDataFrame.get_past_meteorological: estaciones parameter has not value." )
    
    whereStr = ""

    query_Template = "select * "
    query_Template = query_Template + "FROM public.gv_aragoneseclimaticstationsdatawithforecastandestimatedata2 acsd"

    if last:
        query_Template = query_Template 
    if not estaciones is None and  len (estaciones)> 0:
        lista_estaciones = ""
        index = 0
        for estacion in estaciones:
            if index == 0:
                lista_estaciones = lista_estaciones + "("
            lista_estaciones = lista_estaciones + "'" + estacion + "',"
            index = index + 1
        lista_estaciones = lista_estaciones[:-1] + ")"
        #whereStr = " where upper(acsd.nombrecorto) in " + lista_estaciones
        whereStr = " where acsd.nombrecorto in " + lista_estaciones

    if last:
        if len(whereStr)==0:
            whereStr = " where "
        else:
            whereStr = whereStr + " and"
        whereStr = whereStr + " acsd.fecha>= (CURRENT_DATE-21) "

    if startYear > 2000 and startYear <= current_year:
        #Todo look for the minus starting date of the sessions (to aggregate from then)
        # startDate = datetime.datetime(startYear, 1, 1)
        # si existen trato de coger datos que permitan determinar el comienzo de la campaña "segun las temperaturas de otoño"
        startDate = getStartDate(startYear)
        startDateStr = startDate.strftime('%Y-%m-%d')
        if len(whereStr)==0:
            whereStr = " where "
        else:
            whereStr = whereStr + " and"
        whereStr = whereStr + " acsd.fecha>='" + startDateStr + "'"

    query_Template = query_Template + whereStr
    query_Template = query_Template + " order by acsd.idestacion, acsd.fecha, acsd.hora"
    query = query_Template

    data = pd.read_sql_query(query, con=conexion)# conexion.commit done
    conexion.commit()
    conexion.close()
        
    data['fecha']=data.apply(lambda row: add_hours(row['fecha'],row['horamin']),axis=1)
    
    data=data[['fecha', 'nombrecorto', 'idprovincia', 'idestacion', 'tempmedia', 'humedadmedia', 'velviento', 'dirviento', 
            'radiacion', 'precipitacion', 'tempmediacaja', 'tempsuelo1', 'tempsuelo2', 'codtempsuelo2', 'xutm', 'yutm', 
            'huso', 'altitud', 'latitude', 'longitude', 'horamin']]
    
    return data


class ClimaticStationDataFrame(DataFrameHandler):

    pathAndPatternsSeparator=";"
    mandatoryParameters=["dataDriver", "diaryObservationsPathsAndPatterns", "stationsGISPathDefinitions", 
    "warmingThresHoldArray", "timeZeros", "warmingHourlyMethods", "maxWarmingThresHold",
    "chillingThresHoldArray", "minChillingThreshold", "warmingHourlyMethods",
    "hourlyPreprocessesFileName"] 
    columns=['estacion', 'ubi', 'anio', 'dia', 'fecha', 'horamin', 'tmed', 'hr', 'vv', 'dv', 'rad',	'precip','t10',	't30']
    stationGISColumns=["ID", "Station (Settelment)", "Longitude", "Latitude", "Altitude", "OD"] 


    observationDFIntegerColumns= [ "anio", "dia", "horamin"]
    observationDFDateTimeColumns=["timeStamp", "weekReference"]
    observationDFTimeColumns=["horaminTime"]
    observationDFFloatColumns=["tmed", "hr", "vv", "dv", "rad", "precip", "t10", "t30", 
        "hourFrac", "warmingFraction_4.5", "warmingFraction_4.5_Max", "warmingFraction_10.0", "warmingFraction_10.0_Max", 
        "chillingFraction_7.0", "chillingFraction_min", 
        "gdd_4.5_t0_Tbase", "gdd_4.5_t0_TbaseMax", "gdd_4.5_1_Tbase", "gdd_4.5_1_TbaseMax", "gdd_4.5_2_Tbase", "gdd_4.5_2_TbaseMax", 
        "gdd_10.0_t0_Tbase", "gdd_10.0_t0_TbaseMax", "gdd_10.0_1_Tbase", "gdd_10.0_1_TbaseMax", "gdd_10.0_2_Tbase", "gdd_10.0_2_TbaseMax", 
        "chillingDD_7.0_t0_Tbase", "chillingDD_7.0_t0_Tbasemin", "chillingDD_7.0_t0_Utah", "chillingDD_7.0_1_Tbase", "chillingDD_7.0_1_Tbasemin", 
        "chillingDD_7.0_1_Utah", "chillingDD_7.0_2_Tbase", "chillingDD_7.0_2_Tbasemin", "chillingDD_7.0_2_Utah", 
        "gdd_4.5_t0_Tbase_Cumm", "gdd_4.5_t0_TbaseMax_Cumm", "gdd_4.5_1_Tbase_Cumm", "gdd_4.5_1_TbaseMax_Cumm", 
        "gdd_4.5_2_Tbase_Cumm", "gdd_4.5_2_TbaseMax_Cumm", "gdd_10.0_t0_Tbase_Cumm", "gdd_10.0_t0_TbaseMax_Cumm", "gdd_10.0_1_Tbase_Cumm", 
        "gdd_10.0_1_TbaseMax_Cumm", "gdd_10.0_2_Tbase_Cumm", "gdd_10.0_2_TbaseMax_Cumm", "chillingDD_7.0_t0_Tbase_Cumm", 
        "chillingDD_7.0_t0_Tbasemin_Cumm", "chillingDD_7.0_t0_Utah_Cumm", "chillingDD_7.0_1_Tbase_Cumm", "chillingDD_7.0_1_Tbasemin_Cumm", 
        "chillingDD_7.0_1_Utah_Cumm", "chillingDD_7.0_2_Tbase_Cumm", "chillingDD_7.0_2_Tbasemin_Cumm", "chillingDD_7.0_2_Utah_Cumm", 
        "rad__t0__Cumm", "rad__1__Cumm", "rad__2__Cumm", "precip__t0__Cumm", "precip__1__Cumm", "precip__2__Cumm"]

    dailyObservationsDFIntegerColumns=["anio", "dia"]
    dailyObservationDFFloatColumns=["tmed_min", "tmed_max", "tmed_mean", "rad_min", "rad_max", "rad_mean", "hourFrac_sum", "potentialDormancyDay", 
        "gdd_4.5_t0_Tbase_sum", "gdd_4.5_t0_TbaseMax_sum", "gdd_4.5_1_Tbase_sum", "gdd_4.5_1_TbaseMax_sum", "gdd_4.5_2_Tbase_sum", 
        "gdd_4.5_2_TbaseMax_sum", "gdd_10.0_t0_Tbase_sum", "gdd_10.0_t0_TbaseMax_sum", "gdd_10.0_1_Tbase_sum", 
        "gdd_10.0_1_TbaseMax_sum", "gdd_10.0_2_Tbase_sum", "gdd_10.0_2_TbaseMax_sum", "chillingDD_7.0_t0_Tbase_sum", 
        "chillingDD_7.0_t0_Tbasemin_sum", "chillingDD_7.0_t0_Utah_sum", "chillingDD_7.0_1_Tbase_sum", 
        "chillingDD_7.0_1_Tbasemin_sum", "chillingDD_7.0_1_Utah_sum", "chillingDD_7.0_2_Tbase_sum", 
        "chillingDD_7.0_2_Tbasemin_sum", "chillingDD_7.0_2_Utah_sum", "rad_sum", "precip_sum", "winkler_4.5_Tbase", 
        "winkler_4.5_TbaseMax", "winkler_10.0_Tbase", "winkler_10.0_TbaseMax", "gdd_4.5_t0_Tbase_sum_Cumm", "gdd_4.5_t0_TbaseMax_sum_Cumm", 
        "gdd_4.5_1_Tbase_sum_Cumm", "gdd_4.5_1_TbaseMax_sum_Cumm", "gdd_4.5_2_Tbase_sum_Cumm", "gdd_4.5_2_TbaseMax_sum_Cumm", 
        "gdd_10.0_t0_Tbase_sum_Cumm", "gdd_10.0_t0_TbaseMax_sum_Cumm", "gdd_10.0_1_Tbase_sum_Cumm", "gdd_10.0_1_TbaseMax_sum_Cumm", 
        "gdd_10.0_2_Tbase_sum_Cumm", "gdd_10.0_2_TbaseMax_sum_Cumm", "chillingDD_7.0_t0_Tbase_sum_Cumm", "chillingDD_7.0_t0_Tbasemin_sum_Cumm", 
        "chillingDD_7.0_t0_Utah_sum_Cumm", "chillingDD_7.0_1_Tbase_sum_Cumm", "chillingDD_7.0_1_Tbasemin_sum_Cumm", "chillingDD_7.0_1_Utah_sum_Cumm", 
        "chillingDD_7.0_2_Tbase_sum_Cumm", "chillingDD_7.0_2_Tbasemin_sum_Cumm", "chillingDD_7.0_2_Utah_sum_Cumm", "rad__t0__Cumm", 
        "rad__1__Cumm", "rad__2__Cumm", "precip__t0__Cumm", "precip__1__Cumm", "precip__2__Cumm", "winkler_4.5_t0_Tbase_Cumm", "winkler_4.5_t0_TbaseMax_Cumm", 
        "winkler_4.5_1_Tbase_Cumm", "winkler_4.5_1_TbaseMax_Cumm", "winkler_4.5_2_Tbase_Cumm", "winkler_4.5_2_TbaseMax_Cumm", "winkler_10.0_t0_Tbase_Cumm", 
        "winkler_10.0_t0_TbaseMax_Cumm", "winkler_10.0_1_Tbase_Cumm", "winkler_10.0_1_TbaseMax_Cumm", "winkler_10.0_2_Tbase_Cumm", "winkler_10.0_2_TbaseMax_Cumm", 
        "wind_N", "wind_NE", "wind_E", "wind_SE", "wind_S", "wind_SW", "wind_W", "wind_NW"	]

    dailyObservationDFDatetimeColumns=["fecha"]




    hourlyPreprocessFiName="PreprocessedHourlyObservations.csv"


    def __init__(self, parameters:dict=None):
        super().__init__(parameters)
        #self.parameters = parameters
        if not self.parameters.areAllMandatoryParametersPresent():
            raise Exception("ClimaticStationDataFrame.__init__: not all mandatoryParameters ("+ 
                    str(self.mandatoryParameters)+ ")) are set.")

        # Climatic Stations definition        
        self.stationGISDF = None

        # Climatic Observations
        self.observationsDF= None #"Hourly"
        self.dailyObservationsDF = None #Daily"
        # For Gubler  Index calculation
        self.dailyGublerDF = None
        self.hourlyGublerDF = None

        self.timeZerosArray=self.parameters.getParameter("timeZeros").split(";")

        self.warmingThresHoldArray=self.parameters.getParameter("warmingThresHoldArray").split(";")
        self.warmingHourlyMethodsArray=self.parameters.getParameter("warmingHourlyMethods").split(";")
        self.maxWarmingThresHold=self.parameters.getParameter("maxWarmingThresHold")
    
        self.chillingThresHoldArray=self.parameters.getParameter("chillingThresHoldArray").split(";")
        self.minChillingThreshold = self.parameters.getParameter("minChillingThreshold")
        self.chillingHourlyMethodsArray=self.parameters.getParameter("chillingHourlyMethods").split(";")




   
    def readStationGIS(self)->bool:
        read = True

        stationsGISPathDefinitions = self.parameters.getParameter("stationsGISPathDefinitions")
        if not isinstance(stationsGISPathDefinitions, str):
            raise Exception("ClimaticStationDataFrame.readStationGIS: stationsGISPathDefinitions is no a valid string.")
        if (stationsGISPathDefinitions is None) or (len(stationsGISPathDefinitions)==0):
            raise Exception("ClimaticStationDataFrame.readStationGIS: stationsGISPathDefinitions is no a valid string.")
        
        self.stationGISDF = self.readDFFromExcelFile(stationsGISPathDefinitions)
        
        return read


    def saveObservationsHourlyDF(self, typeOfSave:str="csv", fileSeparator:str=";")->bool:
        """
           It saves self.observationsDF to file ~path/<fileName>.<typeofSave>
           Returns true if file is correctly saved otherwise an exception is raised.
           Currently implemented formats:  
            * csv, it considers fileSeparator param
        """
        saved = True

               
        if (typeOfSave is None) or len(typeOfSave)==0:
            typeOfSave="csv"

        if (fileSeparator is None) or len(fileSeparator)==0:
            fileSeparator=";"

        if (typeOfSave=="csv"):
            fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "." + typeOfSave
            filePath= self.createOutputFileNamePath(fileName=fileName, typeOfSave=typeOfSave, fileSeparator=fileSeparator)
            self.saveDataFrameToCsv(df=self.observationsDF,filePath=filePath, fileSeparator=fileSeparator)
        return saved


    def saveObservationsDailyDF(self, typeOfSave:str="csv", fileSeparator:str=";")->bool:
        """
           It saves self.dailyObservationsDF to file ~path/<fileName>_daily.<typeofSave>
           Returns true if file is correctly saved otherwise an exception is raised.
           Currently implemented formats:  
            * csv, it considers fileSeparator param
        """
        saved = True

               
        if (typeOfSave is None) or len(typeOfSave)==0:
            typeOfSave="csv"

        if (fileSeparator is None) or len(fileSeparator)==0:
            fileSeparator=";"

        if (typeOfSave=="csv"):
            fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "_daily." + typeOfSave
            filePath= self.createOutputFileNamePath(fileName=fileName, typeOfSave=typeOfSave, fileSeparator=fileSeparator)
            self.saveDataFrameToCsv(df=self.dailyObservationsDF,filePath=filePath, fileSeparator=fileSeparator)
        return saved



    def saveObservationsDailyDFBySeasonAndStation(self, typeOfSave:str="csv", fileSeparator:str=";",
        seasonColumnName:str="season", stationColumnName:str="estacion",sortingColumnName:str="timeStamp")->bool:
        """
           It saves self.dailyObservationsDF to file ~path/<fileName>_daily.<typeofSave>
           Returns true if file is correctly saved otherwise an exception is raised.
           Currently implemented formats:  
            * csv, it considers fileSeparator param
        """
        saved = True

        if (typeOfSave is None) or len(typeOfSave)==0:
            typeOfSave="csv"

        if (fileSeparator is None) or len(fileSeparator)==0:
            fileSeparator=";"

        # if (season is None) or (len(season)==0):
        #     raise Exception("ClimaticStationDataFrame.determinet0:  season parameter has not value.")

        # if (station is None) or (len(station)==0):
        #     raise Exception("ClimaticStationDataFrame.determinet0:  station parameter has not value.")


        seasons = self.determineSeasonsToBeProcessed(self.observationsDF, seasonColumnName=seasonColumnName)
        stations = self.determineStationsToBeProcessed(self.observationsDF, stationColumnName="estacion")

        for currentSeason in seasons:
            for station in stations:
                if (typeOfSave=="csv"):
                    fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "_daily_"+ station + "_"+ currentSeason+ \
                        "_"+ typeOfSave
                    filePath= self.createOutputFileNamePath(fileName=fileName, typeOfSave=typeOfSave, fileSeparator=fileSeparator)
                    currentSeasonDF = self.observationsDF.loc[ ((self.observationsDF[seasonColumnName]==currentSeason) &   
                            (self.observationsDF[stationColumnName]==station))].sort_values(sortingColumnName, ascending=(True))
                    saved=self.saveDataFrameToCsv(df=currentSeasonDF,filePath=filePath, fileSeparator=fileSeparator)
        return saved

    def saveDFs(self, typeOfSave:str="csv", fileSeparator:str=";")->bool:
        """
           It saves self.dailyObservationsDF to file ~path/<fileName>_daily.<typeofSave>
           Returns true if file is correctly saved otherwise an exception is raised.
           Currently implemented formats:  
            * csv, it considers fileSeparator param
        """
        saved = True
        if (typeOfSave is None) or len(typeOfSave)==0:
            typeOfSave="csv"

        if (fileSeparator is None) or len(fileSeparator)==0:
            fileSeparator=";"

        self.saveObservationsHourlyDF(typeOfSave="csv", fileSeparator=";")
        self.saveObservationsDailyDF(typeOfSave="csv", fileSeparator=";")

        return saved
    
    def dms2dd(self, degrees, minutes, seconds, direction):
        dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
        if direction == 'S' or direction == 'W':
            dd *= -1
        return dd;
    
    def dd2dms(self,deg):
        d = int(deg)
        md = abs(deg - d) * 60
        m = int(md)
        sd = (md - m) * 60
        return [d, m, sd]

    def parseDms2Real(self, dms:str = None)->float:
        """
            Convert dms to its real representation
            Acepted format= dºm's.s" d m s.s
        """
        parts = re.split('[^\d\w]+', dms)
        real = self.dms2dd(parts[0], parts[1], parts[2], parts[3])
        return (real)

    def parseLatitudeDms2Real(self, dms:str = None)->float:
        """
            Convert dms to its real representation
            Acepted format= dºm's.s" d m s.s
        """
        if ("-" in dms):
            dms = dms.replace("-","")
            dms = dms + " W"
        else:
            dms = dms.replace("+","")
            dms = dms + " E"
        parts = re.split('[^\d\w]+', dms)
        real = self.dms2dd(parts[0], parts[1], parts[2]+"."+ parts[3], parts[4])
        return (real)

    def parseLongitudeDms2Real(self, dms:str = None)->float:
        """
            Convert dms to its real representation
            Acepted format= dºm's.s" d m s.s
        """
        if ("-" in dms):
            dms = dms.replace("-","")
            dms = dms + " S"
        else:
            dms = dms.replace("+","")
            dms = dms + " N"
        parts = re.split('[^\d\w]+', dms)
        real = self.dms2dd(parts[0], parts[1], parts[2]+"."+ parts[3], parts[4])
        return (real)

    def convertStatonGIStoDecimal(self)->bool:
        """
            Create new columns with the conversion of self.stationGISDF longitud and latitud columns to
            format nn.nnnnnn 
        """
        converted = True
        if (self.stationGISDF is None):
            raise Exception("ClimaticStationDataFrame.convertStatonGIStoDecimal: stationGISDF has not been read yet")
        self.stationGISDF["newLongitude"]=self.stationGISDF["Longitude"].apply(self.parseLongitudeDms2Real)
        self.stationGISDF["newLatitude"]=self.stationGISDF["Latitude"].apply(self.parseLatitudeDms2Real)
        return converted

    def readStationHourlyData(self)-> bool:
        """
            Read data from files in folder obtained from :
                self.parameter.diaryObservationsPathsAndPatterns[0]
            Files names has to start with the pattern contained by self.parameter.diaryObservationsPathsAndPatterns[1]
            It expects data from stations to be contained of first sheet.
        """
        read = True
        pathsAndPatterns = self.parameters.getParameter("diaryObservationsPathsAndPatterns")
        if not isinstance(pathsAndPatterns, str):
            raise Exception("ClimaticStationDataFrame.readStationDiaryData: diaryObservationsPathsAndPatterns is no a valid string.")
        if (pathsAndPatterns is None) or (len(pathsAndPatterns)==0):
            raise Exception("ClimaticStationDataFrame.readStationDiaryData: diaryObservationsPathsAndPatterns is no a valid string.")

        splitPathsAndPatterns=pathsAndPatterns.split(self.pathAndPatternsSeparator)
        if (len(splitPathsAndPatterns)!=2):
            raise Exception("ClimaticStationDataFrame.readStationDiaryData: diaryObservationsPathsAndPatterns paremeter should have the format <pathToFolder>;<Prefix>.")

        folderPath = splitPathsAndPatterns[0]
        prefix = splitPathsAndPatterns[1]
        allClimaticDataDataFrame = self.createEmptyClimaticStationDataFrame()
        if not(os.path.isdir(folderPath)):
            raise Exception("ClimaticStationDataFrame.readStationDiaryData: diaryObservationsPathsAndPatterns path paremeter does not point to an accesible folder:"+ folderPath)
        for root, dirs, files in os.walk(folderPath, topdown=False):
            frames = [ self.readDFFromExcelPrefixedFile(folderPath,filename, prefix) for filename in files ]
            frames = filter(None.__ne__, frames)
        allClimaticDataDataFrame = pd.concat(frames)
        self.observationsDF=allClimaticDataDataFrame
        return read

    def convertHoraMinToTime(self, row, columnName)->datetime:
        """
            row[columnName] is an interger representing horaMin as (hora*100)+min.  E.g, 1030 represents 10:30AM and 2015 representes 08:15PM
            it returns a time representation of time.
        """
        horaMinTime = None
        try:
            horaMin = int(row[columnName])
            hora = int(horaMin/100)
            minute = horaMin - (hora*100)
            if (hora==24):
                    hora = 0
            horaMinTime = datetime.time(hour=hora, minute=minute, second=0)
        except Exception as e:
            raise str(e)

        return horaMinTime

    def convertRowHoraMinToTime(self, originColumn:str="horaMin", destinationColumn:str="horaMinTime")->bool:
        converted = True

        if ((originColumn is None) or len(originColumn)==0):
            raise Exception("ClimaticStationDataFrame.convertHoraMinToTime: originColumn does not have any value")

        if (originColumn not in self.observationsDF.columns):
            raise Exception("ClimaticStationDataFrame.convertHoraMinToTime: originColumn is not a column of self.observationsDF")

        if ((destinationColumn is None) or len(destinationColumn)==0):
            raise Exception("ClimaticStationDataFrame.convertHoraMinToTime: horaMinTime does not have any value")

        try:
            self.observationsDF[destinationColumn]=self.observationsDF.apply (lambda row: self.convertHoraMinToTime(row, originColumn), axis=1)
        except Exception as e:
            raise str(e)

        return converted

    def createDateTime(self, row, dateColumnName:str="fecha", timeColumnName:str="horamin")->datetime:
        """
        """
        rowDateTime = None
        rowDate = row[dateColumnName]
        rowTime = int(row[timeColumnName])
        hora = int(rowTime/100)
        minute = rowTime - (hora*100)
        if (hora==24):
                hora = 0
        rowDateTime = datetime.datetime(year=rowDate.year, month=rowDate.month, day=rowDate.day,
             hour=hora, minute=minute, second=0)

        return rowDateTime


    def createRowDateTime(self, dateColumnName:str="fecha", timeColumnName:str="horamin", 
            dateTimeColumnName:str="timeStamp")->bool:
        created = True

        if ((dateColumnName is None) or len(dateColumnName)==0):
            dateColumnName="fecha"

        if ((timeColumnName is None) or len(timeColumnName)==0):
            timeColumnName="horaMinTime"

        if ((dateTimeColumnName is None) or len(dateTimeColumnName)==0):
            raise Exception("ClimaticStationDataFrame.createRowDateTime: dateTimeColumnName does not have any value")

        self.observationsDF[dateColumnName] = pd.to_datetime(self.observationsDF[dateColumnName])

        self.observationsDF[dateTimeColumnName]=self.observationsDF.apply (lambda row: self.createDateTime(row, dateColumnName=dateColumnName,
            timeColumnName=timeColumnName), axis=1)
        return created


    def setHourFraction(self, columnName="hourFraction", fractionValue:float=0.5)->bool:
        setFraction=True
        self.observationsDF[columnName]=fractionValue
        return setFraction
    
    def isOverWarmingThreshold(self, row, temperatureColumnName, hourFractionColumnName, 
            warmingThreshold:float)->int:
        isOver=1
        temperature=float(row[temperatureColumnName])
        hourFraction = row[hourFractionColumnName]
        if temperature >= warmingThreshold:
            isOver = hourFraction
        else:
            isOver = 0
        return isOver

    def isInWarmingThresholds(self, row, temperatureColumnName, hourFractionColumnName, 
            warmingThreshold:float, maxWarmingThreshold:float=35.0)->int:
        isOver=1
        temperature=float(row[temperatureColumnName])
        hourFraction = row[hourFractionColumnName]
        if (temperature >= warmingThreshold) and (temperature<=maxWarmingThreshold):
            isOver = hourFraction
        else:
            isOver = 0
        return isOver

    def setWarmingThresholdRows(self, columnNamePrefix:str="warmingFraction", 
            hourFractionColumnName:str="hourFrac", 
            temperatureColumnName:str="tmed", maxWarmingThreshold:float=35.0)->bool:
        setFraction=True
        for threshold in self.warmingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.isOverWarmingThreshold(row, temperatureColumnName,
               hourFractionColumnName=hourFractionColumnName,warmingThreshold=float(threshold)), axis=1)
            columnName=columnName+"_Max"
            self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.isInWarmingThresholds(row, temperatureColumnName,
               hourFractionColumnName=hourFractionColumnName,warmingThreshold=float(threshold),
               maxWarmingThreshold=maxWarmingThreshold), axis=1)
        return setFraction

    def isUnderChillingThreshold(self, row, temperatureColumnName, hourFractionColumnName, chillingThreshold:float)->int:
        isUnder=1
        temperature=float(row[temperatureColumnName])
        hourFraction = row[hourFractionColumnName]
        if temperature >= chillingThreshold:
            isUnder = 0
        else:
            isUnder = hourFraction
        return isUnder

    def isInChillingThresholds(self, row, temperatureColumnName, hourFractionColumnName, chillingThreshold:float,
        minChillingThreshold:float=0.0)->int:
        isUnder=1
        temperature=float(row[temperatureColumnName])
        hourFraction = row[hourFractionColumnName]
        if temperature >= chillingThreshold and temperature<minChillingThreshold:
            isUnder = 0
        else:
            isUnder = hourFraction
        return isUnder



    def setChillingThresholdRows(self, columnNamePrefix="chillingFraction", hourFractionColumnName="hourFrac",
            temperatureColumnName="tmed", minChillingThreshold:float=0.0)->bool:
        setFraction=True
        for threshold in self.chillingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.isUnderChillingThreshold(row, temperatureColumnName,
               hourFractionColumnName=hourFractionColumnName, chillingThreshold=float(threshold)), axis=1)
            columnName=columnNamePrefix+"_min"
            self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.isInChillingThresholds(row, 
                temperatureColumnName, hourFractionColumnName=hourFractionColumnName, 
                chillingThreshold=float(threshold), minChillingThreshold=minChillingThreshold), axis=1)
        
        return setFraction
    
    def setWinddirection(self, row, columnName):
        windDirection= row[columnName]
        textWinDirection = "N"
        fraction = 360/16
        if (15*fraction<= windDirection) and (windDirection+360<fraction+360):
            textWinDirection="N"
        if (fraction <= windDirection) and (windDirection<3*fraction):
            textWinDirection="NE"
        if (3*fraction <= windDirection) and (windDirection<5*fraction):
            textWinDirection="E"
        if (5*fraction <= windDirection) and (windDirection<7*fraction):
            textWinDirection="SE"
        if (7*fraction <= windDirection) and (windDirection<9*fraction):
            textWinDirection="S"
        if (9*fraction <= windDirection) and (windDirection<11*fraction):
            textWinDirection="SW"
        if (11*fraction <= windDirection) and (windDirection<13*fraction):
            textWinDirection="W"
        if (13*fraction <= windDirection) and (windDirection<15*fraction):
            textWinDirection="NW"

        return textWinDirection
    def setRowsWindDirection(self, columnName:str="windDirection", windDegreesColumnName="dv")->bool:
        set = True
        self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.setWinddirection(row, windDegreesColumnName), axis=1)
        return set


    def setReferenceDayOfWeek(self, row, dateColumnName:str="fecha", dayOfWeekToReturn:int=3):
        referenceDay=datetime.datetime.strptime("1-1-1900", "%d-%m-%Y")

        if (dayOfWeekToReturn<=0) and (dayOfWeekToReturn>=7):
            raise Exception("ClimaticStationDataFrame.setReferenceDayOfWeek: dayOfWeekToReturn parameter must contain an integer value in [0,6]. It has:"+ str(dayOfWeekToReturn))
        date = row[dateColumnName]
        if type(date) == str:
            date = datetime.datetime.strptime("1-1-1900", "%d-%m-%Y")
        dayOfWeek = date.weekday()
        referenceDay = date+datetime.timedelta(dayOfWeekToReturn-dayOfWeek)
        return referenceDay
    def setRowsReferenceDayOfWeek(self, columnName:str="weekReference", dateColumnName:str="fecha",
            dayOfWeekToReturn:int=3)->bool:
        set = True
        self.observationsDF[columnName]=self.observationsDF.apply (lambda row: self.setReferenceDayOfWeek(row, 
            dateColumnName=dateColumnName, dayOfWeekToReturn=dayOfWeekToReturn), axis=1)
        return set


    def preprocessHourlyColumns(self, force:bool=False, typeOfSave="csv", fileSeparator=";")->bool:
        """
                Creates all the derived variables from the read variable from file.  Performence tasks are:
                * convertRowHoraMinToTime
                * setHourFraction
                * setWarmingThresholdRows
                * setChillingThresholdRows
                * 
                While force param is False it verifies if it exists a file with path:
                    parameters['diaryObservationsPathsAndPatterns']/parameters['hourlyPreprocessesFileName']
                If is exists, the fille is read into self.observationsDF,
                If not, self.observationsDF is read from original files.
                Any error is raised as an Exception.

        """
        preprocessed=True

        if (force==False):
            fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "."+ typeOfSave
            filePath=self.createOutputFileNamePath(fileName=fileName,typeOfSave=typeOfSave,
                fileSeparator=fileSeparator)
            if (os.path.isfile(filePath)):
                self.observationsDF = self.readDFFromCSVFile(filePath=filePath, separator=fileSeparator)
            else:
                force=True
        if (force==True):
            self.readStationHourlyData()
            self.convertRowHoraMinToTime(originColumn="horamin", destinationColumn="horaminTime")
            self.createRowDateTime( dateColumnName="fecha", timeColumnName="horamin", dateTimeColumnName="timeStamp")

            self.setRowsReferenceDayOfWeek(columnName="weekReference", dateColumnName="fecha", dayOfWeekToReturn=3)

            self.setHourFraction(columnName="hourFrac", fractionValue=0.5)


            self.setWarmingThresholdRows(columnNamePrefix="warmingFraction", hourFractionColumnName="hourFrac",
                temperatureColumnName="tmed", maxWarmingThreshold= self.maxWarmingThresHold)
            self.setChillingThresholdRows(columnNamePrefix="chillingFraction", hourFractionColumnName="hourFrac", 
                temperatureColumnName="tmed", minChillingThreshold=self.minChillingThreshold)

            self.setRowsWindDirection(columnName="windDirection",windDegreesColumnName="dv")

            self.setDefaultHourlyDataSeason(columnName="season", defaultSeason="1900_1901")
            self.setDefaultHourlyWarmingKPI(columnNamePrefix="gdd")
            self.setDefaultHourlyChillingKPI(columnNamePrefix="chillingDD")

            self.saveObservationsHourlyDF(typeOfSave=typeOfSave, fileSeparator=fileSeparator)

        return preprocessed
    

    def getMinMaxMeanHourlyValuesDataFrame(self, df=None):
        minDf = None
        newDF = df.copy()
        newDF['fecha'] = pd.to_datetime(newDF["fecha"]).dt.date
        minDF = newDF.groupby(['estacion', 'ubi', 'anio','dia','fecha'], as_index=False).agg({
              'tmed':['min', 'max','mean'],
              'rad':['min', 'max','mean'],
              'hr':['mean'],
              #'precip':['sum'],
              'hourFrac':['sum']
            })
        minDF['fecha'] = (minDF["fecha"]).astype('datetime64[ns]')

        return minDF

    def setDefaultHourlyDataSeason(self, columnName="season", defaultSeason:str="1900_1901")->bool:
        
        setSeason=True

        self.observationsDF[columnName]= defaultSeason

        return setSeason


    def setDefaultHourlyWarmingKPI(self, columnNamePrefix:str="gdd")->bool:
        setDefault=True
        for threshold in self.warmingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            for timeZero in self.timeZerosArray:
                columnName2=columnName+"_"+str(timeZero)
                for hourlyMethod in self.warmingHourlyMethodsArray:
                    columnName3=columnName2+"_"+str(hourlyMethod)
                    self.observationsDF[columnName3]=0
        
        return setDefault


    def setDefaultHourlyChillingKPI(self, columnNamePrefix:str="gdd")->bool:
        setDefault=True
        for threshold in self.chillingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            for timeZero in self.timeZerosArray:
                columnName2=columnName+"_"+str(timeZero)
                for hourlyMethod in self.chillingHourlyMethodsArray:
                    columnName3=columnName2+"_"+str(hourlyMethod)
                    self.observationsDF[columnName3]=0
        
        return setDefault



    def isPotentialDormancyDays(self, row, yearColumnIndex:int=2, dateColumnIndex:int=4, 
            maxTempColumnIndex:int=6,  dormancyThreshold:float=10.0, dayFrom:str="0830", dayTo:str="0131")->int:
        isPontential=0
        temperature=float(row[maxTempColumnIndex])
        year = row[yearColumnIndex]

        # rowDateStr= row[dateColumnIndex]
        # rowDate=datetime.datetime.strptime(rowDateStr, "%Y-%m-%d")
        rowDate= row[dateColumnIndex]
        if (type(rowDate) == str):
            rowDate=datetime.datetime.strptime(rowDate, "%Y-%m-%d")

        fromDateStr=str(year)+dayFrom
        fromDate=datetime.datetime.strptime(fromDateStr, "%Y%m%d")

        toDateStr = str(year+1)+dayTo
        toDate=datetime.datetime.strptime(toDateStr, "%Y%m%d")

        if (fromDate <= rowDate) and (rowDate<=toDate):
            if temperature <= dormancyThreshold:
                isPontential = 1
            else:
                isPontential = 0
        else:
            isPontential=0
        return isPontential


    def setPotentialDormancyDays(self, columnName="potentialDormancyDay", yearColumnIndex:int=2, dateColumnIndex:int=4, 
            maxTempColumnIndex:int=6, dormancyThreshold:float=10.0, dayFrom:str="0830", dayTo:str="0131")->bool:
        setDormancy=True
        self.dailyObservationsDF[columnName]=self.dailyObservationsDF.apply (lambda row: 
            self.isPotentialDormancyDays(row, yearColumnIndex=yearColumnIndex, 
             dateColumnIndex=dateColumnIndex, maxTempColumnIndex=maxTempColumnIndex, 
             dormancyThreshold=dormancyThreshold, dayFrom=dayFrom, dayTo=dayTo), axis=1)
        
        return setDormancy

    def setDefaultDailyDataSeason(self, columnName="season", defaultSeason="1900_1901")->bool:
        
        setSeason=True

        self.dailyObservationsDF[columnName]= defaultSeason 

        return setSeason

    def setDefaultDailyDegrees(self, columnName="season", defaultSeason="1900_1901")->bool:
        
        setSeason=True

        self.dailyObservationsDF[columnName]= defaultSeason 

        return setSeason



    def sett0Row(self )->int:
        isPontential=0

        return isPontential


    def sett0AndSeason(self,  seasonColumnName="season", defaultSeason:str="1900_1901")->bool:
        """
                Set the t0 of each season and the season field on dataframes:
                  * self.hourlyObservationsDF
                  * self.dailyObservationsDF
                Returns True if everything goes right an exception is raised if not
        """
        sett0=True
        potentialDormancyDayDF = self.dailyObservationsDF.loc[self.dailyObservationsDF["potentialDormancyDay"]==1]
        mint0DateByStation = potentialDormancyDayDF.groupby(['estacion', 'anio'], as_index=False).agg({
              'fecha':['min'] })
        # Free Temporal Memory
        del potentialDormancyDayDF
        columnsName = ('estacion', 'anio','fecha_min')
        mint0DateByStation.columns =columnsName
        mint0DateByStation.sort_values(["estacion", "anio"], ascending = (True, True), inplace=True)
        previousStation="oldStation"
        previousYear=1900
        numberofRecords = len(mint0DateByStation)
        # if (len(mint0DateByStation)>1):
        #     previousStation="oldStation"
        #     previousYear=1900
        # else:
        #     previousStation = mint0DateByStation.loc[0, 'estacion']
        #     previousYear=mint0DateByStation.loc[0, 'anio']-1
        dateFrom = datetime.datetime.strptime("1-1-1900", "%d-%m-%Y")
        for index,row in mint0DateByStation.iterrows():
            station=row['estacion']
            year=row['anio']
            dateTo = row['fecha_min']
            if numberofRecords == 1:
                previousStation = station
                previousYear=year
                year = year+1
                dateFrom = dateTo
                dateTo = datetime.datetime(year+1, 1, 1)
            if previousStation==station:
                if (year-previousYear)!= 1:
                    raise Exception("ClimaticStationDataFrame.sett0AndSeason: there is a data gap for station:"+ station + 
                     " between year:"+ str(previousYear)+" and year:"+ str(year)+ "please review data.")
                season=str(previousYear)+"_"+str(year)
                mask = (self.dailyObservationsDF['estacion']==station) & \
                    (self.dailyObservationsDF['fecha'] >= dateFrom) & \
                    (self.dailyObservationsDF['fecha'] < dateTo)
                self.dailyObservationsDF[seasonColumnName][mask] = season
                mask = (self.observationsDF['estacion']==station) &\
                    (self.observationsDF['fecha'] >= dateFrom) & \
                    (self.observationsDF['fecha'] < dateTo)
                self.observationsDF[seasonColumnName][mask] = season
                previousStation=station
                previousYear=year
                dateFrom = dateTo
            else:
                #Change of Station
                previousStation=station
                previousYear=year
                dateFrom = dateTo
        # Free Temporal Memory
        del mint0DateByStation

        defaultSeasonRows= self.dailyObservationsDF.loc[self.dailyObservationsDF[seasonColumnName]==defaultSeason]

        # Update first season rows if any
        estacionAnioToBeUpdated = defaultSeasonRows.groupby(['estacion'], as_index=False).agg({
              'fecha':['min'] })
        columnsName = ('estacion', 'dateFrom')
        estacionAnioToBeUpdated.columns =columnsName
        estacionAnioToBeUpdated.sort_values(["estacion", "dateFrom"], ascending = (True, True), inplace=True)
        for index, row in estacionAnioToBeUpdated.iterrows():
            station = row ['estacion']
            rowDateFrom = row['dateFrom']
            if (type(rowDateFrom) == str):
                dateFrom = datetime.datetime.strptime(row['dateFrom'],"%Y-%m-%d")
            else: 
                dateFrom = rowDateFrom
            year = dateFrom.year
            season = str(year-1)+ "_"+ str(year)
            mask = (self.dailyObservationsDF['estacion']==station) &  \
                (self.dailyObservationsDF[seasonColumnName]==defaultSeason) &\
                (self.dailyObservationsDF['anio']==year) 
            self.dailyObservationsDF[seasonColumnName][mask] = season
            mask = (self.observationsDF['estacion']==station) &  \
                (self.observationsDF[seasonColumnName]==defaultSeason) &\
                (self.observationsDF['anio']==year) 
            self.observationsDF[seasonColumnName][mask] = season

        # Update first season rows if any
        estacionAnioToBeUpdated = defaultSeasonRows.groupby(['estacion'], as_index=False).agg({
              'fecha':['max'] })
        columnsName = ('estacion', 'dateTo')
        estacionAnioToBeUpdated.columns =columnsName
        estacionAnioToBeUpdated.sort_values(["estacion", "dateTo"], ascending = (True, True), inplace=True)
        for index, row in estacionAnioToBeUpdated.iterrows():
            station = row ['estacion']
            rowDateTo = row['dateTo']
            if (type(rowDateTo) == str):
                dateTo = datetime.datetime.strptime(rowDateTo,"%Y-%m-%d")
            else: 
                if (type(rowDateTo) is pd.Timestamp):
                    dateTo = rowDateTo.to_pydatetime()
                else:
                    dateTo = rowDateFrom
            year = dateTo.year
            previousYear=year-1
            season = str(previousYear)+ "_"+ str(year)
            mask = (self.dailyObservationsDF['estacion']==station) &  \
                (self.dailyObservationsDF[seasonColumnName]==defaultSeason) 
            self.dailyObservationsDF[seasonColumnName][mask] = season
            mask = (self.observationsDF['estacion']==station) &  \
                (self.observationsDF[seasonColumnName]==defaultSeason) 
            self.observationsDF[seasonColumnName][mask] = season


        return sett0



    def preprocessDailyColumns(self, force:bool=False, typeOfSave="csv", fileSeparator=";")->bool:
        preprocessed=True

        if (force==False):
            fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "_daily."+ typeOfSave
            filePath=self.createOutputFileNamePath(fileName=fileName,typeOfSave=typeOfSave,
                fileSeparator=fileSeparator)
            if (os.path.isfile(filePath)):
                self.dailyObservationsDF = self.readDFFromCSVFile(filePath=filePath, separator=fileSeparator)
            else:
                force=True
        if (force==True):
            self.dailyObservationsDF = self.getMinMaxMeanHourlyValuesDataFrame(self.observationsDF)
            self.setPotentialDormancyDays(columnName="potentialDormancyDay", yearColumnIndex=2, 
                dateColumnIndex=4,  maxTempColumnIndex=6, dormancyThreshold=10.0, dayFrom="0830", dayTo="0131")
            self.dailyObservationsDF=self.renameDataFrameColumns(self.dailyObservationsDF)
            self.setDefaultDailyDataSeason(columnName="season", defaultSeason="1900_1901")
            self.sett0AndSeason(seasonColumnName="season")
            # self.saveObservationsDailyDF(typeOfSave=typeOfSave, fileSeparator=fileSeparator)
        return preprocessed
    

    def calulateHourlyTemperatureKPItbase(self, row, temperatureColumnName:str="tmed",
                                fractionColumnName:str="warmingFraction",
                                tbase:str=10.0,
                                destinationColumn:str="gdd")->float:
        kpi=0.0

        temperature = row [temperatureColumnName]
        degreesHour = temperature - tbase
        fractionColumnName=row [fractionColumnName]
        kpi = degreesHour*fractionColumnName

        return kpi


    def calulateHourlyTemperatureKPItbaseMax(self, row, temperatureColumnName:str="tmed",
                                fractionColumnName:str="warmingFraction",
                                tbase:str=10.0,
                                tbaseMax:str=35.0,
                                destinationColumn:str="gdd")->float:
        kpi=0.0

        temperature = row [temperatureColumnName]
        degreesHour = temperature - tbase

        if (temperature<tbaseMax):
            fractionColumnName=row [fractionColumnName]
            kpi = degreesHour*fractionColumnName
        else:
            kpi = 0.0

        return kpi


    def calculateHourlyTemperatureWarmingKPIs(self, temperatureColumnName:str = "tmed", 
                    warmingFractionPrefix:str="warmingFraction",
                    destinationColumnNamePrefix:str="gdd",
                    maxWarmingThreshold:float=35.0)->bool:
        calculated=True
        for threshold in self.warmingThresHoldArray:
            fractionColumnNamePrefixThreshold=warmingFractionPrefix+"_"+str(threshold)
            for timeZero in self.timeZerosArray:
                for hourlyMethod in self.warmingHourlyMethodsArray:
                    destinationColumn=destinationColumnNamePrefix+"_"+ str(threshold)+\
                            "_"+ timeZero +"_"+hourlyMethod
                    if hourlyMethod == "Tbase":
                        self.observationsDF[destinationColumn]=\
                                self.observationsDF.apply(lambda row: self.calulateHourlyTemperatureKPItbase(row, \
                                    temperatureColumnName=temperatureColumnName,\
                                    fractionColumnName=fractionColumnNamePrefixThreshold,\
                                    tbase=float(threshold),\
                                    destinationColumn=destinationColumn), axis=1)
                    if hourlyMethod == "TbaseMax":
                        self.observationsDF[destinationColumn]=\
                                self.observationsDF.apply(lambda row: self.calulateHourlyTemperatureKPItbaseMax(row, \
                                    temperatureColumnName=temperatureColumnName,\
                                    fractionColumnName=fractionColumnNamePrefixThreshold,\
                                    tbase=float(threshold),\
                                    tbaseMax=maxWarmingThreshold,\
                                    destinationColumn=destinationColumn), axis=1)

        return calculated


    def calulateHourlyTemperatureChillingKPItbase(self, row, temperatureColumnName:str="tmed",
                                fractionColumnName:str="warmingFraction",
                                tbase:str=7.0,
                                destinationColumn:str="gdd")->float:
        kpi=0.0

        temperature = row [temperatureColumnName]
        degreesHour = tbase - temperature
        fractionColumnName=row [fractionColumnName]
        kpi = degreesHour*fractionColumnName

        return kpi


    def calulateHourlyChillingKPItbasemin(self, row, temperatureColumnName:str="tmed",
                                fractionColumnName:str="warmingFraction",
                                tbase:str=7.0,
                                tbaseMin:str=0.0,
                                destinationColumn:str="gdd")->float:
        kpi=0.0

        temperature = row [temperatureColumnName]
        degreesHour = tbase - temperature

        if (tbaseMin<temperature):
            fractionColumnName=row [fractionColumnName]
            kpi = degreesHour*fractionColumnName
        else:
            kpi = 0.0

        return kpi


    def convertFahrenheitToCelsius(self, fahrenheit:float=0.0)->float:
        celsious = 0.0
        
        celsious = (fahrenheit - 32) * 5/9
        
        return celsious

    def calulateHourlyChillingKPItUtah(self, row, temperatureColumnName:str="tmed",
                                fractionColumnName:str="warmingFraction",
                                destinationColumn:str="gdd")->float:
        kpi=0.0

        temperature = row [temperatureColumnName]
        fractionColumnName=row [fractionColumnName]

        # 1 hour below 34 ˚F = 0.0 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(34)
        if ( temperature <= minReferenceTemperature):
            kpi = 0.0
        # 1 hour 34.01 – 36 ˚F = 0.5 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(34.01)
        MaxReferenceTemperature = self.convertFahrenheitToCelsius(36)
        if (minReferenceTemperature < temperature) and \
               (temperature <= MaxReferenceTemperature):
            kpi = 0.5 * fractionColumnName
        # 1 hour 36.01 – 48 ˚F = 1.0 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(36.01)
        MaxReferenceTemperature = self.convertFahrenheitToCelsius(48)
        if (minReferenceTemperature < temperature) and \
               (temperature <= MaxReferenceTemperature):
            kpi = 1.0 * fractionColumnName
        # 1 hour 48.01 – 54 ˚F = 0.5 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(48.01)
        MaxReferenceTemperature = self.convertFahrenheitToCelsius(54)
        if (minReferenceTemperature < temperature) and \
               (temperature <= MaxReferenceTemperature):
            kpi = 1.0 * fractionColumnName
        # 1 hour 54.01 – 60 ˚F = 0.0 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(54.01)
        MaxReferenceTemperature = self.convertFahrenheitToCelsius(60)
        if (minReferenceTemperature < temperature) and \
               (temperature <= MaxReferenceTemperature):
            kpi = 0.0 * fractionColumnName
        # 1 hour 60.01 – 65 ˚F = -0.5 chill unit 
        minReferenceTemperature = self.convertFahrenheitToCelsius(60.01)
        MaxReferenceTemperature = self.convertFahrenheitToCelsius(65)
        if (minReferenceTemperature < temperature) and \
               (temperature <= MaxReferenceTemperature):
            kpi = -0.5 * fractionColumnName
        # 1 hour > 65.01 ˚F = -1.0 chill unit
        if ( MaxReferenceTemperature < temperature ):
            kpi = -1.0 * fractionColumnName

        return kpi


    def calculateHourlyTemperatureChillingKPIs(self, temperatureColumnName:str = "tmed", 
                    chillingFractionPrefix:str="chillingFraction",
                    destinationColumnNamePrefix:str="chillingDD",
                    minChillingThreshold:float=0.0)->bool:
        calculated=True
        for threshold in self.chillingThresHoldArray:
            fractionColumnNamePrefixThreshold=chillingFractionPrefix+"_"+str(threshold)
            for timeZero in self.timeZerosArray:
                for hourlyMethod in self.chillingHourlyMethodsArray:
                    destinationColumn=destinationColumnNamePrefix+"_"+ str(threshold)+\
                            "_"+ timeZero +"_"+hourlyMethod
                    if hourlyMethod == "Tbase":
                        self.observationsDF[destinationColumn]=\
                                self.observationsDF.apply(lambda row: self.calulateHourlyTemperatureChillingKPItbase(row, \
                                    temperatureColumnName=temperatureColumnName,\
                                    fractionColumnName=fractionColumnNamePrefixThreshold,\
                                    tbase=float(threshold),\
                                    destinationColumn=destinationColumn), axis=1)
                    if hourlyMethod == "Tbasemin":
                        self.observationsDF[destinationColumn]=\
                                self.observationsDF.apply(lambda row: self.calulateHourlyChillingKPItbasemin(row, \
                                    temperatureColumnName=temperatureColumnName,\
                                    fractionColumnName=fractionColumnNamePrefixThreshold,\
                                    tbase=float(threshold),\
                                    tbaseMin=minChillingThreshold,\
                                    destinationColumn=destinationColumn), axis=1)

                    if hourlyMethod == "Utah":
                        self.observationsDF[destinationColumn]=\
                                self.observationsDF.apply(lambda row: self.calulateHourlyChillingKPItUtah(row, \
                                    temperatureColumnName=temperatureColumnName,\
                                    fractionColumnName=fractionColumnNamePrefixThreshold,\
                                    destinationColumn=destinationColumn), axis=1)

                                    

        return calculated
    
    def calculateHourlyTemperatureKPIs(self, force:bool=False)-> bool:
        """
            Calculate the hour fraction contributions to Warming and Chilling Degrees Day on 
            self.observationsDF.
            Returns True if it correctly finishes or and exception is raised in other circunstances.
        """

        calculated = True 

        if (force is None):
            force=True

        if force:
            self.calculateHourlyTemperatureWarmingKPIs(temperatureColumnName = "tmed", 
                warmingFractionPrefix="warmingFraction",
                destinationColumnNamePrefix="gdd",
                maxWarmingThreshold=self.maxWarmingThresHold)

            self.calculateHourlyTemperatureChillingKPIs(temperatureColumnName = "tmed", 
                chillingFractionPrefix="chillingFraction",
                destinationColumnNamePrefix="chillingDD",
                minChillingThreshold=self.minChillingThreshold)
        
        return calculated


    def determineSeasonsToBeProcessed(self, df, seasonColumnName:str="season"):
        seasons=[]
        
        if (df is None ):
            raise Exception("ClimaticStationDataFrame.determineSeasonsToBeProcessed:  df parameter cannot be None")

        if (seasonColumnName is None) or len(seasonColumnName)==0:
            seasonColumnName="season"
        
        seasons = df[seasonColumnName].unique()
        return seasons


    def determineStationsToBeProcessed(self, df, stationColumnName:str="estacion"):
        stations=[]
        
        if (df is None ):
            raise Exception("ClimaticStationDataFrame.determineStationsToBeProcessed:  df parameter cannot be None")

        if (stationColumnName is None) or len(stationColumnName)==0:
            stationColumnName="estacion"
        
        stations = df[stationColumnName].unique()
        return stations


    def determinet0 (self, timeZero:str="t0", season:str=None, station:str=None):
        """
             It returns the "initial date" for calculating different gradients for the given hourlyMethod:
                * For t0 it is the 1/1/<season[year1]>
                * For 1 it is the 1/1/<season[year2]>
                * For 2 it it the 1/2/<season[year2]>
             If something goes wrong an Exception is raised.
        """
        t0Date= datetime.datetime.today()

        if (timeZero is None) or (len(timeZero)==0):
            timeZero="t0"

        if (timeZero not in self.timeZerosArray):
            raise Exception("ClimaticStationDataFrame.determinet0:  timeZero parameter has not a valid value: "+  timeZero)

        if (season is None) or (len(season)==0):
            raise Exception("ClimaticStationDataFrame.determinet0:  season parameter has not value.")

        if (station is None) or (len(station)==0):
            raise Exception("ClimaticStationDataFrame.determinet0:  station parameter has not value.")


        seasonYears= season.split("_")
        if (len(seasonYears)!=2):
            raise Exception("ClimaticStationDataFrame.determinet0:  season parameter has not a valid value: "+ season)

        if timeZero=="t0":
            # It seems it is not needed to determine the minor date lets see
            year=int(seasonYears[0])
            month=1
        else:
            year=int(seasonYears[1])
            month=int(timeZero)
        
        t0Date= datetime.datetime(year, month, 1)
        return t0Date

    def createHourlyCumulativeColumns(self, destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"], cumSum:float=0.0):
        initialized = True
        
        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]

        if (cumSum is None) :
            cumSum = 0.0

        for destinatonPrefix in destinationColumnNamePrefix:
            if destinatonPrefix=="gdd":
                thresholdsList=self.warmingThresHoldArray
                methods= self.warmingHourlyMethodsArray
            else:
                if destinatonPrefix=="chillingDD":
                    thresholdsList=self.chillingThresHoldArray
                    methods= self.chillingHourlyMethodsArray
                else:
                    thresholdsList=[""]
                    methods=[""]
            for threshold in thresholdsList:
                for timeZero in self.timeZerosArray:
                    for hourlyMethod in methods:
                        originColumn=destinatonPrefix+"_"+ str(threshold)+\
                                "_"+ timeZero +"_"+hourlyMethod
                        destinationColumn=originColumn+"_Cumm"   
                        self.observationsDF[destinationColumn]=cumSum        
        return initialized

    def initalizeHourlyCumulativeColumns(self, destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"], cumSum:float=0.0,
        seasons:list=None, stations:list=None):

        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]

        if (cumSum is None) :
            cumSum = 0.0

        seasonStationOriginDestinationcolumnZero={}
        for season in seasons:
            stationOriginDestinationcolumnZero={}
            for station in stations:
                cumSum=0.0
                originAndDestinationColumsZero = {}
                for destinatonPrefix in destinationColumnNamePrefix:
                    if destinatonPrefix=="gdd":
                        thresholdsList=self.warmingThresHoldArray
                        methods= self.warmingHourlyMethodsArray
                    else:
                        if destinatonPrefix=="chillingDD":
                            thresholdsList=self.chillingThresHoldArray
                            methods= self.chillingHourlyMethodsArray
                        else:
                            thresholdsList=[""]
                            methods=[""]
                    for threshold in thresholdsList:
                        for timeZero in self.timeZerosArray:
                            for hourlyMethod in methods:
                                datet0=self.determinet0(timeZero=timeZero,season=season, station=station)
                                originColumn=destinatonPrefix+"_"+ str(threshold)+\
                                        "_"+ timeZero +"_"+hourlyMethod
                                destinationColumn=originColumn+"_Cumm"   
                                self.observationsDF[destinationColumn]=cumSum
                                originAndDestinationColumsZero[originColumn]=[destinationColumn, cumSum, datet0]
                stationOriginDestinationcolumnZero[station]=originAndDestinationColumsZero
            seasonStationOriginDestinationcolumnZero[season]=stationOriginDestinationcolumnZero
        
        return seasonStationOriginDestinationcolumnZero



    def calculateHourlyCumulativeColumns(self, force:bool=False, seasonColumnName:str="season", 
            destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"], dateColumnName:str="fecha",
            sortingColumnName:str="timeStamp", stationColumnName:str="estacion", cumSum:float=0.0)->bool:
        """
            It calculates all the cummulated colums for hourly DF (self.observatonDF)
            Parameters are the name of the columns to be considered.
                1.- Initialize all the columns to zero.
                2.- Determine all the possible cummulative values for all the origin and destination columns
                3.- Go through DF for calculating cummulative value.
        """
        calculated = True

        if (seasonColumnName is None) or len(seasonColumnName)==0:
            seasonColumnName="season"

        if (cumSum is None) :
            cumSum = 0.0

        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]
        
        if (force):

            #Making sure dates columns are datetimes
            self.observationsDF[dateColumnName] = pd.to_datetime(self.observationsDF[dateColumnName])
            self.observationsDF[sortingColumnName] = pd.to_datetime(self.observationsDF[sortingColumnName])
            
            self.createHourlyCumulativeColumns(destinationColumnNamePrefix=destinationColumnNamePrefix, cumSum=cumSum)

            # Initialize cummulative values
            seasons = self.determineSeasonsToBeProcessed(self.observationsDF, seasonColumnName=seasonColumnName)
            stations = self.determineStationsToBeProcessed(self.observationsDF, stationColumnName=stationColumnName)

            seasonStationOriginDestinationcolumnZero = self.initalizeHourlyCumulativeColumns(destinationColumnNamePrefix=destinationColumnNamePrefix, 
                cumSum=cumSum, seasons=seasons, stations=stations)

            dataframes=[]
            for currentSeason in seasons:
                for station in stations:
                    currentSeasonDF = self.observationsDF.loc[ \
                        ((self.observationsDF[seasonColumnName]==currentSeason) & \
                        (self.observationsDF[stationColumnName]==station) ) \
                                ].sort_values(sortingColumnName, ascending=(True))
                    cseasonStationDestinationcolumns = copy.deepcopy(seasonStationOriginDestinationcolumnZero[currentSeason][station])
                    for index, row in currentSeasonDF.iterrows():
                        for originColumn in cseasonStationDestinationcolumns.keys():
                            destinationColumAndValue=cseasonStationDestinationcolumns[originColumn]
                            observationDateTime= row[sortingColumnName]
                            if "__" in originColumn:
                                splitOriginColumn=originColumn.split("_")
                                originColumn2=splitOriginColumn[0]
                            else:
                                originColumn2=originColumn
                            ddContribution=row[originColumn2]
                            destinationColumn= destinationColumAndValue[0]
                            cumSum = destinationColumAndValue[1]
                            t0 = destinationColumAndValue[2]
                            if t0<= observationDateTime:
                                cumSum+=ddContribution
                                cseasonStationDestinationcolumns[originColumn]=[destinationColumn,cumSum,t0]
                                currentSeasonDF.loc[ \
                                        (currentSeasonDF[sortingColumnName]==observationDateTime), \
                                            destinationColumn]=cumSum
                    dataframes.append(currentSeasonDF)

            self.observationsDF= pd.concat(dataframes)
        return calculated



    def getMinMaxMeanSumDailyValuesDataFrame(self, df=None, columnNames:list=None):
        minDf = None
        aggregations={}
        for key in columnNames:
            #aggregations[key]=['min', 'max','mean','sum']
            aggregations[key]=['sum']
        aggregationDF = df.groupby(['estacion', 'ubi', 'anio','dia','fecha'], as_index=False).agg(aggregations)
        return aggregationDF



    def initalizeDailySumColumns(self, destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"], cumSum:float=0.0,
        seasons:list=None, stations:list=None):

        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]

        if (cumSum is None) :
            cumSum = 0.0

        seasonStationOriginDestinationcolumnZero={}
        for season in seasons:
            stationOriginDestinationcolumnZero={}
            for station in stations:
                cumSum=0.0
                originAndDestinationColumsZero = {}
                for destinatonPrefix in destinationColumnNamePrefix:
                    if destinatonPrefix=="gdd":
                        thresholdsList=self.warmingThresHoldArray
                        methods= self.warmingHourlyMethodsArray
                    else:
                        if destinatonPrefix=="chillingDD":
                            thresholdsList=self.chillingThresHoldArray
                            methods= self.chillingHourlyMethodsArray
                        else:
                            thresholdsList=[""]
                            methods=[""]
                    for threshold in thresholdsList:
                        for timeZero in self.timeZerosArray:
                            for hourlyMethod in methods:
                                datet0=self.determinet0(timeZero=timeZero,season=season, station=station)
                                originColumn=destinatonPrefix+"_"+ str(threshold)+\
                                        "_"+ timeZero +"_"+hourlyMethod
                                destinationColumn=originColumn+"_Cumm"   
                                #self.dailyObservationsDF[destinationColumn]=cumSum
                                originAndDestinationColumsZero[originColumn]=[destinationColumn, cumSum, datet0]
                stationOriginDestinationcolumnZero[station]=originAndDestinationColumsZero
            seasonStationOriginDestinationcolumnZero[season]=stationOriginDestinationcolumnZero
        
        return seasonStationOriginDestinationcolumnZero




    def createDailyWarmingChillingColumnNames(self,  warmingFractionPrefix:str="warmingFraction",
                    destinationColumnNamePrefix:list=["gdd","chilling"],
                    seasonColumnName:str="season", stationColumnName:str="estacion", cumSum:float=0.0):
        columnNames=[]

        seasons = self.determineSeasonsToBeProcessed(self.observationsDF, seasonColumnName=seasonColumnName)
        stations = self.determineStationsToBeProcessed(self.observationsDF, stationColumnName=stationColumnName)

        cumulativeColumns = self.initalizeDailySumColumns(destinationColumnNamePrefix=destinationColumnNamePrefix, cumSum=0.0,
        seasons=seasons, stations=stations)

        for season  in cumulativeColumns.keys():
            seasonData = cumulativeColumns[season]
            for station in seasonData.keys():
                seasonStationData = seasonData[station]
                for key in seasonStationData.keys():
                    if key.startswith("rad"):
                        key="rad"
                        if not (key in columnNames):
                            columnNames.append(key)
                    else:
                        if key.startswith("precip"):
                            key="precip"
                            if not (key in columnNames):
                                columnNames.append(key)
                        else:
                            columnNames.append(key)
                break
            break
        return columnNames


    def setDefaultDailyWinklerContributionKPI(self, columnNamePrefix:str="winkler")->bool:
        setDefault=True
        for threshold in self.warmingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            for hourlyMethod in self.warmingHourlyMethodsArray:
                columnName2=columnName+"_"+str(hourlyMethod)
                self.dailyObservationsDF[columnName2]=0
        
        return setDefault

    def calulateDailyWinklerKPItbase(self, row, temperatureColumnsName:str=["tmed_min", "tmed_max"],
                                tbase:float=10.0)->float:
        winkler=0.0

        minT = row [temperatureColumnsName[0]]
        maxT = row [temperatureColumnsName[1]]
        avgT = (maxT+minT)/2
        winkler = avgT-tbase
        if (winkler< 0.0):
            winkler=0.0

        return winkler


    def calulateDailyWinklerKPItbaseMax(self, row, temperatureColumnsName:str=["tmed_min", "tmed_max"],
                                tbase:float=10.0, tmax:float=35.0)->float:
        winkler=0.0

        minT = row [temperatureColumnsName[0]]
        maxT = row [temperatureColumnsName[1]]
        if maxT> tmax:
            maxT = tmax
        avgT = (maxT+minT)/2
        winkler = avgT-tbase
        if (winkler< 0.0):
            winkler=0.0
        

        return winkler


    def calculateDailyWinklerTemperatureKPIs(self, temperatureColumnsName:str = ["tmed_min", "tmed_max"], 
                    destinationColumnNamePrefix:str="winkler",
                    maxWarmingThreshold:float=35.0)->bool:
        """
            Calculate the daily Winkler's index contribution.
            Returns True if it correctly finishes or and exception is raised in other circunstances.
        """
        calculated=True
        for threshold in self.warmingThresHoldArray:
            for hourlyMethod in self.warmingHourlyMethodsArray:
                destinationColumn=destinationColumnNamePrefix+"_"+ str(threshold)+"_"+hourlyMethod
                if hourlyMethod == "Tbase":
                    self.dailyObservationsDF[destinationColumn]=\
                            self.dailyObservationsDF.apply(lambda row: self.calulateDailyWinklerKPItbase(row, \
                                temperatureColumnsName=temperatureColumnsName,
                                tbase=float(threshold)), axis=1)
                if hourlyMethod == "TbaseMax":
                    self.dailyObservationsDF[destinationColumn]=\
                            self.dailyObservationsDF.apply(lambda row: self.calulateDailyWinklerKPItbaseMax(row, \
                                temperatureColumnsName=temperatureColumnsName,
                                tbase=float(threshold), tmax=maxWarmingThreshold), axis=1)

        return calculated

    def setDefaultDailyCummulativeWinklerKPI(self, columnNamePrefix:str="winkler")->bool:
        setDefault=True
        for threshold in self.warmingThresHoldArray:
            columnName=columnNamePrefix+"_"+str(threshold)
            for timeZero in self.timeZerosArray:
                columnName2=columnName+"_"+str(timeZero)
                for hourlyMethod in self.warmingHourlyMethodsArray:
                    columnName3=columnName2+"_"+str(hourlyMethod)
                    self.dailyObservationsDF[columnName3]=0
        
        return setDefault



    def calculateDailyTemperatureKPIs(self, force:bool=False, warmingFractionPrefix:str="warmingFraction",
                    destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"],
                    seasonColumnName:str="season", stationColumnName:str="estacion", cumSum:float=0.0,
                    maxWarmingThreshold:float=35.0)-> bool:
        """
            Calculate the daily contributions to Warming and Chilling Degrees Day on 
            self.observationsDF.
            Returns True if it correctly finishes or and exception is raised in other circunstances.
        """

        calculated = True 

        if (force is None):
            force=True

        if force:

            columnNames= self.createDailyWarmingChillingColumnNames(warmingFractionPrefix="warmingFraction",
                    destinationColumnNamePrefix=destinationColumnNamePrefix,
                    seasonColumnName="season", stationColumnName="estacion", cumSum=0.0)

            aggregationDF= self.getMinMaxMeanSumDailyValuesDataFrame(self.observationsDF,columnNames=columnNames)
            aggregationDF=self.renameDataFrameColumns(aggregationDF)
            inner_merged = pd.merge(self.dailyObservationsDF, aggregationDF, on=['estacion', 'ubi', 'anio','dia','fecha'])
            self.dailyObservationsDF=inner_merged

            self.setDefaultDailyWinklerContributionKPI(columnNamePrefix="winkler")

            self.calculateDailyWinklerTemperatureKPIs(temperatureColumnsName = ["tmed_min", "tmed_max"], 
                    destinationColumnNamePrefix="winkler",
                    maxWarmingThreshold=self.maxWarmingThresHold)
        
        return calculated


    def createDailyCumulativeColumns(self, destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip"], cumSum:float=0.0):
        initialized = True
        
        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]

        if (cumSum is None) :
            cumSum = 0.0

        for destinatonPrefix in destinationColumnNamePrefix:
            if destinatonPrefix=="gdd":
                thresholdsList=self.warmingThresHoldArray
                methods= self.warmingHourlyMethodsArray
            else:
                if destinatonPrefix=="chillingDD":
                    thresholdsList=self.chillingThresHoldArray
                    methods= self.chillingHourlyMethodsArray
                else:
                    thresholdsList=[""]
                    methods=[""]
            for threshold in thresholdsList:
                for timeZero in self.timeZerosArray:
                    for hourlyMethod in methods:
                        originColumn=destinatonPrefix+"_"+ str(threshold)+\
                                "_"+ timeZero +"_"+hourlyMethod
                        destinationColumn=originColumn+"_Cumm"   
                        self.dailyObservationsDF[destinationColumn]=cumSum        
        return initialized

    def initalizeDailyCumulativeColumns(self, destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip", "winkler"], cumSum:float=0.0,
        seasons:list=None, stations:list=None):

        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip", "winkler"]

        if (cumSum is None) :
            cumSum = 0.0

        seasonStationOriginDestinationcolumnZero={}
        for season in seasons:
            stationOriginDestinationcolumnZero={}
            for station in stations:
                cumSum=0.0
                originAndDestinationColumsZero = {}
                for destinatonPrefix in destinationColumnNamePrefix:
                    if (destinatonPrefix=="gdd") or (destinatonPrefix=="winkler"):
                        thresholdsList=self.warmingThresHoldArray
                        methods= self.warmingHourlyMethodsArray
                    else:
                        if destinatonPrefix=="chillingDD":
                            thresholdsList=self.chillingThresHoldArray
                            methods= self.chillingHourlyMethodsArray
                        else:
                            thresholdsList=[""]
                            methods=[""]
                    for threshold in thresholdsList:
                        for timeZero in self.timeZerosArray:
                            for hourlyMethod in methods:
                                datet0=self.determinet0(timeZero=timeZero,season=season, station=station)
                                originColumn=destinatonPrefix+"_"+ str(threshold)+\
                                        "_"+ timeZero +"_"+hourlyMethod
                                if (destinatonPrefix=="gdd") or (destinatonPrefix=="chillingDD"):
                                    originColumn=originColumn+"_sum"
                                destinationColumn=originColumn+"_Cumm"   
                                self.dailyObservationsDF[destinationColumn]=cumSum
                                originAndDestinationColumsZero[originColumn]=[destinationColumn, cumSum, datet0]
                stationOriginDestinationcolumnZero[station]=originAndDestinationColumsZero
            seasonStationOriginDestinationcolumnZero[season]=stationOriginDestinationcolumnZero
        
        return seasonStationOriginDestinationcolumnZero


    def initializeDailyWindyColumns(self, force:bool=False, destinationColumnNamePrefix:list="wind", cumSum:float=0.0,
        windDirections:list=["N","NE","E","SE","S","SW", "W", "NW"]):

        initialize=True
        if force:
            if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
                destinationColumnNamePrefix="wind"

            if (windDirections is None) or len(windDirections)==0:
                windDirections=["N","NE","E","SE","S","SW", "W", "NW"]


            if (cumSum is None) :
                cumSum = 0.0

            seasonStationOriginDestinationcolumnZero={}
            for windDirection in windDirections:
                newColumnName=destinationColumnNamePrefix+ "_"+ windDirection
                self.dailyObservationsDF[newColumnName]=cumSum
        return initialize


    def getWindyCummulativeColumnsFromHourlyObservations(self, df=None, groupingColumns:list=["estacion","fecha","season","windDirection"],
        columnNames:list=["vv", "hourFrac"]):
        minDf = None
        aggregations={}
        for key in columnNames:
            aggregations[key]=['sum']
            if (key=="hourFrac"):
                aggregations[key].append("mean")
        aggregationDF = df.groupby(groupingColumns, as_index=False).agg(aggregations)
        return aggregationDF


    def cummulateDailyWindyColumns(self, force:bool=False, destinationColumnNamePrefix:str="wind", seasons:list=None, stations:list=None,   
        groupingColumns:list=["estacion","fecha","windDirection"], columnNames:list=["vv", "hourFrac"]):

        cummulated=True

        if (force):
            for station in stations:
                windValues=self.getWindyCummulativeColumnsFromHourlyObservations(self.observationsDF, groupingColumns=groupingColumns,
                columnNames=["vv", "hourFrac"])
                windValues = self.renameDataFrameColumns(windValues)
                for index, row in windValues.iterrows():
                    date=row["fecha"]
                    windDirection=row["windDirection"]
                    windColumnName=destinationColumnNamePrefix+"_"+windDirection
                    windSpeed=row["vv_sum"]
                    dayFraction= row["hourFrac_mean"]
                    totalDayFractions=row["hourFrac_sum"]
                    dayFractions=24/dayFraction
                    windKPI=(totalDayFractions*windSpeed)/dayFractions

                    mask = (self.dailyObservationsDF[groupingColumns[0]]==station) &  \
                        (self.dailyObservationsDF[groupingColumns[1]]==date) 
                    self.dailyObservationsDF[windColumnName][mask] = windKPI

        return cummulated


    def calculateDailyCumulativeColumns(self, force:bool=False, seasonColumnName:str="season", 
            destinationColumnNamePrefix:list=["gdd","chillingDD","rad","precip","winkler"], dateColumnName:str="fecha",
            sortingColumnName:str="fecha", stationColumnName:str="estacion", cumSum:float=0.0)->bool:
        """
            It calculates all the cummulated colums for hourly DF (self.observatonDF)
            Parameters are the name of the columns to be considered.
                1.- Initialize all the columns to zero.
                2.- Determine all the possible cummulative values for all the origin and destination columns
                3.- Go through DF for calculating cummulative value.
        """

        calculate= True

        if (seasonColumnName is None) or len(seasonColumnName)==0:
            seasonColumnName="season"

        if (cumSum is None) :
            cumSum = 0.0

        if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]
        
        calculated = True

        if (force):
            if (seasonColumnName is None) or len(seasonColumnName)==0:
                seasonColumnName="season"

            if (cumSum is None) :
                cumSum = 0.0

            if (destinationColumnNamePrefix is None) or len(destinationColumnNamePrefix)==0:
                destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"]
        

            #Making sure dates columns are datetimes
            self.dailyObservationsDF[dateColumnName] = pd.to_datetime(self.dailyObservationsDF[dateColumnName])
            
            #self.createDailyCumulativeColumns(destinationColumnNamePrefix=destinationColumnNamePrefix, cumSum=cumSum)

            # # Initialize cummulative values
            seasons = self.determineSeasonsToBeProcessed(self.dailyObservationsDF, seasonColumnName=seasonColumnName)
            stations = self.determineStationsToBeProcessed(self.dailyObservationsDF, stationColumnName=stationColumnName)

            seasonStationOriginDestinationcolumnZero = self.initalizeDailyCumulativeColumns(destinationColumnNamePrefix=destinationColumnNamePrefix, 
                cumSum=cumSum, seasons=seasons, stations=stations)

            dataframes=[]
            for currentSeason in seasons:
                for station in stations:
                    currentSeasonDF = self.dailyObservationsDF.loc[ \
                        ((self.dailyObservationsDF[seasonColumnName]==currentSeason) & \
                        (self.dailyObservationsDF[stationColumnName]==station) ) \
                                ].sort_values(sortingColumnName, ascending=(True))
                    cseasonStationDestinationcolumns = copy.deepcopy(seasonStationOriginDestinationcolumnZero[currentSeason][station])
                    for index, row in currentSeasonDF.iterrows():
                        for originColumn in cseasonStationDestinationcolumns.keys():
                            destinationColumAndValue=cseasonStationDestinationcolumns[originColumn]
                            observationDateTime= row[sortingColumnName]
                            if (originColumn.startswith("rad")):
                                originColumn2 = "rad_sum"
                            else:
                                if (originColumn.startswith("precip")):
                                    originColumn2 = "precip_sum"
                                else:
                                    if originColumn.startswith("winkler"):
                                        splitOriginColumn=originColumn.split("_")
                                        originColumn2 = splitOriginColumn[0]+"_"+splitOriginColumn[1]+"_"+splitOriginColumn[3]
                                    else:
                                        originColumn2=originColumn
                            ddContribution=row[originColumn2]
                            destinationColumn= destinationColumAndValue[0]
                            cumSum = destinationColumAndValue[1]
                            t0 = destinationColumAndValue[2]
                            if t0<= observationDateTime:
                                cumSum+=ddContribution
                                cseasonStationDestinationcolumns[originColumn]=[destinationColumn,cumSum,t0]
                                currentSeasonDF.loc[ \
                                        (currentSeasonDF[sortingColumnName]==observationDateTime), \
                                            destinationColumn]=cumSum
                    dataframes.append(currentSeasonDF)

            self.dailyObservationsDF= pd.concat(dataframes)

        return calculated

    def readDFsFromCVSFiles(self, od:str="Campo de Borja", fileSeparator=";")->bool:
        typeOfSave="csv"
        read = True

        climaticDataFilePath = self.parameters.getParameter("odClimaticStationDataPath") 
        if not climaticDataFilePath.endswith(od) and not climaticDataFilePath.endswith(od+os.path.sep):
            if climaticDataFilePath.endswith(os.path.sep):
                filePath = climaticDataFilePath + od
            else:
                filePath = climaticDataFilePath + os.path.sep + od

        fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "."+ typeOfSave
        filePath=self.createOutputFileNamePath(filePath=filePath, fileName=fileName,typeOfSave=typeOfSave,
                fileSeparator=fileSeparator)

        if (os.path.isfile(filePath)):
            self.observationsDF = self.readDFFromCSVFile(filePath=filePath, separator=fileSeparator)
        
        self.observationsDF = self.convertColumnsToInteger(self.observationsDF, self.observationDFIntegerColumns)
        self.observationsDF = self.convertColumnsToFloat(self.observationsDF, self.observationDFFloatColumns)
        self.observationsDF = self.convertColumnsToDatetime(self.observationsDF, self.observationDFDateTimeColumns)
        self.observationsDF = self.convertColumnsToTime(self.observationsDF, self.observationDFTimeColumns)

        climaticDataFilePath = self.parameters.getParameter("odClimaticStationDataPath") 
        if not climaticDataFilePath.endswith(od) and not climaticDataFilePath.endswith(od+os.path.sep):
            if climaticDataFilePath.endswith(os.path.sep):
                filePath = climaticDataFilePath + od
            else:
                filePath = climaticDataFilePath + os.path.sep + od
        fileName=self.parameters.getParameter("hourlyPreprocessesFileName")+ "_daily."+ typeOfSave
        filePath=self.createOutputFileNamePath(filePath=filePath, fileName=fileName,typeOfSave=typeOfSave,
            fileSeparator=fileSeparator)
        if (os.path.isfile(filePath)):
            self.dailyObservationsDF = self.readDFFromCSVFile(filePath=filePath, separator=fileSeparator)

        self.dailyObservationsDF = self.convertColumnsToInteger(self.dailyObservationsDF, self.dailyObservationsDFIntegerColumns)
        self.dailyObservationsDF = self.convertColumnsToFloat(self.dailyObservationsDF, self.dailyObservationDFFloatColumns)
        self.dailyObservationsDF = self.convertColumnsToDatetime(self.dailyObservationsDF, self.dailyObservationDFDatetimeColumns)

        return read
    
    def getParcelClimaticStation(self, od:str="Campo de Borja", longitude:float=0.0, latitude:float=0.0,
        altitude:float=0.0)->str:
        stationCode=""
        mask=self.stationGISDF["OD"]==od
        dataFrame=self.stationGISDF.loc[mask]
        for index, row in dataFrame.iterrows():
            stationCode=row["ID"]
        return stationCode

    def getParcelClimaticStationData(self, stationID:str="Z14", filterHourlyColumns:list= ["estacion","timeStamp"],
            filterDailyColumns:list= ["estacion","fecha"],
            dateFrom:datetime=None)->str:

        hourlyDF=None
        dailyDF=None

        hourlyMask= (self.observationsDF[filterHourlyColumns[0]]==stationID) & \
            (self.observationsDF[filterHourlyColumns[1]]>=dateFrom)
        dailyMask= (self.dailyObservationsDF[filterDailyColumns[0]]==stationID) & \
            (self.dailyObservationsDF[filterDailyColumns[1]]>=dateFrom)

        hourlyDF=self.observationsDF.loc[hourlyMask].sort_values(filterHourlyColumns[1], ascending=(True))
        dailyDF=self.dailyObservationsDF.loc[dailyMask].sort_values(filterDailyColumns[1], ascending=(True))

        return hourlyDF, dailyDF

    def getClimaticDataForParcels(self, od:str="Campo de Borja", longitude:float=0.0, latitude:float=0.0,
        altitude:float=0.0, dateFrom=None):
        hourlyDF=None
        dailyDF=None

        parcelStation = self.getParcelClimaticStation(od=od, longitude=longitude, latitude=latitude, altitude=altitude)
        hourlyDF, dailyDF= self.getParcelClimaticStationData(stationID=parcelStation,
            filterHourlyColumns= ["estacion","timeStamp"],
            filterDailyColumns= ["estacion","fecha"],
            dateFrom=dateFrom)

        return hourlyDF, dailyDF, parcelStation


    def getSeasonForStation(self, station:str="Z14", dateFrom=None, filterDailyColumns:list = ["estacion","fecha"], 
        groupByColumns:list= ["estacion","season"]):
        """
            For the given station it returns the season from dateFrom to present as well as, starting and ending season date
        """
        seasonsDF=None

        if (station is None) or (len(station)==0):
            raise Exception("ClimaticStationDataFrame.getSeasonForParcels:  parcelStation parameter is not fixed.")
 
        if (filterDailyColumns is None) or (len(filterDailyColumns)==0):
            filterDailyColumns= ["estacion","fecha"]

        if (groupByColumns is None) or (len(groupByColumns)==0):
            groupByColumns= ["estacion","season"]
        
        
        #Making sure everything is a datetime
        self.dailyObservationsDF= self.convertColumnsToDatetime(dataframe=self.dailyObservationsDF, columns=[filterDailyColumns[1]])
        if isinstance(dateFrom, datetime.datetime):
            consideredDate = dateFrom
        if isinstance(dateFrom, datetime.date):
            consideredDate = dtDT(dateFrom.year, dateFrom.month, dateFrom.day)

        dailyMask= (self.dailyObservationsDF[filterDailyColumns[0]]==station) & \
            (self.dailyObservationsDF[filterDailyColumns[1]]>=consideredDate)
        dailyDF=self.dailyObservationsDF.loc[dailyMask].sort_values(filterDailyColumns[1], ascending=(True))

        minmaxDF = dailyDF.groupby(groupByColumns, as_index=False).agg({
              'fecha':['min', 'max','count']
            })

        minmaxDF = self.renameDataFrameColumns(minmaxDF)
        
        return minmaxDF

    # TODO Remove it?
    def readProcessedODClimaticStationData(self)->bool:
        read = True

        return read

    
    def calculateDayFromReferenceDate(self, row=None, referenceDate=None, 
        dateColumnName:str="fecha",
        timeZero:str="t0", season:str="2015_2016")->int:
        """
            If all parameters are correclty set it returns the number of days between referenceDate and dateColumnName
        """
        if  (row is None) :
            raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  row parameter is not present or it is empty.")
        if  (referenceDate is None) :
            raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  referenceDate parameter is not present or it is empty.")
        if  (dateColumnName is None) or (len(dateColumnName)==0):
            raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  dateColumnName parameter is not present or it is empty.")
        if  (timeZero is None) or (len(timeZero)==0):
            raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  timeZero parameter is not present or it is empty.")
        if  (season is None) or (len(season)==0):
            raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  season parameter is not present or it is empty.")



        numberOfDays=-2

        years= season.split("_")
        lastYear= years[1]

        if (timeZero=="t0"):
            dateReferenceDate = referenceDate.date()
        else:
            if (timeZero=="1"):
                dateReferenceDate = dtDT.strptime(lastYear+"-1-1", '%Y-%m-%d')
                dateReferenceDate  = dateReferenceDate.date()
            else:
                if (timeZero=="2"):
                    dateReferenceDate = dtDT.strptime(lastYear+"-2-1", '%Y-%m-%d')
                    dateReferenceDate  = dateReferenceDate.date()
                else:
                    raise Exception("ClimaticStationDataFrame.calculateDayFromReferenceDate:  not implemented t0: "+timeZero+ ".")

        rowDate = row[dateColumnName]
        if (type(rowDate)== str):
            rowDate = dtDT.strptime(rowDate, '%Y-%m-%d')
        rowDate = rowDate.date()
        dateDelta = rowDate - dateReferenceDate
        numberOfDays = dateDelta.days
        if numberOfDays<0:
            numberOfDays=0
        else:
            numberOfDays+=1

        return numberOfDays


    
    def calculateDailySeasonDay(self, force:bool=False, stations:list=None, minDate=None,
        filterDailyColumns:list = ["estacion","fecha"], groupByColumns:list= ["estacion","season"],
        newcolumnsName:list=["SeasonDay"])->bool:

        calculated=True

        if (force):
            if  (stations is None) or (len(stations)==0):
                raise Exception("ClimaticStationDataFrame.calculateSeasonDay:  stations parameter is not present or it is empty.")
            if  (newcolumnsName is None) or (len(newcolumnsName)==0):
                raise Exception("ClimaticStationDataFrame.calculateSeasonDay:  newcolumnsName parameter is not present or it is empty.")
            if  (minDate is None) :
                minDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')

            timeZeros=self.parameters.getParameter(name="timeZeros").split(";")
            columnNamePrefix=newcolumnsName[0]

            self.dailyObservationsDF=self.convertColumnsToDatetime(dataframe=self.dailyObservationsDF,
                columns=[filterDailyColumns[1]])

            #Default Season Day set to -1
            for timeZero in timeZeros:
                newcolumnName = columnNamePrefix+"_"+timeZero
                self.dailyObservationsDF[newcolumnName]=-1

            dataframes=[]

            for station in stations:
                minmaxDF=self.getSeasonForStation(station=station, dateFrom=minDate,
                            filterDailyColumns = filterDailyColumns, groupByColumns= groupByColumns)
                #Default Season Day set to -1
                for index, row in minmaxDF.iterrows():
                    season= row["season"]
                    startingSeasonDate= row["fecha_min"]
                    mask = (self.dailyObservationsDF[groupByColumns[0]]==station) &\
                        (self.dailyObservationsDF[groupByColumns[1]]==season)
                    stationSeasonDailyDF=self.dailyObservationsDF.loc[mask].sort_values(filterDailyColumns[1], ascending=(True))

                    for timeZero in timeZeros:
                        destinationColumn = columnNamePrefix+"_"+timeZero
                        stationSeasonDailyDF[destinationColumn]=stationSeasonDailyDF.apply (\
                            lambda dailyRow: self.calculateDayFromReferenceDate(row=dailyRow,  \
                                    referenceDate=startingSeasonDate, dateColumnName=filterDailyColumns[1],
                                    timeZero=timeZero, season=season), axis=1)
                    dataframes.append(stationSeasonDailyDF)
            self.dailyObservationsDF = pd.concat(dataframes)    
        return calculated

    def calculateHourlySeasonDay(self, force:bool=False, stations:list=None, minDate=None,
        filterDailyColumns:list = ["estacion","fecha"], groupByColumns:list= ["estacion","season"],
        newcolumnsName:list=["SeasonDay"])->bool:

        calculated=True

        if (force):
            if  (stations is None) or (len(stations)==0):
                raise Exception("ClimaticStationDataFrame.calculateHourlySeasonDay:  stations parameter is not present or it is empty.")
            if  (newcolumnsName is None) or (len(newcolumnsName)==0):
                raise Exception("ClimaticStationDataFrame.calculateHourlySeasonDay:  newcolumnsName parameter is not present or it is empty.")
            if  (minDate is None) :
                minDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')

            timeZeros=self.parameters.getParameter(name="timeZeros").split(";")
            columnNamePrefix=newcolumnsName[0]

            self.observationsDF=self.convertColumnsToDatetime(dataframe=self.observationsDF,
                columns=[filterDailyColumns[1]])

            #Default Season Day set to -1
            for timeZero in timeZeros:
                newcolumnName = columnNamePrefix+"_"+timeZero
                self.observationsDF[newcolumnName]=-1

            dataframes=[]

            for station in stations:
                minmaxDF=self.getSeasonForStation(station=station, dateFrom=minDate,
                            filterDailyColumns = filterDailyColumns, groupByColumns= groupByColumns)
                #Default Season Day set to -1
                for index, row in minmaxDF.iterrows():
                    season= row["season"]
                    startingSeasonDate= row["fecha_min"]
                    mask = (self.observationsDF[groupByColumns[0]]==station) &\
                        (self.observationsDF[groupByColumns[1]]==season)
                    stationSeasonHourlyDF=self.observationsDF.loc[mask].sort_values(filterDailyColumns[1], ascending=(True))

                    for timeZero in timeZeros:
                        destinationColumn = columnNamePrefix+"_"+timeZero
                        stationSeasonHourlyDF[destinationColumn]=stationSeasonHourlyDF.apply (\
                            lambda dailyRow: self.calculateDayFromReferenceDate(row=dailyRow,  \
                                    referenceDate=startingSeasonDate, dateColumnName=filterDailyColumns[1],
                                    timeZero=timeZero, season=season), axis=1)
                    dataframes.append(stationSeasonHourlyDF)
            self.observationsDF = pd.concat(dataframes)    
        return calculated
    
    def copyStationGIS2Observations(self, force:bool=False)-> bool:
        copied=True

        if force:
            stations= self.observationsDF.ubi.unique()

            dfColumns = self.observationsDF.columns
            if not ("stationLongitude" in dfColumns):
                self.observationsDF["stationLongitude"]= 0
            if not ("stationLatitude" in dfColumns):
                self.observationsDF["stationLatitude"]= 0
            if not ("stationAltitude" in dfColumns):
                self.observationsDF["stationLongitude"]= 0
            for station in stations:
                mask = (self.stationGISDF['Station (Settelment)']==station)
                gisStationDAta=self.stationGISDF.loc[mask]
                if len(gisStationDAta.index)!=1:
                    raise Exception("ClimaticStationDataFrame.copyStationGIS2Observations:  there is no station defined with name:"+ station+
                        " or there is more than one.  Review statations definitions")

                for index, row in gisStationDAta.iterrows():
                    stationID = row["ID"]
                    longitude = row["newLongitude"]
                    latitude = row["newLatitude"]
                    altitude = row["Altitude"]

                    self.observationsDF.loc[(self.observationsDF['estacion']==stationID),'stationLongitude'] = longitude
                    self.observationsDF.loc[(self.observationsDF['estacion']==stationID),'stationLatitude'] = latitude
                    self.observationsDF.loc[(self.observationsDF['estacion']==stationID),'stationAltitude'] = altitude

        return copied

    def copyStationGIS2HourlyObservations(self, force:bool=False)-> bool:
        copied=True

        if force:
            stations= self.observationsDF.ubi.unique()


            dfColumns = self.dailyObservationsDF.columns
            if not ("stationLongitude" in dfColumns):
                self.dailyObservationsDF["stationLongitude"]= 0
            if not ("stationLatitude" in dfColumns):
                self.dailyObservationsDF["stationLatitude"]= 0
            if not ("stationAltitude" in dfColumns):
                self.dailyObservationsDF["stationLongitude"]= 0

            for station in stations:
                mask = (self.stationGISDF['Station (Settelment)']==station)
                gisStationDAta=self.stationGISDF.loc[mask]
                if len(gisStationDAta.index)!=1:
                    raise Exception("ClimaticStationDataFrame.copyStationGIS2HourlyObservations:  there is no station defined with name:"+ station+
                        " or there is more than one.  Review statations definitions")

                for index, row in gisStationDAta.iterrows():
                    stationID = row["ID"]
                    longitude = row["newLongitude"]
                    latitude = row["newLatitude"]
                    altitude = row["Altitude"]


                    # mask = self.dailyObservationsDF['estacion'] == stationID
                    # idx = mask.idxmax() if mask.any() else np.repeat(False, len(self.dailyObservationsDF['estacion'] == stationID))
                    # self.dailyObservationsDF.loc[idx, 'stationLongitude'] = longitude
                    # self.dailyObservationsDF.loc[idx, 'stationLatitude'] = latitude
                    # self.dailyObservationsDF.loc[idx, 'stationAltitude'] = altitude
                    self.dailyObservationsDF.loc[(self.dailyObservationsDF['estacion']==stationID),'stationLongitude'] = longitude
                    self.dailyObservationsDF.loc[(self.dailyObservationsDF['estacion']==stationID),'stationLatitude'] = latitude
                    self.dailyObservationsDF.loc[(self.dailyObservationsDF['estacion']==stationID),'stationAltitude'] = altitude

        return copied

    def cleanPreviousSeasonDataFromDataSet (self, data:object=None, startYear:int=None):
    # For the received data(frame) returns the data created from starYear and created a dictionary with the stations as dates 
    # and the first date of the season <startYear-1>_<startYear>
    #
        if data is None or not isinstance(data, pd.DataFrame):
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSet:  data parameter is None or is not a dataset.")            
        if startYear is None:
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSet:  startYear parameter is None.")            
        if startYear < 2008 and startYear > date.today().year +1 :
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSet:  startYear parameter has not a valid value: "+
                 str(startYear))     
        try:       
            initialDate2UpdateByStation = []
            previousSeasonId = str(startYear-2)+'_'+str(startYear-1)
            data=data[data['season']!=previousSeasonId]

            #Minimun date having data for the season
            currentSeasonId = str(startYear)+'_'+str(startYear+1)
            currentSeasonData=data[data['season']==currentSeasonId]
            firstDatePerStation = data.groupby(["estacion"], as_index=False)['fecha'].min()
            initialDate2UpdateByStation = firstDatePerStation.to_dict('records')
        except Exception as e:
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSet catched Excpeton:  "+ str(e)+ " " + e.args)                 
        return data, initialDate2UpdateByStation

    def cleanPreviousSeasonDataFromDataSets (self, startYear:int=None):
    # For the received data(frame) returns the data created from starYear and created a dictionary with the stations as dates 
    # and the first date of the season <startYear-1>_<startYear>
    #
        observationDFinitialDate2UpdateByStation = []
        dailyobservationDFinitialDate2UpdateByStation = []
        try:
            self.dailyObservationsDF, dailyobservationDFinitialDate2UpdateByStation = self.cleanPreviousSeasonDataFromDataSet(
                data=self.dailyObservationsDF, startYear=startYear)
            self.observationsDF, observationDFinitialDate2UpdateByStation = self.cleanPreviousSeasonDataFromDataSet(
                data=self.observationsDF, startYear=startYear)
            for idx, dailyStationData in enumerate(dailyobservationDFinitialDate2UpdateByStation):
                found = False
                for idx2, hourlyStationData in enumerate(observationDFinitialDate2UpdateByStation):
                  if ((dailyStationData['estacion']==hourlyStationData['estacion'] ) and
                        abs(dailyStationData['fecha'].date()-hourlyStationData['fecha'].date()).days<=1):
                        found = True
                if not found:
                    raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSet: " + \
                        'the obtained values for first date of daily and hourly data per station do not exactly match dayly: ' + str(dailyStationData)+ 
                        ',\nHourly: ' + str(hourlyStationData) +'.')                 

    # does not exist

        except Exception as e:
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonDataFromDataSets catched Excpetion:  "+ str(e))                 
        return observationDFinitialDate2UpdateByStation, dailyobservationDFinitialDate2UpdateByStation


    def deleteDataToBeUpdated(self, hourlyObservationStartDate:list = None, 
            dailyObservationStartDateestaciones:list=None, 
            startYear:int=None, connectionData:dict=None)->bool:
        # '''
        # last: Turned True if we only want the last values of each station (for prediction). If the full database is 
        # desired use last='False'.
        # '''
        deleted = True
        data = None
    #     # cur = conexion.cursor()

        if startYear is None:
            raise ("ClimaticStationDataFrame.deleteDataToBeUpdated: invoqued with startYear having value None. " )

        current_year = date.today().year
        if startYear < 2008 or startYear > current_year:
            raise ("ClimaticStationDataFrame.deleteDataToBeUpdated parameter: invoqued with an invalid value for startYear parameter: " + str(startYear))
        
        if hourlyObservationStartDate is None:
            raise ("ClimaticStationDataFrame.deleteDataToBeUpdated: hourlyObservation parameter has not value." )
        if dailyObservationStartDateestaciones is None:
            raise ("ClimaticStationDataFrame.deleteDataToBeUpdated: dailyObservation parameter has not value." )
        if connectionData is None:
            raise ("ClimaticStationDataFrame.deleteDataToBeUpdated: connectionData parameter has not value." )


        # Deleting data to be update winkler of stations in hourly time base
        try:

            conexion = psycopg2.connect(database=connectionData["db_database"], 
                                        user=connectionData["db_user"], 
                                        password=connectionData["db_password"], 
                                        host=connectionData["db_host"], 
                                        port=connectionData["db_port"])
            cur = conexion.cursor()

            for stationData in hourlyObservationStartDate:
                station = stationData["estacion"]
                fecha = stationData["fecha"]
                query_Template = 'delete from  "ITAINNOVA"."MeteorologicalHourlyData" '
                query_Template = query_Template + "where estacion ='"+ station  \
                +"' and fecha >= '" +  fecha.strftime("%Y-%m-%d") + "'"
                cur.execute(query_Template)
                conexion.commit()

            for stationData in dailyObservationStartDateestaciones:
                station = stationData["estacion"]
                fecha = stationData["fecha"]
                query_Template = 'delete from  "ITAINNOVA"."MeteorologicalDailyData" '
                query_Template = query_Template + "where estacion ='"+ station  \
                +"' and fecha >= '" +  fecha.strftime("%Y-%m-%d") + "'"
                cur.execute(query_Template)
                conexion.commit()   

            cur.close()
            conexion.close()
        except Exception as e:
            cur.close()
            conexion.close()
            raise Exception("ClimaticStationDataFrame.deleteDataToBeUpdated catched exception:"+ str(e))
        
        return deleted
    
    def insertClimaticDataUpdates(self,  
            if_exists:bool=None, connectionData:dict=None)->bool:
        inserted  = True

        if if_exists is None:
            raise ("ClimaticStationDataFrame.insertClimaticDataUpdates: if_exists parameter has not value." )
        if connectionData is None:
            raise ("ClimaticStationDataFrame.insertClimaticDataUpdates: connectionData parameter has not value." )

        try:
            engine = create_engine('postgresql://' + connectionData["db_user"] + ':' + connectionData["db_password"] + \
                '@' + connectionData["db_host"] + ':' + str(connectionData["db_port"]) + '/meteo')
            self.dailyObservationsDF.to_sql('MeteorologicalDailyData', engine, schema='ITAINNOVA', if_exists=if_exists,
                                                                index=False, chunksize=1000, dtype=None, method=None)
            self.observationsDF.to_sql('MeteorologicalHourlyData', engine, schema='ITAINNOVA', if_exists=if_exists,
                                                                index=False, chunksize=1000, dtype=None, method=None)
            engine.dispose()
        except Exception as e:
            engine.dispose()  
            raise Exception("ClimaticStationDataFrame.insertClimaticDataUpdates catched exception:"+ str(e))          


        return inserted
    
    
    def loadClimaticDataDataFramesForNewDevelopments(self,  
            climaticStations:list=None, startYear:int=None)->bool:
        read  = True
        hourlyQuery = 'select * from "ITAINNOVA"."MeteorologicalHourlyData" <where> order by ubi, fecha'
        dailyQueryWhere = None
        conexion = None

        current_year = date.today().year
        if startYear is None:
            startYear = date.today().year
        if startYear < 2008 or startYear > current_year:
            raise Exception("ClimaticStationDataFrame.loadClimaticDataDataFramesForNewDevelopments: invoqued with an invalid value for startYear parameter: " + str(startYear))

        try:

            inList = ''
            if not(climaticStations is None) and len(climaticStations) > 0:
                inList = ' ('
                for index, station in enumerate(climaticStations):
                    inList = inList + " '" + station + "'"
                    if index < len(climaticStations)-1:
                        inList = inList + ','
                inList = inList + ')'
                hourlyeQueryWhere = ' where estacion in ' + inList
    

            if startYear > 2000 and startYear <= current_year:
                startDate = getStartDate(startYear)
                startDateStr = startDate.strftime('%Y-%m-%d')
                if dailyQueryWhere is None or len(dailyQueryWhere)==0:
                    hourlyeQueryWhere = "where"
                else:
                    dailyQueryWhere = dailyQueryWhere + " and"
                    hourlyeQueryWhere = hourlyeQueryWhere + " and"
                hourlyeQueryWhere = hourlyeQueryWhere + " fecha>='" + startDateStr + "'"

            hourlyQuery = hourlyQuery.replace('<where>', hourlyeQueryWhere) 

            conexion = psycopg2.connect(database=cf.postgress_Database, #meteorological_past_database, 
                                        user=cf.postgress_Username,#meteorological_past_user, 
                                        password=cf.postgress_Password,#meteorological_past_password, 
                                        host=cf.postgress_Host,#meteorological_past_host, 
                                        port=cf.postgress_Port)#meteorological_past_port)

            self.observationsDF = pd.read_sql_query(hourlyQuery, con=conexion)# conexion.commit done
            conexion.commit()
            conexion.close()
        except Exception as e:
            if not conexion is None:
                conexion.commit()
                conexion.close()
            raise Exception("ClimaticStationDataFrame.loadClimaticDataDataFramesForNewDevelopments catched exception:"+ str(e))          

        return read

    def initializeDailyGubler(self,  
            climaticStations:list=None, startYear:int=None)->bool:
        read  = True
        dailyQuery = 'SELECT distinct   indicativo, "date" FROM "TablasAuxiliares".estacion_calendario_horas_minutos <where> order by indicativo, "date"'
        dailyQueryWhere = ''
        conexion = None

        current_year = date.today().year
        current_date = date.today()
        if startYear is None:
            startYear = date.today().year
        if startYear < 2008 or startYear > current_year:
            raise Exception("ClimaticStationDataFrame.initializeDailyGubler: invoqued with an invalid value for startYear parameter: " + str(startYear))

        try:

            inList = ''
            if not(climaticStations is None) and len(climaticStations) > 0:
                inList = ' ('
                for index, station in enumerate(climaticStations):
                    inList = inList + " '" + station + "'"
                    if index < len(climaticStations)-1:
                        inList = inList + ','
                inList = inList + ')'
                dailyQueryWhere = ' where indicativo in ' + inList
    

            if startYear > 2000 and startYear <= current_year:
                startDate = getStartDate(startYear)
                startDateStr = startDate.strftime('%Y-%m-%d')
                if dailyQueryWhere is None or len(dailyQueryWhere)==0:
                    dailyQueryWhere = " where "
                else:
                    dailyQueryWhere = dailyQueryWhere + " and"
                dailyQueryWhere = dailyQueryWhere +   ' "date" ' + " >='" + startDateStr + "'"
            if len(dailyQueryWhere)== '0':
                dailyQueryWhere = " where " ' "date" ' + " <'" + current_date.strftime("%Y-%d-%m") + "'"
            else:
                dailyQueryWhere =  dailyQueryWhere + " and " ' "date" ' + " <'" + current_date.strftime("%Y-%m-%d") + "'"
                
            dailyQuery = dailyQuery.replace('<where>', dailyQueryWhere) 

            conexion = psycopg2.connect(database=cf.postgress_Database, #meteorological_past_database, 
                                        user=cf.postgress_Username,#meteorological_past_user, 
                                        password=cf.postgress_Password,#meteorological_past_password, 
                                        host=cf.postgress_Host,#meteorological_past_host, 
                                        port=cf.postgress_Port)#meteorological_past_port)

            self.dailyGublerDF = pd.read_sql_query(dailyQuery, con=conexion)# conexion.commit done
            conexion.commit()
            conexion.close()

            self.dailyGublerDF.rename(columns={ cf.GublerCalculation["dailySelectedFields"][0]: cf.GublerCalculation["hourlySelectedFields"][0],
                                   cf.GublerCalculation["dailySelectedFields"][1]:cf.GublerCalculation["hourlySelectedFields"][2]}, inplace=True)
            self.dailyGublerDF.rename(columns={ cf.GublerCalculation["dailySelectedFields"][0]: cf.GublerCalculation["hourlySelectedFields"][0],
                                   cf.GublerCalculation["dailySelectedFields"][1]:cf.GublerCalculation["hourlySelectedFields"][2]}, inplace=True)
            self.dailyGublerDF[cf.GublerCalculation["hourlySelectedFields"][2]]= pd.to_datetime(self.dailyGublerDF[cf.GublerCalculation["hourlySelectedFields"][2]])

            auxDF = self.observationsDF.reindex(columns=cf.GublerCalculation["hourlySelectedFields"])
            auxDF["tmed"] = auxDF["tmed"].fillna(0)
            aggdictionary = {
                cf.GublerCalculation["hourlySelectedFields"][5]:'min'
                }
            groupBycolumns = cf.GublerCalculation["dailyGroupByFields"]
            groupBycolumns.append(cf.GublerCalculation["hourlySelectedFields"][2])
            auxDF[cf.GublerCalculation["hourlySelectedFields"][2]] = auxDF[cf.GublerCalculation["hourlySelectedFields"][2]].dt.date
            auxDF = auxDF.groupby(groupBycolumns, as_index=False).agg(aggdictionary).reset_index()
            auxDF[cf.GublerCalculation["hourlySelectedFields"][2]] = pd.to_datetime(auxDF[cf.GublerCalculation["hourlySelectedFields"][2]])
            mergeColumns = [cf.GublerCalculation["hourlySelectedFields"][0],cf.GublerCalculation["hourlySelectedFields"][2]]
            self.dailyGublerDF = pd.merge(self.dailyGublerDF, auxDF,  how='left', on=mergeColumns)


        except Exception as e:
            if not conexion is None:
                conexion.commit()
                conexion.close()
            raise Exception("ClimaticStationDataFrame.initializeDailyGubler catched exception:"+ str(e))          

        return read


    def calculateDailyGublerContribution(self, row= None)->float:
        try:
            if row is None:
                raise Exception("ClimaticStationDataFrame.calculateDailyGublerContribution row is None." )     

            tmin = row[cf.GublerCalculation["dailyFields"][0]]
            if math.isnan(tmin):
                tmin = 0.0
            hoursIn_21_30_C =  row[cf.GublerCalculation["dailyFields"][0]]
            if math.isnan(hoursIn_21_30_C):
                hoursIn_21_30_C = 0.0            

            if tmin > 35.0:
                return -10.0
            if hoursIn_21_30_C < 6.0:
                return -10.0
            if hoursIn_21_30_C >= 6.0:
                return 20.0
        except Exception as e: 
            raise Exception("ClimaticStationDataFrame.calculateDailyGublerContribution catched exception:"+ str(e) )     
        
        return 0.0
    
    def calculateDailyGublerContributionDay(self, row= None)->int:
        try:
            if row is None:
                raise Exception("ClimaticStationDataFrame.calculateDailyGublerContributionDay row is None." )     

            tmin = row[cf.GublerCalculation["dailyFields"][0]]
            if math.isnan(tmin):
                tmin = 0
            hoursIn_21_30_C =  row[cf.GublerCalculation["dailyFields"][0]]
            if math.isnan(hoursIn_21_30_C):
                hoursIn_21_30_C = 0            

            if tmin > 35.0:
                return 0
            if hoursIn_21_30_C < 6.0:
                return 0
            if hoursIn_21_30_C >= 6.0:
                return 1
        except Exception as e: 
            raise Exception("ClimaticStationDataFrame.calculateDailyGublerContributionDay catched exception:"+ str(e) )     
        
        return 0

    def putZeroInDaysNotContributing(self, row= None, estacion=None, season=None, fecha=None)->int:
        pass
        try:
            if row is None:
                raise Exception("ClimaticStationDataFrame.putZeroInDaysNotContributing row is None." )     
            if estacion is None:
                raise Exception("ClimaticStationDataFrame.putZeroInDaysNotContributing estacion is None." )     
            if season is None:
                raise Exception("ClimaticStationDataFrame.putZeroInDaysNotContributing season is None." )     
            if fecha is None:
                raise Exception("ClimaticStationDataFrame.putZeroInDaysNotContributing fecha is None." )   

            if (row[cf.GublerCalculation["hourlySelectedFields"][0]]== estacion 
                and row[cf.GublerCalculation["hourlySelectedFields"][4]]==season 
                and row[cf.GublerCalculation["hourlySelectedFields"][2]]< fecha):
                return 0
            else:
                return row[cf.GublerCalculation["dailyFields"][3]]
        except Exception as e: 
            raise Exception("ClimaticStationDataFrame.calculateDailyGublerContributionDay catched exception:"+ str(e) )     
        
        return 0
    
    def calculateDateOfConditions(self,df:object = None)->object:
        dfs = []
        df2 = None
        if df is None:
                raise Exception("ClimaticStationDataFrame.calculateDateOfConditions df parameter is None." )    
        if not isinstance(df, pd.DataFrame): 
                raise Exception("ClimaticStationDataFrame.calculateDateOfConditions df parameter is not a dataframe." )    
        try:
            for gb,x0 in df.groupby(['estacion','ubi','season']):
            #     print('-'*100)
            #     print(gb)
                x = x0.sort_values(['estacion','ubi','season','fecha']).copy()
                x['init_cond'] = (x.dailyContribution.rolling(window=3,center=False).sum()==60).cumsum()>0
                x['subgroup'] = (x.dailyContribution.diff().abs()>0).cumsum()
                x[cf.GublerCalculation["dailyFields"][4]] = range(len(x))
                x = pd.merge(x,x.groupby('subgroup').Gublert0Aux2.min().rename('MIN'),on='subgroup')
                x[cf.GublerCalculation["dailyFields"][4]] = x[cf.GublerCalculation["dailyFields"][4]]-x['MIN']+1
                #x = x.drop(['subgroup','MIN', 'ubi', 'season', 'tmed', 'hoursIn_21_30_C',  'dailyContribution', 'Gublert0Aux1'],axis=1)
            #     display(x)
                dfs.append(x)
            df2 = pd.concat(dfs,ignore_index=True)
        except Exception as e:
            raise Exception("ClimaticStationDataFrame.calculateDateOfConditions catched exception:"+ str(e) )     
        return df2

    def calculateConditionsOfPowderyMildew(self):
        # Comienza el cálculo del Gubler según el paper https://www.apsnet.org/edcenter/apsnetfeatures/Pages/UCDavisRisk.aspx ,doi: 10.1094/APSnetFeature-1999-0199

        calculated = True
        try:
            # Me quedo sólo con las columnas que me interesan para el Gubler
            self.hourlyGublerDF = self.observationsDF.reindex(columns=cf.GublerCalculation["hourlySelectedFields"])
            self.hourlyGublerDF = self.observationsDF[cf.GublerCalculation["hourlySelectedFields"]]

            # Calculo indicadores de idoneidad por fracción de día para la que hay registro
            self.hourlyGublerDF["tmed"] = self.hourlyGublerDF["tmed"].fillna(0)
            self.hourlyGublerDF[cf.GublerCalculation["hourlyFields"][0]]=(self.hourlyGublerDF["tmed"]>= 21.0) & (self.hourlyGublerDF["tmed"]<= 30.0)

            # Calculo del número de horas en el primer intérvalo
            auxDF = self.hourlyGublerDF.loc[self.hourlyGublerDF[cf.GublerCalculation["hourlyFields"][0]]]
            auxDF[cf.GublerCalculation["hourlySelectedFields"][2]] = pd.to_datetime(auxDF[cf.GublerCalculation["hourlySelectedFields"][2]]).dt.date
            aggdictionary = {
                # cf.GublerCalculation["hourlySelectedFields"][5]:'min',
                cf.GublerCalculation["hourlyFields"][0]:'count'
                }
            auxDF = auxDF.groupby(cf.GublerCalculation["hourlyGroupByFields"], as_index=False).agg(aggdictionary).reset_index()

            # Me quedo cambiando la forma de calculo, la parte del merge no hace falta, creo
            auxDF[cf.GublerCalculation["hourlyFields"][0]] = auxDF[cf.GublerCalculation["hourlyFields"][0]] * \
                auxDF[cf.GublerCalculation["hourlySelectedFields"][3]]
            auxDF.rename(columns={cf.GublerCalculation["hourlyFields"][0]: cf.GublerCalculation["dailyFields"][0],
                                  cf.GublerCalculation["hourlySelectedFields"][5]: cf.GublerCalculation["dailyFields"][1]}, inplace=True)
            auxDF.drop(columns=[cf.GublerCalculation["hourlySelectedFields"][1]], inplace=True)
            auxDF.drop(columns=[cf.GublerCalculation["hourlySelectedFields"][3]], inplace=True)
            auxDF.drop(columns=[cf.GublerCalculation["hourlySelectedFields"][4]], inplace=True)
            auxDF[cf.GublerCalculation["hourlySelectedFields"][2]]= pd.to_datetime(auxDF[cf.GublerCalculation["hourlySelectedFields"][2]])

            # Creo el campo para calcular la contribucion diaria al Gubler
            mergeColumns = [cf.GublerCalculation["hourlySelectedFields"][0],cf.GublerCalculation["hourlySelectedFields"][2]]
            auxDF[cf.GublerCalculation["dailyFields"][0]] = auxDF[cf.GublerCalculation["dailyFields"][0]].fillna(0)
            self.dailyGublerDF = pd.merge(self.dailyGublerDF, auxDF,  how='left', on=mergeColumns)
            self.dailyGublerDF[cf.GublerCalculation["dailyFields"][2]]=-1
            self.dailyGublerDF[cf.GublerCalculation["dailyFields"][2]] = 0
            self.dailyGublerDF[cf.GublerCalculation["dailyFields"][2]] = \
                self.dailyGublerDF.apply(lambda x: self.calculateDailyGublerContribution(row = x), axis =1)
            self.dailyGublerDF.drop(columns=["index_x","index_y"], inplace=True)
            self.observationsDF.reindex(columns=[cf.GublerCalculation["hourlySelectedFields"][0],
                                                 cf.GublerCalculation["hourlySelectedFields"][2],
                                                 cf.GublerCalculation["hourlySelectedFields"][4]])
            self.dailyGublerDF.sort_values(by=[cf.GublerCalculation["hourlySelectedFields"][0],
                                               cf.GublerCalculation["hourlySelectedFields"][2],
                                               cf.GublerCalculation["hourlySelectedFields"][4]], 
                                               inplace=True,    ascending = [True, True, True])

            self.dailyGublerDF[cf.GublerCalculation["dailyFields"][3]] =  \
                self.dailyGublerDF.apply(lambda x: self.calculateDailyGublerContributionDay(row = x), axis =1)
            groupByFields = [cf.GublerCalculation["hourlySelectedFields"][0],
                             cf.GublerCalculation["hourlySelectedFields"][1],
                             cf.GublerCalculation["hourlySelectedFields"][2],
                    cf.GublerCalculation["hourlySelectedFields"][4],
                    cf.GublerCalculation["dailyFields"][3]
                    ]
            # self.dailyGublerDF[cf.GublerCalculation["dailyFields"][4]] = self.dailyGublerDF.groupby(groupByFields).cumcount()
            auxDF1= self.dailyGublerDF[['estacion', 'ubi',   'season', 'dailyContribution','Gublert0Aux1']].copy()
            # self.dailyGublerDF[cf.GublerCalculation["dailyFields"][4]] = \
            #     auxDF1.groupby(['estacion', 'ubi',   'season',
            #                                             'dailyContribution','Gublert0Aux1']).cumcount()
            auxDF2 = self.calculateDateOfConditions(self.dailyGublerDF)
            # self.saveDataFrameToCsv(df=auxDF2,filePath=r'C:\TEMP\GRAPEVINE\20230328_testGubler01.csv', fileSeparator=";")

            auxDF3 = auxDF2[auxDF2['init_cond'] == True][["estacion","season","fecha"]]
            auxDF3 = auxDF3.groupby(['estacion','season'])['fecha'].min().to_frame()
            auxDF3.reset_index(inplace=True)

            auxDF2["GublerIndex"]= 0.0
            auxDF2["GublerRiskLevel"]= 0
            listDFs=[]
            for index, row in auxDF3.iterrows():
                currentGubler = 0.0
                auxDF4= auxDF2[(auxDF2['season']==row['season']) & (auxDF2['fecha']>=row['fecha'])]
                auxDF4.reset_index(inplace=True)
                for index4, row4 in auxDF4.iterrows():
                    currentGubler = currentGubler + row4['dailyContribution']
                    if currentGubler < 0:
                        currentGubler = 0
                    if currentGubler > 100:
                        currentGubler = 100
                    auxDF4.at[index4, 'GublerIndex']= currentGubler
                    if currentGubler> 60:
                        auxDF4.at[index4, 'GublerRiskLevel']=4
                    if currentGubler> 50 and currentGubler<=60:
                        auxDF4.at[index4, 'GublerRiskLevel']=3
                    if currentGubler> 40 and currentGubler<=50:
                        auxDF4.at[index4, 'GublerRiskLevel']=2
                    if currentGubler> 30 and currentGubler<=40:
                        auxDF4.at[index4, 'GublerRiskLevel']=1
                    if currentGubler<= 30 :
                        auxDF4.at[index4, 'GublerRiskLevel']=0
                listDFs.append(auxDF4)
            auxDF5=pd.concat(listDFs)

            self.dailyGublerDF = auxDF5
            # self.saveDataFrameToCsv(df=auxDF5,filePath=r'C:\TEMP\GRAPEVINE\20230328_testGubler05.csv', fileSeparator=";")



        except Exception as e:
            raise Exception("ClimaticStationDataFrame.calculateConditionsOfPowderyMildew catched exception:"+ str(e))     
        return calculated


    def cleanPreviousSeasonGublerDataFromDataSet (self, startYear:int=None)->list:
    # For the received data(frame) returns the data created from starYear and created a dictionary with the stations as dates 
    # and the first date of the season <startYear-1>_<startYear>
    #
        observationDFinitialDate2UpdateByStation = []
        dailyobservationDFinitialDate2UpdateByStation = []
        try:
            self.hourlyGublerDF, gublerDataStartDateestaciones = self.cleanPreviousSeasonDataFromDataSet(
                data=self.hourlyGublerDF, startYear=startYear)
            self.dailyGublerDF, gublerDataStartDateestaciones = self.cleanPreviousSeasonDataFromDataSet(
                data=self.dailyGublerDF, startYear=startYear)

    # does not exist

        except Exception as e:
            raise Exception("ClimaticStationDataFrame.cleanPreviousSeasonGublerDataFromDataSet catched Excpetion:  "+ str(e))                 
        return gublerDataStartDateestaciones
    
    def deleteGublerDataToBeUpdated(self, gublerDataStartDateestaciones:list=None, 
            startYear:int=None, connectionData:dict=None)->bool:
        # '''
        # last: Turned True if we only want the last values of each station (for prediction). If the full database is 
        # desired use last='False'.
        # '''
        deleted = True
        data = None
    #     # cur = conexion.cursor()

        if startYear is None:
            raise ("ClimaticStationDataFrame.deleteGublerDataToBeUpdated: invoqued with startYear having value None. " )

        current_year = date.today().year
        if startYear < 2008 or startYear > current_year:
            raise ("ClimaticStationDataFrame.deleteGublerDataToBeUpdated parameter: invoqued with an invalid value for startYear parameter: " + str(startYear))
        
        if gublerDataStartDateestaciones is None:
            raise ("ClimaticStationDataFrame.deleteGublerDataToBeUpdated: dailyObservation parameter has not value." )
        if connectionData is None:
            raise ("ClimaticStationDataFrame.deleteGublerDataToBeUpdated: connectionData parameter has not value." )


        # Deleting data to be update winkler of stations in hourly time base
        try:

            conexion = psycopg2.connect(database=connectionData["db_database"], 
                                        user=connectionData["db_user"], 
                                        password=connectionData["db_password"], 
                                        host=connectionData["db_host"], 
                                        port=connectionData["db_port"])
            cur = conexion.cursor()
            table = cf.GublerCalculation["schema"] + '.' + cf.GublerCalculation["dailytable"]
            hourlytable = cf.GublerCalculation["schema"] + '.' + cf.GublerCalculation["hourlytable"]

            for stationData in gublerDataStartDateestaciones:
                station = stationData["estacion"]
                fecha = stationData["fecha"]

                query_Template = 'delete from  ' +  hourlytable +' '
                query_Template = query_Template + "where estacion ='"+ station  \
                +"' and fecha >= '" +  fecha.strftime("%Y-%m-%d") + "'"
                cur.execute(query_Template)
                conexion.commit()   


                query_Template = 'delete from  ' +  table +' '
                query_Template = query_Template + "where estacion ='"+ station  \
                +"' and fecha >= '" +  fecha.strftime("%Y-%m-%d") + "'"
                cur.execute(query_Template)
                conexion.commit()   

            cur.close()
            conexion.close()
        except Exception as e:
            cur.close()
            conexion.close()
            raise Exception("ClimaticStationDataFrame.deleteGublerDataToBeUpdated catched exception:"+ str(e))
        
        return deleted
    
    def gublerDataInsertOrUpdate(self,  
            if_exists:bool=None, connectionData:dict=None)->bool:
        inserted  = True

        if if_exists is None:
            raise ("ClimaticStationDataFrame.gublerDataInsertOrUpdate: if_exists parameter has not value." )
        if connectionData is None:
            raise ("ClimaticStationDataFrame.gublerDataInsertOrUpdate: connectionData parameter has not value." )

        hourlytable =  cf.GublerCalculation["hourlytable"]
        table =  cf.GublerCalculation["dailytable"]
        schema = cf.GublerCalculation["schema"] 


        try:
            engine = create_engine('postgresql://' + connectionData["db_user"] + ':' + connectionData["db_password"] + \
                '@' + connectionData["db_host"] + ':' + str(connectionData["db_port"]) + '/meteo')
            self.hourlyGublerDF.to_sql(hourlytable, engine, schema=schema, if_exists=if_exists,
                                                                index=False, chunksize=1000, dtype=None, method=None)
            self.dailyGublerDF.to_sql(table, engine, schema=schema, if_exists=if_exists,
                                                                index=False, chunksize=1000, dtype=None, method=None)
            engine.dispose()
        except Exception as e:
            engine.dispose()  
            raise Exception("ClimaticStationDataFrame.gublerDataInsertOrUpdate catched exception:"+ str(e))          


        return inserted