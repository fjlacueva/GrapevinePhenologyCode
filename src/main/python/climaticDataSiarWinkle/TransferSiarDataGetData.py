from re import S
import sys
import pandas as pd
import numpy as np
import psycopg2
import utm
import os
import config as cf
logger= cf.logger

from sqlalchemy import create_engine

from HandlingDataFrames.ClimaticStationDataFrame import ClimaticStationDataFrame
from Parameters import Parameters
import sys
import datetime
from datetime import datetime as dtDT
from datetime import date

def add_hours(fecha, minutes_sconds):
    return fecha + pd.DateOffset(hours=float(minutes_sconds//100), minutes= float(minutes_sconds%100))

def getStartDate (startYear:int=None)->datetime: 
    if startYear is None:
        raise Exception("getStartDate: invoqued with and invalid value for startYear parameter.")
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
        raise Exception("get_past_meteorological: invoqued with an invalid value for startYear parameter: " + str(startYear))
    
    if estaciones is None:
        raise Exception("get_past_meteorological: estaciones parameter has not value." )
    
    whereStr = ""
    #query_Template = "select * "
    #query_Template = query_Template + "FROM public.gv_aragoneseclimaticstationsdatawithforecastandestimatedata acsd"

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
        
    if len(data) > 0:
        data['fecha']=data.apply(lambda row: add_hours(row['fecha'],row['horamin']),axis=1)
        
        data=data[['fecha', 'nombrecorto', 'idprovincia', 'idestacion', 'tempmedia', 'humedadmedia', 'velviento', 'dirviento', 
                'radiacion', 'precipitacion', 'tempmediacaja', 'tempsuelo1', 'tempsuelo2', 'codtempsuelo2', 'xutm', 'yutm', 
                'huso', 'altitud', 'latitude', 'longitude', 'horamin']]
        
    return data

def executeWickle(inicio,inicioYear):
    logger.info('executing Winckler with inicio:{};inicioYear:{}'.format( str(inicio), str(inicioYear)))
    conexion = psycopg2.connect(database=cf.postgress_Database, #meteorological_past_database, 
                                user=cf.postgress_Username,#meteorological_past_user, 
                                password=cf.postgress_Password,#meteorological_past_password, 
                                host=cf.postgress_Host,#meteorological_past_host, 
                                port=cf.postgress_Port)#meteorological_past_port)
    

    if cf.allStations == False:
        estaciones = cf.stations
    else:
        cur = conexion.cursor()
        cur.execute("""SELECT distinct indicativo FROM public.gv_estaragonrestringido order by indicativo;""")
        #cur.execute("""SELECT distinct indicativo FROM public.gv_estaragonrestringido where indicativo like 'gv05' order by indicativo;""")
        rows = cur.fetchall()

        estaciones = []
        for row in rows:
            #if (row[0].upper() != 'GV13'):
            estaciones.append(row[0])

        conexion.close()
    
    if inicio == False:
        currentDateTime = datetime.datetime.now()
        date = currentDateTime.date()
        startYear = int(date.strftime("%Y"))
    else:
        startYear = inicioYear

    logger.debug('estaciones: ' + str(estaciones))
    logger.debug('startYear: ' + str(startYear))

    datosDailyList=[]
    datosHourlyList=[]

    estaciones = ['HU18']

    connectionData={}
    connectionData["db_database"]=cf.postgress_Database
    connectionData["db_user"]=cf.postgress_Username
    connectionData["db_password"]=cf.postgress_Password
    connectionData["db_host"]=cf.postgress_Host
    connectionData["db_port"]=cf.postgress_Port

    # if_exists='replace'
    if_exists='append'

    for estacion in estaciones:

        try:
            listEstacion = [estacion]
            datos_meteo_past=get_past_meteorological(estaciones=listEstacion, startYear= startYear, last=False)
            if len(datos_meteo_past) > 0:
                datos_meteo_past['corrected']=0
                if estacion == 'Z01':
                    datos_meteo_past2=get_past_meteorological(estaciones=listEstacion, startYear= startYear, last=False)
                    busca=datos_meteo_past2[datos_meteo_past2.nombrecorto=='Z05'].sort_values('fecha').set_index('fecha')
                    datos_extra=busca[(busca.index>pd.to_datetime('01/01/2018Z00:00:00',format='%d/%m/%YZ%H:%M:%S')) \
                        &(busca.index<pd.to_datetime('01/01/2019Z00:00:00',format='%d/%m/%YZ%H:%M:%S'))]
                    datos_extra['nombrecorto']='Z01'
                    datos_extra['corrected']=1
                    datos_meteo_past=pd.concat([datos_meteo_past,datos_extra.reset_index()])
                datos_meteo_past[datos_meteo_past.humedadmedia<0]['corrected']=2
                datos_meteo_past[datos_meteo_past.humedadmedia<0]['humedadmedia']=0
                datos_est=datos_meteo_past[datos_meteo_past.nombrecorto==estacion]
                datos_est['ubi']=datos_est['nombrecorto']
                datos_est['estacion']=datos_est['nombrecorto']
                datos_est['anio']=datos_est['fecha'].dt.year
                datos_est['dia']=datos_est['fecha'].dt.dayofyear
                # datos_est['horamin']=datos_est['fecha'].dt.hour*100+datos_est['fecha'].dt.minute
                datos_est['tmed']=datos_est['tempmedia']
                datos_est['hr']=datos_est['humedadmedia']
                datos_est['vv']=datos_est['velviento']
                datos_est['dv']=datos_est['dirviento']
                datos_est['rad']=datos_est['radiacion']
                datos_est['precip']=datos_est['precipitacion']
                datos_est["t10"] = np.nan
                datos_est["t30"] = np.nan

                datos_est=datos_est[['estacion', 'ubi', 'anio','dia', 'fecha','horamin','tmed', 'hr', 'vv', 'dv', 'rad', 'precip', 't10', 't30', 'corrected']]


                ### TODOS ESTOS PARÁMETROS NO SE USAN PARA NAD, SOLO SIRVEN PARA QUE ESTÉN CONTENTAS LAS FUNCIONES DE FRANCISCO ###
                parameters = Parameters()
                force = True

                parameters.setParameter(name="dataDriver", value="localDisk")     

                #odExcelFolder = odExcelFolder #r'C:\Users\fjlacueva\Downloads\Grapevine\DatosIntermedios'
                #SIARstationGISFile = SIARstationGISFile #r'C:\Users\fjlacueva\Downloads\Grapevine\DatosIntermedios\20200604_Climatic_And_Parcel_Data.xlsx'
                odExcelFolder = cf.realClimateDataCalculus_odExcelFolder
                SIARstationGISFile = cf.realClimateDataCalculus_SIARstationGISFile

                kindOfClimaticData = 'Horarios'

                parameters.setParameter(name="diaryObservationsPathsAndPatterns", value=odExcelFolder+";"+kindOfClimaticData)


                parameters.setParameter(name="stationsGISPathDefinitions", 
                    value=SIARstationGISFile)

                parameters.setParameter(name="timeZeros", value ="t0;1;2")

                parameters.setParameter(name="warmingThresHoldArray", value ="4.5;10.0")
                # parameters.setParameter(name="maxWarmingThresHold", value =30.0)
                # por lo hablado con cscv el día 20220902
                parameters.setParameter(name="maxWarmingThresHold", value =35.0)
                parameters.setParameter(name="warmingHourlyMethods", value ="Tbase;TbaseMax")


                parameters.setParameter(name="chillingThresHoldArray", value ="7.0")
                parameters.setParameter(name="minChillingThreshold", value =0.0)
                parameters.setParameter(name="chillingHourlyMethods", value ="Tbase;Tbasemin;Utah")


                parameters.setParameter(name="hourlyPreprocessesFileName", value ="PreprocessedHourlyObservations")

                mandatoryParameters=ClimaticStationDataFrame.mandatoryParameters
                parameters.addMandatoryParameters(names=mandatoryParameters)

                climaticStationsDataFrame = ClimaticStationDataFrame(parameters=parameters)
                climaticStationsDataFrame.observationsDF=datos_est

                climaticStationsDataFrame.convertRowHoraMinToTime(originColumn="horamin", destinationColumn="horaminTime")
                climaticStationsDataFrame.createRowDateTime( dateColumnName="fecha", timeColumnName="horamin", dateTimeColumnName="timeStamp")

                climaticStationsDataFrame.setRowsReferenceDayOfWeek(columnName="weekReference", dateColumnName="fecha", dayOfWeekToReturn=3)

                climaticStationsDataFrame.setHourFraction(columnName="hourFrac", fractionValue=0.5)


                climaticStationsDataFrame.setWarmingThresholdRows(columnNamePrefix="warmingFraction", hourFractionColumnName="hourFrac",
                    temperatureColumnName="tmed", maxWarmingThreshold= climaticStationsDataFrame.maxWarmingThresHold)
                climaticStationsDataFrame.setChillingThresholdRows(columnNamePrefix="chillingFraction", hourFractionColumnName="hourFrac", 
                    temperatureColumnName="tmed", minChillingThreshold=climaticStationsDataFrame.minChillingThreshold)

                climaticStationsDataFrame.setRowsWindDirection(columnName="windDirection",windDegreesColumnName="dv")

                climaticStationsDataFrame.setDefaultHourlyDataSeason(columnName="season", defaultSeason="1900_1901")
                climaticStationsDataFrame.setDefaultHourlyWarmingKPI(columnNamePrefix="gdd")
                climaticStationsDataFrame.setDefaultHourlyChillingKPI(columnNamePrefix="chillingDD")


                logger.debug("preprocessDailyColumns :" + estacion)
                horaMinTime=climaticStationsDataFrame.preprocessDailyColumns(force=force)

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                logger.debug("calculateDailyTemperatureKPIs :" + estacion)
                climaticStationsDataFrame.calculateHourlyTemperatureKPIs(force=force)
                
                climaticStationsDataFrame.calculateHourlyCumulativeColumns(force=force, seasonColumnName="season", 
                    destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
                    dateColumnName="fecha", sortingColumnName="timeStamp", stationColumnName="estacion", cumSum=0.0)

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                climaticStationsDataFrame.calculateDailyTemperatureKPIs(force=force, warmingFractionPrefix="warmingFraction",
                                    destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
                                    seasonColumnName="season", stationColumnName="estacion", cumSum=0.0, 
                                    maxWarmingThreshold=parameters.getParameter("maxWarmingThresHold"))
                logger.debug("calculateDailyTemperatureKPIs :" + estacion)
            #    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                climaticStationsDataFrame.calculateDailyCumulativeColumns(force=force, seasonColumnName="season", 
                    destinationColumnNamePrefix=["gdd","chillingDD","rad","precip", "winkler"],
                    dateColumnName="fecha", sortingColumnName="fecha", stationColumnName="estacion", cumSum=0.0)

                climaticStationsDataFrame.initializeDailyWindyColumns(force = force, destinationColumnNamePrefix="wind", cumSum=0.0,
                    windDirections=["N","NE","E","SE","S","SW", "W", "NW"])


            #    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                seasons = climaticStationsDataFrame.determineSeasonsToBeProcessed(climaticStationsDataFrame.dailyObservationsDF, seasonColumnName="season")
                stations = climaticStationsDataFrame.determineStationsToBeProcessed(climaticStationsDataFrame.dailyObservationsDF, stationColumnName="estacion")
                logger.debug("calculateDailyTemperatureKPIs :" + estacion)

                climaticStationsDataFrame.cummulateDailyWindyColumns( force=force, destinationColumnNamePrefix="wind", seasons=seasons, stations=stations,   
                        groupingColumns=["estacion","fecha","windDirection"], columnNames=["vv", "hourFrac"])


                minDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')
                climaticStationsDataFrame.calculateDailySeasonDay(force=force, stations=stations, minDate=minDate,
                        filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"], newcolumnsName=["SeasonDay"])

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                climaticStationsDataFrame.calculateHourlySeasonDay(force=force, stations=stations, minDate=minDate,
                        filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"], newcolumnsName=["SeasonDay"])

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                hourlyObservationStartDate, dailyObservationStartDate = climaticStationsDataFrame.cleanPreviousSeasonDataFromDataSets(startYear)

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                #deleted = climaticStationsDataFrame.deleteDataToBeUpdated(hourlyObservationStartDate, dailyObservationStartDate, startYear, connectionData)

                # climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

                #climaticStationsDataFrame.insertClimaticDataUpdates(if_exists, connectionData)
            
            else:

                raise ("There is not any data of " + str(estacion) + " from the get_past_meteorological function")

        except Exception as e:
            logger.error('Error {} processing station {}. The process can continue, but beware if this error happens too many times. Details: {}'
                .format(str(type(e)), estacion, sys.exc_info()))#str(e)))
            #raise ("Error calculating the winkler " + str(e))
    return 0

executeWickle(cf.inicio,cf.inicioYear)