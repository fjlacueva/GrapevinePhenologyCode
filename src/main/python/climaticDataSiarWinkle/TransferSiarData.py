from re import S
import pandas as pd
import numpy as np
import psycopg2
import os
import config as cf

from sqlalchemy import create_engine

from HandlingDataFrames.ClimaticStationDataFrame import ClimaticStationDataFrame
from Parameters import Parameters
import sys
import datetime
from datetime import datetime as dtDT
from datetime import date
import log as l

#logger= cf.logger
logger = l.configLog(cf.realClimateDataCalculus_loggingPath)

def add_hours(fecha, minutes_sconds):
    return fecha + pd.DateOffset(hours=float(minutes_sconds//100), minutes= float(minutes_sconds%100))


def get_past_meteorological(estaciones=[], startYear:int=None, last=False):
    conexion = psycopg2.connect(database=cf.postgress_Database, #meteorological_past_database, 
                                user=cf.postgress_Username,#meteorological_past_user, 
                                password=cf.postgress_Password,#meteorological_past_password, 
                                host=cf.postgress_Host,#meteorological_past_host, 
                                port=cf.postgress_Port)#meteorological_past_port)
    '''
    last: Turned True if we only want the last values of each station (for prediction). If the full database is 
    desired use last='False'.
    '''
    dataToReturn = []
    cur = conexion.cursor()

    
    whereStr = ""
    query_Template = "select * "
    # query_Template = query_Template + "from public.datoshorarios dh inner join public.estaciones e on e.idprovincia=dh.idprovincia "
    # query_Template = query_Template + "where e.idestacion=dh.idestacion "
    query_Template = query_Template + "FROM public.gv_aragoneseclimaticstationsdata acsd"

    if last:
        # query_Template = query_Template + " AND fecha>= (CURRENT_DATE-21) "
        query_Template = query_Template 
    if not estaciones is None and  len (estaciones)> 0:
        lista_estaciones = "("
        for estacion in estaciones:
            if len(lista_estaciones)> 1:
                lista_estaciones = lista_estaciones + ", "
            lista_estaciones = lista_estaciones + "'" + estacion + "'"
        lista_estaciones = lista_estaciones + ")"
        whereStr = " where acsd.nombrecorto in  " + lista_estaciones

    if last:
        if len(whereStr)==0:
            whereStr = " where "
        else:
            whereStr = whereStr + " and"
        whereStr = whereStr + " acsd.fecha>= (CURRENT_DATE-21) "
    
    if not startYear is None :
        current_year = date.today().year

        if startYear > 2000 and startYear<= current_year:
            #Todo look for the minus starting date of the sessions (to aggregate from then)
            startDate = datetime.datetime(startYear, 1, 1)
            startDateStr = startDate.strftime('%Y-%m-%d')
            if len(whereStr)==0:
                whereStr = " where "
            else:
                whereStr = whereStr + " and"
            whereStr = whereStr + " acsd.fecha>=  '" + startDateStr + "'"



    query_Template = query_Template + whereStr
    query_Template = query_Template + " order by acsd.idestacion, acsd.fecha"
    # if last:
    #     query="select * from public.datoshorarios dh inner join public.estaciones e on e.idprovincia=dh.idprovincia where e.idestacion=dh.idestacion AND fecha>= (CURRENT_DATE-21)"
    # else:
    #     query="select * from public.datoshorarios dh inner join public.estaciones e on e.idprovincia=dh.idprovincia where e.idestacion=dh.idestacion"
    query = query_Template

    data = pd.read_sql_query(query, con=conexion)# conexion.commit done
    conexion.commit()
    conexion.close()
        
    data['fecha']=data.apply(lambda row: add_hours(row['fecha'],row['horamin']),axis=1)
    
    data=data[['fecha', 'nombrecorto', 'idprovincia', 'idestacion', 'tempmedia', 'humedadmedia', 'velviento', 'dirviento', 
               'radiacion', 'precipitacion', 'tempmediacaja', 'tempsuelo1', 'tempsuelo2', 'codtempsuelo2', 'xutm', 'yutm', 
               'huso', 'altitud', 'latitude', 'longitude', 'horamin']]
    # data['latitude']=[utm.to_latlon(list(element)[0],list(element)[1],list(element)[2],list(element)[3]) [1]
    #  for element in zip(data['xutm'], data['yutm'], data['huso'], ['U']*len(data))]
    # data['longitude']=[utm.to_latlon(list(element)[0],list(element)[1],list(element)[2],list(element)[3]) [0]
    #  for element in zip(data['xutm'], data['yutm'], data['huso'], ['U']*len(data))]
    
    return data

def transferSiarData(estaciones,startYear):

    datos_meteo_past=get_past_meteorological(estaciones=estaciones, startYear= startYear, last=False)
    datos_meteo_past['corrected']=0
    busca=datos_meteo_past[datos_meteo_past.nombrecorto=='Z05'].sort_values('fecha').set_index('fecha')
    datos_extra=busca[(busca.index>pd.to_datetime('01/01/2018Z00:00:00',format='%d/%m/%YZ%H:%M:%S'))&(busca.index<pd.to_datetime('01/01/2019Z00:00:00',format='%d/%m/%YZ%H:%M:%S'))]
    datos_extra['nombrecorto']='Z01'
    datos_extra['corrected']=1
    datos_meteo_past=pd.concat([datos_meteo_past,datos_extra.reset_index()])
    datos_meteo_past[datos_meteo_past.humedadmedia<0]['corrected']=2
    datos_meteo_past[datos_meteo_past.humedadmedia<0]['humedadmedia']=0
    datosDailyList=[]
    datosHourlyList=[]

    if_exists='replace'

    for estacion in datos_meteo_past.nombrecorto.unique():
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

        odExcelFolder = cf.realClimateDataCalculus_odExcelFolder
        SIARstationGISFile = cf.realClimateDataCalculus_SIARstationGISFile
        kindOfClimaticData = 'Horarios'

        parameters.setParameter(name="diaryObservationsPathsAndPatterns", value=odExcelFolder+";"+kindOfClimaticData)

        parameters.setParameter(name="stationsGISPathDefinitions", 
            value=SIARstationGISFile)

        parameters.setParameter(name="timeZeros", value ="t0;1;2")

        parameters.setParameter(name="warmingThresHoldArray", value ="4.5;10.0")
        parameters.setParameter(name="maxWarmingThresHold", value =30.0)
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



        horaMinTime=climaticStationsDataFrame.preprocessDailyColumns(force=force)
        logger.debug("calculateDailyTemperatureKPIs :" + estacion)

        logger.debug("preprocessDailyColumns :" + estacion)

    #   climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

        climaticStationsDataFrame.calculateHourlyTemperatureKPIs(force=force)

        climaticStationsDataFrame.calculateHourlyCumulativeColumns(force=force, seasonColumnName="season", 
            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
            dateColumnName="fecha", sortingColumnName="timeStamp", stationColumnName="estacion", cumSum=0.0)

        climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

        climaticStationsDataFrame.calculateDailyTemperatureKPIs(force=force, warmingFractionPrefix="warmingFraction",
                            destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
                            seasonColumnName="season", stationColumnName="estacion", cumSum=0.0, 
                            maxWarmingThreshold=30.0)
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

    #    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

        climaticStationsDataFrame.calculateHourlySeasonDay(force=force, stations=stations, minDate=minDate,
                filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"], newcolumnsName=["SeasonDay"])

        climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")
        logger.debug("saveDFs :" + estacion)

        # datosDailyList.append(climaticStationsDataFrame.dailyObservationsDF)

        # datosHourlyList.append(climaticStationsDataFrame.observationsDF)
        #     climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

        climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

        engine = create_engine('postgresql://' + cf.postgress_Username + ':' + cf.postgress_Password + '@' + cf.postgress_Host + ':' + str(cf.postgress_Port) + '/' + cf.postgress_Database)
        climaticStationsDataFrame.dailyObservationsDF.to_sql('MeteorologicalDailyData', engine, schema='ITAINNOVA', if_exists=if_exists,
                                                            index=False, chunksize=1000, dtype=None, method=None)
        climaticStationsDataFrame.observationsDF.to_sql('MeteorologicalHourlyData', engine, schema='ITAINNOVA', if_exists=if_exists,
                                                            index=False, chunksize=1000, dtype=None, method=None)
        if_exists='append'                                                            
        engine.dispose()
        

    # conexion = psycopg2.connect(database=db_database, 
    #                                 user=db_user, 
    #                                 password=db_password, 
    #                                 host=db_host, 
    #                                 port=db_port)

    # engine = create_engine('postgresql://' + db_user + ':' + db_password + '@' + db_host + ':' + str(db_port) + '/meteo')
    # # climaticStationsDataFrame.dailyObservationsDF.to_sql('meteorological_data', engine, schema='public', if_exists='replace',
    # #                                                      index=False, chunksize=1000, dtype=None, method=None)
    # dailyDF=pd.concat(datosDailyList)
    # dailyDF.to_sql('MeteorologicalDailyData', engine, schema='public', if_exists='replace',
    #                                                      index=False, chunksize=1000, dtype=None, method=None)

    # hourlyDF=pd.concat(datosHourlyList)
    # hourlyDF.to_sql('MeteorologicalHourlyData', engine, schema='public', if_exists='replace',
    #                                                      index=False, chunksize=1000, dtype=None, method=None)

    # engine.dispose()

estaciones = ['HU07', 'Z14', 'Z01', 'Z24', 'Z05', 'HU12', 'TE01', 'TE02', 'TE03', 'Z04', 'Z15', 'Z18', 'Z22', 'Z25', 'Z28']

startYear = 2008

logger.debug('Invoking transferSiarData({},{})'.format(estaciones,startYear))
transferSiarData(estaciones,startYear)