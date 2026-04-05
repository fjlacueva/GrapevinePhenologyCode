import pandas as pd
import os
import platform
from datetime import date
from datetime import datetime as dtDT
from Tools.Parameters import Parameters
from HandlingDataFrames.ClimaticStationDataFrame import ClimaticStationDataFrame

if platform.system()=="Windows":
    excelFolder = r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\ClimaticStationObservation\SIAR'
    SIARstationGISdata = r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\ClimaticStationObservation\SIAR\StationGIS\20200604_Climatic_And_Parcel_Data.xlsx'

else:
    excelFolder = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/ClimaticStationObservation/SIAR'
    SIARstationGISFile = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/ClimaticStationObservation/SIAR/StationGIS/20200604_Climatic_And_Parcel_Data.xlsx'

kindOfClimaticData = 'Horarios'

force = False
originDenominations= ["Calatayud", "Campo de Borja", "Cariñena", "Somontano"]
#originDenominations= ["Somontano"]

for od in originDenominations:
    parameters = Parameters()
    parameters.setParameter(name="dataDriver", value="localDisk")

    if (excelFolder.endswith(os.path.sep)):
        odExcelFolder = excelFolder + od
    else: 
        odExcelFolder = excelFolder + os.path.sep + od

    parameters.setParameter(name="diaryObservationsPathsAndPatterns", value=odExcelFolder+";"+kindOfClimaticData)

# parameters = Parameters()
# parameters.setParameter(name="dataDriver", value="localDisk")

# parameters.setParameter(name="diaryObservationsPathsAndPatterns", value=excelFolder+";"+kindOfClimaticData)

    parameters.setParameter(name="stationsGISPathDefinitions", 
        value=SIARstationGISdata)

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
    #df= climaticStationsDataFrame.createEmptyClimaticStationDataFrame()  
    climaticStationsDataFrame.readStationGIS()
    climaticStationsDataFrame.convertStatonGIStoDecimal()

    horaMinTime=climaticStationsDataFrame.preprocessHourlyColumns(force=force)

    horaMinTime=climaticStationsDataFrame.preprocessDailyColumns(force=force)

    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

    climaticStationsDataFrame.calculateHourlyTemperatureKPIs(force=force)

    climaticStationsDataFrame.calculateHourlyCumulativeColumns(force=force, seasonColumnName="season", 
        destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
        dateColumnName="fecha", sortingColumnName="timeStamp", stationColumnName="estacion", cumSum=0.0)

    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

    climaticStationsDataFrame.calculateDailyTemperatureKPIs(force=force, warmingFractionPrefix="warmingFraction",
                        destinationColumnNamePrefix=["gdd","chillingDD","rad","precip"],
                        seasonColumnName="season", stationColumnName="estacion", cumSum=0.0, 
                        maxWarmingThreshold=30.0)

    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

    climaticStationsDataFrame.calculateDailyCumulativeColumns(force=force, seasonColumnName="season", 
        destinationColumnNamePrefix=["gdd","chillingDD","rad","precip", "winkler"],
        dateColumnName="fecha", sortingColumnName="fecha", stationColumnName="estacion", cumSum=0.0)

    climaticStationsDataFrame.initializeDailyWindyColumns(force = force, destinationColumnNamePrefix="wind", cumSum=0.0,
        windDirections=["N","NE","E","SE","S","SW", "W", "NW"])


    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

    seasons = climaticStationsDataFrame.determineSeasonsToBeProcessed(climaticStationsDataFrame.dailyObservationsDF, seasonColumnName="season")
    stations = climaticStationsDataFrame.determineStationsToBeProcessed(climaticStationsDataFrame.dailyObservationsDF, stationColumnName="estacion")

    climaticStationsDataFrame.cummulateDailyWindyColumns( force=force, destinationColumnNamePrefix="wind", seasons=seasons, stations=stations,   
            groupingColumns=["estacion","fecha","windDirection"], columnNames=["vv", "hourFrac"])


    minDate = dtDT.strptime("1900-1-1", '%Y-%m-%d')
    climaticStationsDataFrame.calculateDailySeasonDay(force=force, stations=stations, minDate=minDate,
            filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"], newcolumnsName=["SeasonDay"])

    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")

    climaticStationsDataFrame.calculateHourlySeasonDay(force=force, stations=stations, minDate=minDate,
            filterDailyColumns = ["estacion","fecha"], groupByColumns= ["estacion","season"], newcolumnsName=["SeasonDay"])

    climaticStationsDataFrame.copyStationGIS2Observations(force=True)

    climaticStationsDataFrame.copyStationGIS2HourlyObservations(force=True)


    climaticStationsDataFrame.saveDFs(typeOfSave="csv", fileSeparator=";")


    print(climaticStationsDataFrame.dailyObservationsDF.columns)