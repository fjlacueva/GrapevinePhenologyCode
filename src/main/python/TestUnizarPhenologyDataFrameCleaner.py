import pandas as pd
import os
import platform
from datetime import date
import json
from Tools.Parameters import Parameters
from HandlingDataFrames.PhenologyDataFrame import PhenologyDataFrame
from HandlingDataFrames.ClimaticStationDataFrame import ClimaticStationDataFrame

if platform.system()=="Windows":
    excelFolder = r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\ClimaticStationObservation\SIAR'
    SIARstationGISdata = r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Conf\ControlParcels&ClimaticStationGIS\20200604_Climatic_And_Parcel_Data.xlsx'
    controlParcelsFile= r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Conf\ControlParcels&ClimaticStationGIS\20200821_ParcelasControl.xlsx'
    redFARAHistoricPhenologyFile=r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Phenology\RedFARA\fenología_vid.csv'
    unizarHistoricPhenologyFile=r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Phenology\Unizar\20201011_fenologiaunizar.csv'
    phenologicalStatesMapping=r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Conf\20200910_PhenologicalStatesMapping.xlsx'
else:
    excelFolder = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/ClimaticStationObservation/SIAR'
    SIARstationGISdata = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/ControlParcels&ClimaticStationGIS/20200604_Climatic_And_Parcel_Data.xlsx'
    controlParcelsFile= r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/ControlParcels&ClimaticStationGIS/20200821_ParcelasControl.xlsx'
    redFARAHistoricPhenologyFile=r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Phenology/RedFARA/fenología_vid.csv'
    unizarHistoricPhenologyFile=r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Phenology/RedFARA/20201011_fenologiaunizar.csv'
    phenologicalStatesMapping=r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/20200910_PhenologicalStatesMapping.xlsx'

kindOfClimaticData = 'Horarios'
csvSeparator=";"

force = True
#originDenominations= ["Calatayud", "Campo de Borja", "Cariñena", "Somontano"]
originDenominations= ["Campo de Borja"]

#filter={"ProvID":"50", "OD":"Calatayud"}
#filter={"ProvID":"50", "OD":"Calatayud", "MunID":"29", "Poligono":"8", "Parcela":"166"}
#filter={"ProvID":"50", "OD":"Campo de Borja"}
filter={}


##################################################################################################
#  Initializing  ClimaticStationDataFrame Mandatory Parameters
#
climaticStationParameters = Parameters()
climaticStationParameters.setParameter(name="dataDriver", value="localDisk")
climaticStationParameters.setParameter(name="diaryObservationsPathsAndPatterns", value=excelFolder+";"+kindOfClimaticData)
climaticStationParameters.setParameter(name="stationsGISPathDefinitions", value=SIARstationGISdata)
climaticStationParameters.setParameter(name="odClimaticStationDataPath", value=excelFolder)
climaticStationParameters.setParameter(name="timeZeros", value ="t0;1;2")
climaticStationParameters.setParameter(name="warmingThresHoldArray", value ="4.5;10.0")
climaticStationParameters.setParameter(name="maxWarmingThresHold", value =30.0)
climaticStationParameters.setParameter(name="warmingHourlyMethods", value ="Tbase;TbaseMax")
climaticStationParameters.setParameter(name="chillingThresHoldArray", value ="7.0")
climaticStationParameters.setParameter(name="minChillingThreshold", value =0.0)
climaticStationParameters.setParameter(name="chillingHourlyMethods", value ="Tbase;Tbasemin;Utah")
climaticStationParameters.setParameter(name="hourlyPreprocessesFileName", value ="PreprocessedHourlyObservations")
climaticStationMandatoryParameters=ClimaticStationDataFrame.mandatoryParameters
climaticStationParameters.addMandatoryParameters(names=climaticStationMandatoryParameters)

##################################################################################################
#  Initializing  PhenologyDataFrame Mandatory Parameters
#
phenologyMandatoryparameters = Parameters()
phenologyMandatoryparameters.setParameter(name="dataDriver", value="localDisk")
phenologyMandatoryparameters.setParameter(name="controlParcelFile", value=controlParcelsFile)
phenologyMandatoryparameters.setParameter(name="redFaraPhenologyFile", value=redFARAHistoricPhenologyFile)
phenologyMandatoryparameters.setParameter(name="unizarPhenologyFile", value=redFARAHistoricPhenologyFile)
phenologyMandatoryparameters.setParameter(name="phenologicalStateMappingFile", value=phenologicalStatesMapping)
phenologyMandatoryparameters.setParameter(name="unizarHistoricPhenologyFile",value=unizarHistoricPhenologyFile)
phenologyMandatoryparameters.setParameter(name="csvSeparator",value=csvSeparator)



query={
        "bbdd":"DatosGeograficos",
        "coleccion":"ParcelaCatastral",
        "query":{"codigo":"<codigo>"},
        "fields": {"codigo": 1, "descripcion":1, "coordenadas_epsgWGS84":1, "rectangleWGS84":1, "coordenadasParcela":1}, 
        "orderField":"descripcion",
        "order":"DESCENDING"
    }
jsonQuery=json.dumps(query)
phenologyMandatoryparameters.setParameter(name="agrolakeAPIQuery" , value=jsonQuery)
mandatoryPhenologyParameters=PhenologyDataFrame.mandatoryParameters
phenologyMandatoryparameters.addMandatoryParameters(names=mandatoryPhenologyParameters)

##################################################################################################
# Initializing classes
#
climaticStationsDataFrame = ClimaticStationDataFrame(parameters=climaticStationParameters)
df= climaticStationsDataFrame.createEmptyClimaticStationDataFrame()
climaticStationsDataFrame.readStationGIS()
climaticStationsDataFrame.convertStatonGIStoDecimal()

phenologyDataFrame = PhenologyDataFrame(parameters=phenologyMandatoryparameters)
phenologyDataFrame.readControlParcelsData()
phenologyDataFrame.readPhenologicalStateMapping()

# Parcel filter 
phenologyDataFrame.getParcelsCoordinates( force = force, filterParcels=filter,
        keyParcelColumns=["Country", "ProvID", "MunID", "Poligono", "Parcela"],  keyDBColumn={"codigo"},
        sortingColumnName="Localidad")
phenologyDataFrame.saveControlParcelsData()


selectingFields:dict={"Comentario": "Unizar", "Considered": "Yes"}
selectingFields:dict={"Comentario": "Unizar"}

phenologyDataFrame.getParcelClimaticData( force=False,  parcelFilter=filter,
        keyParcelColumns=["Country", "OD", "ProvID", "MunID", "Poligono", "Parcela", "cadastralCode",
        "longitude","latitude","altitude", "Variedad", "Especie"], 
        keyPhenologyColumns=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"], 
        sortingColumnName="cadastralCode",
        climaticStationsDataFrame=climaticStationsDataFrame)
phenologyDataFrame.selectUnizarControlParcelsData(selectingFields=selectingFields)
phenologyDataFrame.readUnizarPhenologyRawData()
phenologyDataFrame.reviewUnizarPhenologyRawData(
    keyPhenologyColumns=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"])
phenologyDataFrame.mergeRedFaraUnizarDataFrames()

phenologyDataFrame.savePhenologyRawData()

phenologyDataFrame.getParcelClimaticData( force=True,  parcelFilter=filter,
        keyParcelColumns=["Country", "OD", "ProvID", "MunID", "Poligono", "Parcela", "cadastralCode",
        "longitude","latitude","altitude", "Variedad", "Especie"], 
        keyPhenologyColumns=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"], 
        sortingColumnName="cadastralCode",
        climaticStationsDataFrame=climaticStationsDataFrame)


phenologyDataFrame.saveParcelPhenologicalDataTimeSeriesDF()



print(phenologyDataFrame.unizarControlParcelsDF)
