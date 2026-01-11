import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
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
    phenologicalStatesMapping=r'C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\OwnCloud\Conf\20200910_PhenologicalStatesMapping.xlsx'
else:
    excelFolder = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/ClimaticStationObservation/SIAR'
    SIARstationGISdata = r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/ControlParcels&ClimaticStationGIS/20200604_Climatic_And_Parcel_Data.xlsx'
    controlParcelsFile= r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/ControlParcels&ClimaticStationGIS/20200821_ParcelasControl.xlsx'
    redFARAHistoricPhenologyFile=r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Phenology/RedFARA/fenología_vid.csv'
    phenologicalStatesMapping=r'/data/PRO19_0383_GRAPEVINE/OwnCloud/Conf/20200910_PhenologicalStatesMapping.xlsx'

kindOfClimaticData = 'Horarios'

#originDenominations= ["Calatayud", "Campo de Borja", "Cariñena", "Somontano"]
originDenominations= ["Campo de Borja"]

#filter={"ProvID":"50", "OD":"Calatayud"}
#filter={"ProvID":"50", "OD":"Calatayud", "MunID":"29", "Poligono":"8", "Parcela":"166"}
#filter={"ProvID":"50", "OD":"Campo de Borja"}
filter={}



##################################################################################################
#  Initializing  PhenologyDataFrame Mandatory Parameters
#
phenologyMandatoryparameters = Parameters()
phenologyMandatoryparameters.setParameter(name="dataDriver", value="localDisk")
# phenologyMandatoryparameters.setParameter(name="controlParcelFile", 
#     value=r"C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\DatosAtrias\Históricos Climáticos\20200821_ParcelasControl.xlsx")
phenologyMandatoryparameters.setParameter(name="controlParcelFile", value=controlParcelsFile)
# phenologyMandatoryparameters.setParameter(name="redFaraPhenologyFile", 
#     value=r"C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\RedFara\fenología_vid.csv")
phenologyMandatoryparameters.setParameter(name="redFaraPhenologyFile", value=redFARAHistoricPhenologyFile)
phenologicalStatesMapping
# phenologyMandatoryparameters.setParameter(name="phenologicalStateMappingFile", 
#     value=r"C:\Users\fjlacueva\PRO19_0383_GRAPEVINE\Data\DatosAtrias\Históricos Climáticos\ ")
phenologyMandatoryparameters.setParameter(name="phenologicalStateMappingFile", value=phenologicalStatesMapping)
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

# Initializing classes
phenologyDataFrame = PhenologyDataFrame(parameters=phenologyMandatoryparameters)
#phenologyDataFrame.readControlParcelsData()

# Reading preprocessed data
force = False

phenologyDataFrame.readPhenologicalStateMapping()
phenologyDataFrame.getParcelsCoordinates( force=force, filterParcels=filter,
        keyParcelColumns=["Country", "ProvID", "MunID", "Poligono", "Parcela"],  keyDBColumn={"codigo"},
        sortingColumnName="Localidad")
phenologyDataFrame.getParcelClimaticData( force=force,  parcelFilter=filter,
        keyParcelColumns=["Country", "OD", "ProvID", "MunID", "Poligono", "Parcela", "cadastralCode",
        "longitude","latitude","altitude", "Variedad", "Especie"], 
        keyPhenologyColumns=["codigo", "greatest", "especie", "variedad", "plaga_nombre", "greatest_min"], 
        sortingColumnName="cadastralCode",
        climaticStationsDataFrame=None) # Not required if data is not (re)created

columToLookForCorrelation = ["tmed_min", "tmed_max", "tmed_mean", "rad_min", "rad_max", "rad_mean", 
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
                "wind_SW", "wind_W", "wind_NW", "SeasonDay_t0", "SeasonDay_1", "SeasonDay_2",  
                "stationAltitude",  "variedad", "grapevineState",  "Parcel_altitude"]


columToLookForCorrelation = [
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
                "wind_SW", "wind_W", "wind_NW", "SeasonDay_t0", "SeasonDay_1", "SeasonDay_2",  
                "stationAltitude",  "variedad", "grapevineState",  "Parcel_altitude"]


consideredSeasons = ["2016_2017", "2017_2018", "2018_2029"]
#consideredPhenologicalStates =[1,2,3,4]
consideredPhenologicalStates =[5,6,7,8,9,10,11,12,13,14,15,16]
#consideredPhenologicalStates =[]
#normalizationMethod = "min_max_scaler"
normalizationMethod = None
notNormalizedColumns = ["season", "SeasonDay_t0", "SeasonDay_1", "SeasonDay_2", "variedad", "grapevineState"]
correlationMatrix = phenologyDataFrame.getProcessedPhenologyCorrelationMatrix(
            selectedFields=columToLookForCorrelation,
            consideredSeasons = consideredSeasons,
            consideredPhenologicalStates =consideredPhenologicalStates,
            normalizationMethod = normalizationMethod,
            notNormalizedColumns = notNormalizedColumns)

sm.graphics.plot_corr(correlationMatrix, xnames=columToLookForCorrelation)
plt.show()