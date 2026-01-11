from BBDD import Postrest
from common import config as cf
from requests.auth import HTTPBasicAuth
from common import WebServicesOperations as ws
from common import Fechas as f
import requests
import log.log as l
import psycopg2
import pandas as pd
import datetime
import urllib3

user = cf.user
password = cf.password
general_url = cf.general_url
aragon_table_name = cf.aragonstationstable

logger = l.configLog(cf.loggingPath)

def getForecast():

    logger.info("Programa empezado")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    dates_predictions = cf.file_system_prediction
    file = open(dates_predictions, "r")
    dateFile = file.read()
    file.close()
    differentDays = dateFile.split('\n')
    dateToday = datetime.datetime.now().date()
    dateToday = str(dateToday)
    if dateToday in differentDays:
        print("El almacenaje de predicciones está realicado en estas fechas")
        logger.info("El almacenaje de predicciones está realicado en estas fechas")
        return 0
    else:
        file = open(dates_predictions, "a")
        dateFile = file.write('\n' + dateToday)
        file.close()

    basic = HTTPBasicAuth(username=user, password=password)
    connection = Postrest.getConnection(general_url, basic)

    if connection == 1:
        logger.info("Conexión a BBDD correcta")
        url = general_url + aragon_table_name
        try:
            response = requests.get(url, auth=basic, verify=False)
            stations = response.json()
            logger.info("Estaciones recuperadas")
            date = ""
            weatherforecast = ""
            for station in stations:
                try:
                    logger.info("estacion empezado lon " + str(station[cf.columnNameLon]) + " lat " + str(
                        station[cf.columnNameLat]))
                    long = station[cf.columnNameLon]
                    lat = station[cf.columnNameLat]
                    url = ws.createUrlConcreteColumns(long,lat,cf.past_days)
                    results = ws.getValues(url)
                    hours = results[cf.hours]
                    windspeed = hours[cf.windspeed]
                    winddirection = hours[cf.winddirection]
                    dewpoint = hours[cf.dewpoint]
                    relativehumidity = hours[cf.relativehumidity]
                    precipitation = hours[cf.precipitation]
                    directradiation = hours[cf.directradiation]
                    temperature = hours[cf.temperature]
                    timing = hours[cf.time]
                    logger.info("Datos de las variables recuperadas de las estación " + str(station))
                    distinctFechas = f.getDistinctDates(hours[cf.time])
                    keys = list(distinctFechas.keys())
                    min = 0
                    for key in keys:
                        try:
                            # add one row at weatherforecast table
                            data = {'idestacion': station['id'], 'long': long, 'lat': lat, 'forecastdate': key,
                                    'weatherforecastproviderid': cf.data_provider, 'resolutionid': cf.data_resolution}
                            Postrest.insertRow(general_url,cf.weatherforecasttable,data,basic)
                            logger.info(str(data) + " grabado")
                            columns = ['idestacion','long','lat','forecastdate','weatherforecastproviderid','resolutionid']
                            values = [station['id'], long, lat, key, cf.data_provider, cf.data_resolution]
                            weatherforecast = Postrest.getDataTableWithFilter(general_url,cf.weatherforecasttable,columns,values,basic)
                            weatherforecast = weatherforecast.json()
                            code = weatherforecast[0]['id']
                            logger.info(str(code) + " recuperado")
                            quantity = distinctFechas[key]
                            try:
                                for i in range(min,min+quantity):
                                    # add a new row for each hour
                                    actualwindspeed = windspeed[i]
                                    actualwinddirection = winddirection[i]
                                    actualdewpoint = dewpoint[i]
                                    actualrelativehumidity = relativehumidity[i]
                                    actualprecipitation = precipitation[i]
                                    actualradiation = directradiation[i]
                                    actualtemperature = temperature[i]
                                    actualtiming = timing[i]
                                    data = {'idweatherforecast': code, 'forecasttimestamp' : actualtiming, 'temperature' : actualtemperature, 'dewpoint' : actualdewpoint, 'relativehumidity' : actualrelativehumidity, 'winddirection' : actualwinddirection, 'windspeed': actualwindspeed, 'precipitation' : actualprecipitation, 'radiation' : actualradiation}
                                    Postrest.insertRow(general_url, cf.weatherforecastdatatable, data, basic)
                                    logger.info(str(data) + " grabado")

                                min = min + quantity

                            except Exception as e2:
                                logger.error(str(e2))
                                logger.error(str(code))
                                '''try:
                                    for i in range(min,min+quantity):
                                        # add a new row for each hour
                                        actualtiming = timing[i]
                                        try:
                                            Postrest.deleteRow(general_url, cf.weatherforecastdatatable, ['idweatherforecast','forecasttimestamp'], [code,actualtiming], basic)
                                            logger.info(str([code,actualtiming]) + " borrado ")
                                        except Exception as e:
                                            print("Error borrando los registros de la estacion " + str(code) + " en la fecha " + str(actualtiming) + " "  + str(e))
                                            logger.error(str(e))

                                    min = min + quantity

                                except Exception as e:
                                    print("Error en la ejecución de la recuperación de información de las estaciones " + str(e))
                                    logger.error(str(e))'''
                                #return 0

                        except Exception as e:
                            print("Error en la ejecución de la recuperación de información de las estaciones " + str(e))
                            logger.error(str(e))

                except Exception as e:
                    print("Error en la ejecución de la recuperación de las estaciones " + str(e))
                    logger.error(str(e))

            return 1

        except Exception as e:
            print("Error en la ejecución de la recuperación de las estaciones " + str(e))
            logger.error(str(e))
            return 0

    else:
        print("Conexión a BBDD incorrecta")
        logger.info("Conexión a BBDD incorrecta")
        return 0

#getForecast()
