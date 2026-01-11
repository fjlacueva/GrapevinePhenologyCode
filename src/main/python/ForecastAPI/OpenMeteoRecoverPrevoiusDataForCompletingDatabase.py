import config as cf
from BBDD import Postrest
from requests.auth import HTTPBasicAuth
from common import WebServicesOperations as ws
from common import Fechas as f
import requests
import log.log as l
import datetime
import pandas as pds
import psycopg2
import urllib3
import time

def getForecast(logger,years,connection_db):

    logger.info("Programa empezado")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    dates_predictions = cf.file_system_prediction_2
    file = open(dates_predictions, "r")
    dateFile = file.read()
    file.close()
    differentDays = dateFile.split('\n')
    dateToday = datetime.datetime.now().date()
    dateToday = str(dateToday)
# TODO preguntar a Iñigo que quería hacer aquí porque aquí se sale y no hace nada más.
#    if dateToday not in differentDays:
    if dateToday  in differentDays:
        # print("El almacenaje de predicciones está realicado en estas fechas")
        logger.info("El almacenaje de predicciones está realizado en estas fechas")
        return 0
    else:
        file = open(dates_predictions, "a")
        dateFile = file.write('\n' + dateToday)
        file.close()

    basic = HTTPBasicAuth(username=cf.artemisaNGinx_user, password=cf.artemisaNGinx_password)
    connection = Postrest.getConnection(cf.artemisaNGinx_url, basic)
    query = 'select max(id) as maximo from estimate_arahorario;'
    index = pds.read_sql(query, connection_db)# conexion.commit done
    connection_db.commit()
    try:
        index = int(index['maximo'][0]) + 1
    except Exception as e:
        index = 1
   
    if connection == 1:
        logger.debug("Conexión a BBDD correcta")
        url = cf.artemisaNGinx_url + cf.aragonstationstable
        try:
            response = requests.get(url, auth=basic, verify=False)
            stations = response.json()
            logger.debug("Estaciones recuperadas")
            date = ""
            weatherforecast = ""
            #stations = [{"indicativo": "HU01", "long": -0.147890, "lat": 41.531500}]
            
            for station in stations:
                if stations[0]['indicativo'] != 'Z28':
                    
                    #years = [2022]
                    for year in years:
                        try:

                            logger.debug("estacion empezado lon " + str(station[cf.columnNameLon]) + " lat " + str(
                                station[cf.columnNameLat]))
                            long = station[cf.columnNameLon]
                            lat = station[cf.columnNameLat]
                            indicativo = station[cf.indicativo]

                            # La siguiente línea se tiene que borrar cuando funcione con un año.                        
                            
                            query = 'SELECT "date", indicativo, samples  FROM public.gen_dateswithlesssamplesforstations where date_part(' + "'" + 'YEAR' + "'" + ', "date") = ' + str(
                                year) + ' and upper(indicativo) = upper(' + "'" + str(
                                indicativo) + "')" + ' order by "date" desc, indicativo desc;'

                            logger.debug(query)
                            
                            dataFrameSamples = pds.read_sql(query, connection_db)# conexion.commit done
                            connection_db.commit()

                            if len(dataFrameSamples.values) > 0:

                                for row in dataFrameSamples.values:
                                    start_date = str(row[0])
                                    end_date = row[0] + datetime.timedelta(days=1)
                                    time.sleep(0.02)
                                    url = ws.createUrlConcreteColumnsPreviousData(long, lat, start_date, end_date)
                                    results = ws.getValues(url)
                                    hoursData = results[cf.hours]
                                    windspeed = hoursData[cf.windspeed]
                                    if windspeed[0] != None:
                                        query = 'SELECT * from public.arahorario where indicativo = ' + "'" + str(
                                            indicativo) + "'" + ' and fecha = ' + "'" + str(start_date) + "'" + ';'
                                        logger.debug(query)
                                        dataFrameAragonTiming = pds.read_sql(query, connection_db);# conexion.commit done
                                        connection_db.commit()
                                        emptyDays = []
                                        emptyHours = []
                                        if len(dataFrameAragonTiming.values) > 0:
                                            # The following code controls if it has half time and which hour are not introduce.
                                            halfTime = False
                                            for element in dataFrameAragonTiming.values:
                                                hours = element[cf.index_hour]
                                                minutes = str(hours)[3:5]
                                                if minutes == '30':
                                                    halfTime = True
                                            if halfTime == True:
                                                # The following function finds the empty hours.
                                                allTimes = 0
                                                for i in range(0, 24):
                                                    halfTimeValues = ['00', '30']
                                                    for j in halfTimeValues:
                                                        if i < 10:
                                                            TimeValue = '0' + str(i) + ':' + j + ':00'
                                                        else:
                                                            TimeValue = str(i) + ':' + j + ':00'
                                                        encontrado = False
                                                        for element in dataFrameAragonTiming.values:
                                                            day = element[cf.index_day]
                                                            hours = element[cf.index_hour]
                                                            hora = str(hours)
                                                            if TimeValue == hora:
                                                                encontrado = True
                                                                break
                                                        if encontrado == False:
                                                            emptyDays.append(str(day))
                                                            emptyHours.append(TimeValue)
                                            else:
                                                # The following function finds the empty hours.
                                                allTimes = 0
                                                for i in range(0, 24):
                                                    encontrado = False
                                                    for element in dataFrameAragonTiming.values:
                                                        day = element[cf.index_day]
                                                        hours = element[cf.index_hour]
                                                        hora = int(str(hours)[0:2])
                                                        if i == hora:
                                                            encontrado = True
                                                            break
                                                    if encontrado == False:
                                                        emptyDays.append(str(day))
                                                        emptyHours.append(hora)

                                            positionHour = 0

                                            for emptyHour in emptyHours:

                                                '''
                                                
                                                Preguntar a Francisco como guardamos la información en BBDD. ¿Sacamos la media de la horas puntuales anterior y actual?. Porque uno tenemos en hora y media y el otro
                                                da, los valores en punto, sólo.
                                                
                                                '''

                                                position = 0

                                                for data in hoursData['time']:
                                                    hourData = str(data)[11:] + ":00"
                                                    hourDataOnly = str(data)[11:13]
                                                    hourEmpty = emptyHour[0:2]
                                                    if (hourData == str(emptyHour) or (hourDataOnly == hourEmpty)):

                                                        query = "SELECT count(*) as contar from estimate_arahorario where fecha = '" + emptyDays[positionHour] + "' and hora = '" + emptyHour + "' and indicativo = '" + indicativo + "';"
                                                        #print(query)
                                                        dataCount = pds.read_sql(query, connection_db)# conexion.commit done
                                                        connection_db.commit()

                                                        if dataCount['contar'][0] == 0:

                                                            # Comprobar si lo nombres de las columnas concuerdan
                                                            windspeed = hoursData[cf.windspeed][position]
                                                            timing = hoursData[cf.time][position]
                                                            winddirection = hoursData[cf.winddirection][position]
                                                            if winddirection == None:
                                                                winddirection = "0"
                                                            dewpoint = hoursData[cf.dewpoint][position]
                                                            relativehumidity = hoursData[cf.relativehumidity][position]
                                                            precipitation = hoursData[cf.precipitation][position]
                                                            directradiation = hoursData[cf.directradiation][position]
                                                            temperature = hoursData[cf.temperature][position]
                                                            try:
                                                                # Faltaría la inserción en la BBDD.
                                                                data = {"id": index, "indicativo": indicativo, "ubi": None,
                                                                        "año": year, "dia": int(emptyDays[positionHour][8:]),
                                                                        "fecha": emptyDays[positionHour], "horamin": 0,
                                                                        "tmed": temperature, "hr": relativehumidity,
                                                                        "precip": precipitation/2, "vv": windspeed,
                                                                        "dv": int(winddirection), "humhoja": None,
                                                                        "presion": None, "rad": directradiation/2,
                                                                        "h30": None, "t30": None, "inso": None, "bat": None,
                                                                        "hora": emptyHour, "stimationproviderid": 1}
                                                                logger.debug(str(data))
                                                                Postrest.insertRow(cf.artemisaNGinx_url, cf.store_table,
                                                                                   data, basic)
                                                                index = index + 1

                                                            except Exception as e:
                                                                logger.error(
                                                                    "Error en la ejecución de la recuperación de de datos antiguos de las estaciones " + str(
                                                                        e))
                                                                logger.error(str(e))

                                                        break

                                                    position = position + 1

                                            positionHour = positionHour + 1
                                        else:
                                            index_all = 0
                                            for hour in hoursData['time']:
                                                # Comprobar si lo nombres de las columnas concuerdan
                                                windspeed = hoursData[cf.windspeed][index_all]
                                                if windspeed == None:
                                                    windspeed = "None"
                                                timing = hoursData[cf.time][index_all]
                                                if timing == None:
                                                    timing = "None"
                                                winddirection = hoursData[cf.winddirection][index_all]
                                                if winddirection == None:
                                                    winddirection = "0"
                                                dewpoint = hoursData[cf.dewpoint][index_all]
                                                if dewpoint == None:
                                                    dewpoint = "None"
                                                relativehumidity = hoursData[cf.relativehumidity][index_all]
                                                if relativehumidity == None:
                                                    relativehumidity = "None"
                                                precipitation = hoursData[cf.precipitation][index_all]
                                                if precipitation == None:
                                                    precipitation = "None"
                                                directradiation = hoursData[cf.directradiation][index_all]
                                                if directradiation == None:
                                                    directradiation = "None"
                                                temperature = hoursData[cf.temperature][index_all]
                                                if temperature == None:
                                                    temperature = "None"
                                                try:
                                                    # Faltaría la inserción en la BBDD.
                                                    data = {"id": index, "indicativo": indicativo, "ubi": None,
                                                            "año": hour.split('T')[0].split('-')[0], "dia": hour.split('T')[0].split('-')[2],
                                                            "fecha": hour.split('T')[0], "horamin": 0,
                                                            "tmed": temperature, "hr": relativehumidity,
                                                            "precip": precipitation/2, "vv": windspeed,
                                                            "dv": int(winddirection), "humhoja": None,
                                                            "presion": None, "rad": directradiation/2,
                                                            "h30": None, "t30": None, "inso": None, "bat": None,
                                                            "hora": hour.split('T')[1], "stimationproviderid": 1}
                                                    logger.debug(str(data))
                                                    Postrest.insertRow(cf.artemisaNGinx_url, cf.store_table,
                                                                        data, basic)
                                                    index_all = index_all + 1
                                                    index = index + 1
                                                except Exception as e:
                                                    logger.warning(
                                                        "Error en la ejecución de la recuperación de de datos antiguos de las estaciones " + str(
                                                        e))
                                                    logger.warning(str(e))
                                    else:
                                        pass
                                        #logger.info("Ningún registro encontrado con valores que no sean null " + str(
                                        #    dataFrameSamples.values))

                            else:
                                pass
                                #logger.info("Ningún registro encontrado en la BBDD " + str(dataFrameSamples.values))

                        except Exception as e:
                            logger.warning(
                                "Error en la ejecución de la recuperación de de datos antiguos de una estacion " + str(
                                    e))
                            logger.warning(str(e))
            # return 1
            logger.info("Programa finalizado correctamente")
            exit(0)  # ok

        except Exception as e:
            logger.error("Error en la ejecución de la recuperación de de datos antiguos de las estaciones " + str(e))
            logger.error(str(e))
            exit(-1)
    else:
        logger.error("Conexión a BBDD incorrecta")
        exit(-1)

# logger = l.configLog(cf.loggingPath)
logger = cf.logger

def main(year = None):
    connection_db = psycopg2.connect(user=cf.postgress_Username,
                                      password=cf.postgress_Password,
                                      host=cf.postgress_Host,
                                      port=cf.postgress_Port,
                                      database=cf.postgress_Database)

    if year == None:
        minYear = 2010
    else:
        minYear = year

    currentDateTime = datetime.datetime.now()
    date = currentDateTime.date()
    maxYear = int(date.strftime("%Y"))
    years = range(minYear,maxYear+1)

    try:
        getForecast(logger,years,connection_db)
        connection_db.close()
    except Exception as e:
        logger.error("Error invoking getForecast: " + str(e))
        connection_db.close()
        exit(-2)

main(cf.initialYear)