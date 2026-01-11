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
weatherTable = cf.weatherforecasttable

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def getLastDateResolution():

    basic = HTTPBasicAuth(username=user, password=password)
    connection = Postrest.getConnection(general_url, basic)

    if connection == 1:

        url = general_url + "weatherforecast_resolution"
        try:
            response = requests.get(url, auth=basic, verify=False)
            resolutions = response.json()
        except:
            print("error")
            return 0

        url = general_url + "weatherforecast?weatherforecastproviderid=eq.1"
        try:
            response = requests.get(url, auth=basic, verify=False)
            weatherforecast = response.json()
        except:
            print("error")
            return 0

        listResolutions = []
        for resolution in resolutions:
            if resolution['active'] == 'true':
                listResolutions.append(resolution['name'])
        lastWeatherForecast = weatherforecast[len(weatherforecast)-1]['forecastdate']
        date_time_obj = datetime.datetime.strptime(lastWeatherForecast, '%Y-%m-%d')
        date_time_obj += datetime.timedelta(days=1)
        date_time_obj = str(date_time_obj)
        date_time_obj = date_time_obj[0:10]
        dateToday = datetime.datetime.now().date()
        dateToday = str(dateToday)
        valuesToReturn = [listResolutions,date_time_obj,dateToday]
        print(str(valuesToReturn))
        return valuesToReturn

    else:

        print("error en la conexión a BBDD")

results = getLastDateResolution()
resolutions = results[0]
for resolution in resolutions:
    print(str(results[1]) + " " + str(results[2]) + " " + str(resolution))
