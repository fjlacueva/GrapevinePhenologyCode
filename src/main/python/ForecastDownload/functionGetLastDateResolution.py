# from BBDD import Postrest
import sys
import Postrest
import config as cf
from requests.auth import HTTPBasicAuth
import requests
import datetime
import launchDownload
import urllib3
from datetime import datetime, timedelta
from utils import utils

user = cf.artemisaNGinx_user
password = cf.artemisaNGinx_password
general_url = cf.artemisaNGinx_url
weatherTable = cf.weatherforecasttable

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def getLastDateResolution(logger):

    basic = HTTPBasicAuth(username=user, password=password)
    connection = Postrest.getConnection(general_url, basic)

    if connection == 1:

        url = general_url + "weatherforecast_resolution"
        try:
            response = requests.get(url, auth=basic, verify=False)
            resolutions = response.json()
        except:
            logger.error('weatherforecast_resolution requests has executed incorrectly')
            return 0

        url = general_url + "weatherforecast?weatherforecastproviderid=eq.1"
        #try:
        #    response = requests.get(url, auth=basic, verify=False)
        #    weatherforecast = response.json()
        #except:
        #    logger.error('weatherforecast?weatherforecastproviderid=eq.1 requests has executed incorrectly')
        #    return 0

        listResolutions = []
        for resolution in resolutions:
            if resolution['active'] == 'true':
                listResolutions.append(resolution['name'])
        #lastWeatherForecast = weatherforecast[len(weatherforecast)-1]['forecastdate']
        #date_time_obj = datetime.datetime.strptime(lastWeatherForecast, '%Y-%m-%d')
        #date_time_obj += datetime.timedelta(days=1)
        #date_time_obj = str(date_time_obj)
        #date_time_obj = date_time_obj[0:10]
        nDays=utils.tryGetVariable(cf, 'number_days_agroapps', 3)
        yesterday = datetime.now() - timedelta(nDays)
        date_time_obj = datetime.strftime(yesterday, '%Y%m%d')
        dateToday = datetime.now().date()
        dateToday = str(dateToday)
        valuesToReturn = [listResolutions,date_time_obj,dateToday.replace('-','')]
        return valuesToReturn

    else:
        logger.error('Database connection is incorrect')
        return 0

logger=cf.logger
results = getLastDateResolution(logger)
if results != 0:
    launchDownload.d.invalidThreddsURLS=[]
    launchDownload.d.threddsURLContents=[]
    resolutions = results[0]
    for resolution in resolutions:    
        logger.info( 'Invoking loadDataAgroappsData({},{},{})'.format(str(results[1]), str(results[2]),str(resolution)))
        launchDownload.loadDataAgroappsData(dateFromStr=results[1], endDateStr=results[2], resolution=str(resolution))
        logger.info( 'Finished loadDataAgroappsData({},{},{})'.format(str(results[1]), str(results[2]),str(resolution)))
else:
    sys.exit(-1)