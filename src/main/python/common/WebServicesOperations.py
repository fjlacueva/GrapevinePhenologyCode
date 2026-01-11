import requests

def createUrl(longitude,latitude,past_days):

    url_all_data = 'https://api.open-meteo.com/v1/forecast?latitude=' + str(latitude) + '&longitude=' + str(
        longitude) + '&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,pressure_msl,precipitation,weathercode,snow_depth,freezinglevel_height,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,evapotranspiration,vapor_pressure_deficit,windspeed_10m,windspeed_80m,windspeed_120m,windspeed_180m,winddirection_10m,winddirection_80m,winddirection_120m,winddirection_180m,windgusts_10m,soil_temperature_0cm,soil_temperature_6cm,soil_temperature_18cm,soil_temperature_54cm,soil_moisture_0_1cm,soil_moisture_1_3cm,soil_moisture_3_9cm,soil_moisture_9_27cm,soil_moisture_27_81cm&daily=weathercode,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,sunrise,sunset,precipitation_sum,precipitation_hours,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum&timezone=Europe%2FLondon&past_days=' + str(
        past_days)
    return url_all_data

def createUrlConcreteColumns(longitude,latitude,past_days):

    url_all_data = 'https://api.open-meteo.com/v1/forecast?latitude=' + str(latitude) + '&longitude=' + str(
        longitude) + '&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,precipitation,direct_radiation,windspeed_10m,winddirection_10m&past_days=' + str(
        past_days)
    return url_all_data

def getValues(url):

    answer = requests.get(url)
    return answer.json()