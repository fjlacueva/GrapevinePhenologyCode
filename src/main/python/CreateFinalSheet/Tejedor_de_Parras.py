import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
from config import *

from datetime import timedelta
from datetime import datetime

import utm
import time


def get_closest(lon,lat,estaciones):
    distancias_list=[]
    cercano=0
    closest=10000000
    for i in range(len(estaciones)):
        dist=np.sqrt((estaciones['long'].iloc[i]-lon)**2+(estaciones['lat'].iloc[i]-lat)**2)
        distancias_list.append(dist)
        if(dist<closest):
            closest=dist
            cercano=estaciones['nombrecorto'].iloc[i]
    estaciones['distancias']=distancias_list
    return cercano


conexion = psycopg2.connect(database=postgress_Database, 
                                user=postgress_Username, 
                                password=postgress_Password, 
                                host=postgress_Host, 
                                port=postgress_Port)

if_exists='append'
if full_sabana==True:
    year_sabana=2015
    if_exists='replace'

query='''select date, variedad, min(phenologystageid) as phenologystageid, codigocatastro as codigo 
from redfara.redfara_fenologia
where UPPER(especie)='VIÑEDO VINIFICACION' and EXTRACT(YEAR FROM date)>='''+str(year_sabana) + '''
group by date, variedad, codigo, phenologystageid;'''
phenological_data = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()
phenological_data['variedad']=phenological_data.variedad.str.upper().replace(dic_variedades_replace)
phenological_data=phenological_data[phenological_data.variedad.isin(variedades)]
phenological_data['year']=phenological_data.date.dt.year
phenological_data['codigo']=[cod if len(cod)==14 else cod[:9]+'0'+cod[9:] for cod in phenological_data.codigo]


query="select a.codigo, replace(cast(a.coordenadas_epsgwgs84 as varchar),'{','') as coordenadas, a.altitud, b.doc_id from cadastral.parcelas a inner join cadastral.gv_controlparcelsgeom b on b.codigo=a.codigo where a.codigo in "+ str(list(phenological_data.codigo.unique())).replace('[','(').replace(']',')') + ';'
cadastral_data = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()
cadastral_data['longitude']=cadastral_data['coordenadas'].apply(lambda x:float(x.split(',')[0]))
cadastral_data['latitude']=cadastral_data['coordenadas'].apply(lambda x:float(x.split(',')[1]))
cadastral_data=cadastral_data[['codigo', 'longitude','latitude','altitud', 'doc_id']].drop_duplicates()
cadastral_data


query='select * from public.datosestacionporyearantiguos sy inner join public.gv_estaragon48 e on sy.indicativo=e.indicativo;'
stations_per_year = pd.read_sql_query(query, con=conexion)# conexion.commit done
conexion.commit()
print(len(stations_per_year))
proportion_days=0.9
proportion_records=0.9
stations_per_year=stations_per_year[(stations_per_year.days>proportion_days*365)&(stations_per_year.totalsamplesperday>proportion_records*365*24)]
stations_per_year=stations_per_year[['indicativo', 'año','alt','long','lat']].T.drop_duplicates().T

query='select * from public.datosestacionporyearactual sy inner join public.gv_estaragon48 e on sy.indicativo=e.indicativo;'
stations_per_year2 = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()
print(len(stations_per_year2))
days=proportion_days*datetime.now().timetuple().tm_yday
records=proportion_records*datetime.now().timetuple().tm_yday*48
stations_per_year2=stations_per_year2[(stations_per_year2.days>days)&(stations_per_year2.totalsamplesperday>records)]
stations_per_year2=stations_per_year2[['indicativo', 'año','alt','long','lat']].T.drop_duplicates().T
stations_per_year=pd.concat([stations_per_year,stations_per_year2])


stations_per_year['nombrecorto']=stations_per_year.indicativo
list_year_stations_used=[]
cadastral_data_year_list=[]
for year in stations_per_year.año.unique():
    stations_per_year_sel=stations_per_year[stations_per_year.año==year]
    cercanias=[]
    cadastral_data_year=cadastral_data.copy()
    for i in range(len(cadastral_data)):
        cercanias.append(get_closest(cadastral_data_year.iloc[i].longitude,cadastral_data_year.iloc[i].latitude,stations_per_year_sel))
    cadastral_data_year['closest']=cercanias
    cadastral_data_year['year']=year
    list_year_stations_used=list_year_stations_used+[[year, est] for est in np.unique(cercanias)]
    cadastral_data_year_list.append(cadastral_data_year)
df_year_stations_used=pd.DataFrame(np.asarray(list_year_stations_used), columns=['year','station'])

engine = create_engine('postgresql://' + postgress_Username + ':' + postgress_Password + '@' + postgress_Host + ':' + str(postgress_Port) + '/meteo')

t = text("DELETE FROM public.grapevine_stations_used WHERE CAST(year AS int)>="+str(year_sabana))
result = engine.connect().execute(t)
df_year_stations_used.to_sql('grapevine_stations_used', engine, schema='public', if_exists=if_exists,
                                                         index=False, chunksize=1000, dtype=None, method=None)
cadastral_data=pd.concat(cadastral_data_year_list)


query='''select a.codigocatastro, b.fecha, b.estacion, b.season
from (
    select *
    from redfara.redfara_fenologia rf
        where  UPPER(rf."especie")='VIÑEDO VINIFICACION'
    order by rf."date"
)  a right outer join (
    select *
    from "ITAINNOVA"."MeteorologicalDailyData" mdd
) b on date(a."date") = b.fecha
where EXTRACT(YEAR FROM b.fecha)>='''+str(year_sabana) + ';'
muleta=pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()
muleta=muleta[~muleta.codigocatastro.isna()]
muleta['year']=[int(seas.split('_')[1]) for seas in muleta.season]
muleta['codigocatastro']=[cod if len(cod)==14 else cod[:9]+'0'+cod[9:] for cod in muleta.codigocatastro]


muleta2=pd.merge(cadastral_data,muleta, left_on=['codigo','year','closest'], 
                 right_on=['codigocatastro','year','estacion'], how='right')
muleta2=muleta2[~muleta2.codigo.isna()]
muleta2['fecha']=pd.to_datetime(muleta2['fecha'])


phenological_data['year']=phenological_data.date.dt.year
lowest_year=phenological_data.year.min()
phenological_data['fecha']=pd.to_datetime(phenological_data.date.dt.date)


datos_part=pd.merge(phenological_data,muleta2, left_on=['fecha','codigo'],right_on=['fecha','codigo'], how='inner')
datos_part=datos_part[['fecha', 'variedad', 'phenologystageid', 'codigo', 'closest', 'longitude','latitude', 'altitud','doc_id']].drop_duplicates()


subdatas_list=[]
for campo in datos_part.codigo.unique():
    subdata=datos_part[datos_part.codigo==campo].drop_duplicates(subset=['fecha', 'codigo']) ## Salen 11 campos duplicados (cambia el estado fenológico)
    subdata['date']=pd.to_datetime(subdata['fecha'],format='%Y-%m-%d')
    subdata['date2']=subdata['date']
    subdata=subdata.sort_values('date').set_index('date').resample('1D').ffill().reset_index()
    subdata['diff']=(subdata['date']-subdata['date2']).dt.days
    subdatas_list.append(subdata[subdata['diff']<=7].drop(columns=['diff','date2']))
datos_part2=pd.concat(subdatas_list)


#lowest_year=2022
query='select * from "ITAINNOVA"."MeteorologicalDailyData" WHERE anio >= ' + str(year_sabana) + ';'
meteorological_data = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()


meteorological_data = meteorological_data.drop(['ubi'], axis=1)


datos_meteo_buenos_list=[]

for estacion in meteorological_data['estacion'].unique():
    datos_est=meteorological_data[meteorological_data.estacion==estacion].sort_values('fecha').set_index('fecha')
    datos_meteo_buenos_est_list=[datos_est[['estacion','season']]]
    for var in variables_diarias_min:
        datos_var_est=datos_est[[var]].resample('1D').min().fillna(method='bfill')
        for i in range(1,n_dias_atras):
            datos_var_est[var + " " + str(i) + "_dias_atras"]=datos_var_est[var].resample('1D').min().fillna(method='bfill').shift(i)
        for i in range(1,n_dias_alante):
            datos_var_est[var + " " + str(i) + "_dias_adelante"]=datos_var_est[var].resample('1D').min().fillna(method='bfill').shift(-i)
        datos_meteo_buenos_est_list.append(datos_var_est)
    for var in variables_diarias_max:
        datos_var_est=datos_est[[var]].resample('1D').max().fillna(method='bfill')
        for i in range(1,n_dias_atras):
            datos_var_est[var + " " + str(i) + "_dias_atras"]=datos_var_est[var].resample('1D').max().fillna(method='bfill').shift(i)
        for i in range(1,n_dias_alante):
            datos_var_est[var + " " + str(i) + "_dias_adelante"]=datos_var_est[var].resample('1D').max().fillna(method='bfill').shift(-i)
        datos_meteo_buenos_est_list.append(datos_var_est)
    for var in variables_diarias_mean:
        datos_var_est=datos_est[[var]].resample('1D').max().fillna(method='bfill')
        for i in range(1,n_dias_atras):
            datos_var_est[var + " " + str(i) + "_dias_atras"]=datos_var_est[var].resample('1D').max().fillna(method='bfill').shift(i)
        for i in range(1,n_dias_alante):
            datos_var_est[var + " " + str(i) + "_dias_adelante"]=datos_var_est[var].resample('1D').max().fillna(method='bfill').shift(-i)
        datos_meteo_buenos_est_list.append(datos_var_est)
    for var in variables_semanales:
        datos_var_est=datos_est[[var]].resample('1D').max().fillna(method='bfill')
        for i in range(1,1+n_dias_atras//7):
            datos_var_est[var + " " + str(i) + "_semanas_atras"]=datos_var_est[var].resample('1D').ffill().fillna(method='bfill').shift(i*7)
        for i in range(1,1+n_dias_alante//7):
            datos_var_est[var + " " + str(i) + "_semanas_adelante"]=datos_var_est[var].resample('1D').ffill().fillna(method='bfill').shift(-i*7)
        datos_meteo_buenos_est_list.append(datos_var_est)
        
    datos_var_est=pd.concat(datos_meteo_buenos_est_list,axis=1).reset_index()
    datos_meteo_buenos_list.append(datos_var_est)
datos_meteo_buenos=pd.concat(datos_meteo_buenos_list).dropna()


datos_part3=pd.merge(datos_part2,datos_meteo_buenos, left_on=['date','closest'], right_on=['fecha','estacion'], how='inner').drop(columns=['fecha_x']).rename(columns={'fecha_y':'fecha'})


query='''select codigo, date, AVG(min) as min, AVG(max) as max, AVG(mean) as mean, AVG(std) as std, AVG(meidan) as median from 
public.copernicus_nvdi where pixels_array is not null and tesela is not null and EXTRACT(YEAR FROM date)>='''+str(year_sabana) + '''
group by codigo, date;'''
satelital_data = pd.read_sql_query(query, con=conexion).drop_duplicates()# conexion.commit done
conexion.commit()


subdatas_list=[]
for campo in satelital_data.codigo.unique():
    subdata=satelital_data[satelital_data.codigo==campo]
    subdata['date']=pd.to_datetime(subdata['date'],format='%Y-%m-%d')
    subdata['date2']=subdata['date']
    subdata=subdata.sort_values('date').set_index('date').resample('1D').ffill().reset_index()
    subdata['diff']=(subdata['date']-subdata['date2']).dt.days
    subdatas_list.append(subdata[subdata['diff']<21].drop(columns=['date2']))
satelital_data2=pd.concat(subdatas_list)
satelital_data2['dia']=satelital_data2.date.dt.dayofyear
satelital_data2['anio']=satelital_data2.date.dt.year


datos_total=pd.merge(datos_part3, satelital_data2, left_on=['codigo','fecha'], right_on=['codigo','date']).drop(columns=['date_x','date_y']).rename(columns={'fecha_x':'fecha'})

datos_total['doc_id']=datos_total['doc_id'].replace(dos)

datos_total=datos_total.reset_index(drop=True).join(pd.get_dummies(datos_total['doc_id'].tolist()+list(dos.values()),prefix='do')).drop(columns=['doc_id'])

datos_final=datos_total.drop(columns=['closest','estacion','anio'])

if not full_sabana:
    t = text("DELETE FROM public.sabana WHERE EXTRACT(YEAR FROM fecha)>"+str(year_sabana)+ " OR season='" + str(year_sabana) + '_' + str(year_sabana+1) + "';")
    result = engine.connect().execute(t)

datos_final=datos_final[(datos_final.fecha.dt.year>year_sabana)|(datos_final.season==(str(year_sabana) + '_' + str(year_sabana+1)))]
datos_final.to_sql('sabana', engine, schema='public', if_exists=if_exists,
                                                         index=False, chunksize=1000, dtype=None, method=None)
