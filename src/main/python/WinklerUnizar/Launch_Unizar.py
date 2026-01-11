import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from sklearn.metrics import accuracy_score, matthews_corrcoef, roc_auc_score, confusion_matrix, f1_score, classification_report, recall_score, roc_curve

from sqlalchemy import create_engine, text

import psycopg2
import config as cfg
logger= cfg.logger

from datetime import timedelta

import utm
import time

import glob

def main():

    try:
    
        logger.debug('Executing WinklerUnizar.main')
        conexion = psycopg2.connect(database=cfg.postgress_Database, 
                                        user=cfg.postgress_Username, 
                                        password=cfg.postgress_Password, 
                                        host=cfg.postgress_Host, 
                                        port=cfg.postgress_Port)

        query='''select c.modelo_id, c.winklermin, c.winklermax, c.phenologystageid as value, m.pdos_id, m.nombre as variedad from unizar.correspondenciawinkler c inner join unizar.modelos m on m.id=c.modelo_id;'''
        correspondencias = pd.read_sql_query(query, con=conexion).drop_duplicates()
        conexion.commit()
        correspondencias['variedad']=correspondencias.variedad.str.upper().replace({'GARNACHA-BORJA':'GARNACHA'})
        correspondencias


        all_preds=cfg.unizarForecastingGeneration_all_preds
        if all_preds:
            query='''select variedad, codigo, longitude, latitude, fecha, doc_id, phenologystageid, "winkler_10.0_2_Tbase_Cumm 1_semanas_adelante" as winkler from public.sabana_sin_rad;'''
        else:
            query="select variedad, codigo, longitude, latitude, fecha, doc_id, phenologystageid, " + '"winkler_10.0_2_Tbase_Cumm 1_semanas_adelante" as winkler from public.sabana_sin_rad ' + "WHERE fecha >= (current_date - INTEGER '51');"
        sabana = pd.read_sql_query(query, con=conexion).drop_duplicates()
        conexion.commit()
        #sabana

        dos=cfg.unizarForecastingGeneration_dos#{1.:'Borja',4.:'Calatayud',6.:'Somontano',8.:'CariĂ±ena'}

        preds=sabana.merge(correspondencias, how='left', left_on=['variedad'], right_on=['variedad'])
        preds=preds[(preds.winkler>=preds.winklermin)&(preds.winkler<preds.winklermax)]
        preds=preds.sort_values('pdos_id')
        preds['pdos_id']=preds.pdos_id.replace(dos)
        preds=preds.drop_duplicates(subset=['codigo','fecha'],keep='first')
        preds

        preds['time']=preds['fecha']+timedelta(days=7)
        preds['pac_code']=preds['codigo']
        #preds['variety']=preds['variedad']
        preds['magnitude']=1
        preds['model']='UNIZAR'
        preds=preds[['variedad','pac_code','longitude','latitude','time','value','magnitude','model']]
        engine = create_engine('postgresql://' + cfg.postgress_Username + ':' + cfg.postgress_Password + '@' + cfg.postgress_Host + ':' + str(cfg.postgress_Port) + '/meteo')

        if all_preds:
            t = text("DELETE FROM " +'"ITAINNOVA"'+".predictions WHERE model='UNIZAR';")
            result = engine.connect().execute(t)
            preds.to_sql('predictions', engine, schema='ITAINNOVA', if_exists='append',
                                                                index=False, chunksize=1000, dtype=None, method=None)
        else:
            t = text("DELETE FROM " +'"ITAINNOVA"'+".predictions WHERE model='UNIZAR' AND time>='"+str(preds.fecha.min().year)+"-"+str(preds.fecha.min().month)+"-"+str(preds.fecha.min().day)+"';")
            result = engine.connect().execute(t)
            preds.to_sql('predictions', engine, schema='ITAINNOVA', if_exists='append',
                                                                index=False, chunksize=1000, dtype=None, method=None)
                                                            
        logger.debug('Finished WinklerUnizar.main')

    except Exception as e:
        logger.error(str(e))
        exit(-1)

main()