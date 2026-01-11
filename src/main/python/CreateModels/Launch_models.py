import sys
import traceback
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json

os.environ["CUDA_VISIBLE_DEVICES"]="-1"

import tensorflow as tf
import keras
from keras.backend import clear_session
from keras.datasets import mnist
from keras.layers import Conv2D
from keras.layers import Dense
from keras.layers import Flatten
from keras.layers import Input
from keras.layers import Activation
from keras.layers import Dropout
from keras.models import Sequential
from keras.optimizers import RMSprop
from keras.models import Sequential, load_model, Model
from keras.callbacks import EarlyStopping, ModelCheckpoint
from keras.models import model_from_json

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, matthews_corrcoef, roc_auc_score, confusion_matrix, f1_score, classification_report, recall_score, roc_curve

from sqlalchemy import create_engine, text

import optuna

import psycopg2
import config as cfg

logger= cfg.logger

from datetime import timedelta

import utm
import time

import pickle

import glob

from sklearn.ensemble import RandomForestClassifier
from sklearn.utils import class_weight
import optuna

def predictionAlgorithm():

    try:

        conexion = psycopg2.connect(database=cfg.postgress_Database, 
                                        user=cfg.postgress_Username, 
                                        password=cfg.postgress_Password, 
                                        host=cfg.postgress_Host, 
                                        port=cfg.postgress_Port)


        if cfg.all_preds:
            query='''select * from public.sabana_sin_rad;'''
        else:
            query="select * from public.sabana_sin_rad  WHERE fecha >= (current_date - INTEGER '21');"
        sabana = pd.read_sql_query(query, con=conexion).drop_duplicates()
        conexion.commit()

        datos_total=sabana.reset_index(drop=True)#.join(pd.get_dummies(sabana['variedad'].tolist()+variedades,prefix='variedad')).rename(columns={"variedad": "variety"})
        conexion.close()
        if len(datos_total) == 0:
            logger.error('Error on predictionAlgorithm: Datos_total is empty, so no processing can be done')
            return -1

        if cfg.all_preds:
            datos_predict=datos_total
        else:
            datos_predict=datos_total[datos_total.fecha.dt.date>=(datos_total.fecha.max() - timedelta(days=2))]
        datos_predict['time']=datos_predict['fecha']+timedelta(days=7)
        datos_predict['pac_code']=datos_predict['codigo']

        list_dfs=[]

        try:
        
            with open(cfg.path_to_models+'pheno_info.json', 'r') as fp:
                    info = json.load(fp)
            if (info['type']=='RF')|(info['type']=='GB'):
                file = open(cfg.path_to_models+'pheno.pkl', 'rb')
                model = pickle.load(file)
                file.close()
                
                preds=model.predict(datos_predict[info['measures'][:-1]])
                probs=model.predict_proba(datos_predict[info['measures'][:-1]])
                
            elif (info['type']=='NN'):
                json_file = open(cfg.path_to_models + 'pheno.json')
                loaded_model_json = json_file.read()
                json_file.close()
                model = model_from_json(loaded_model_json)
                model.load_weights(cfg.path_to_models + 'pheno.h5')
                probs=model.predict(datos_predict[info['measures'][:-1]])
                preds=np.argmax(probs, axis=1)

            preds=np.asarray([preds[i] if preds[i]>=datos_predict['phenologystageid'].iloc[i] else datos_predict['phenologystageid'].iloc[i] for i in range(len(preds))])
            probs=probs[range(probs.shape[0]),preds.astype(int)-1]

            datos_predict2=datos_predict[['time','longitude','latitude', 'pac_code','variedad']].copy()
            datos_predict2.rename(columns={'variedad':'variety'})
            datos_predict2['model']="ITA"
                
            datos_predict2['value']=preds
            datos_predict2['magnitude']=1
            list_dfs.append(datos_predict2.copy())
            
            datos_predict2['value']=probs
            datos_predict2['magnitude']=2
            list_dfs.append(datos_predict2.copy())

        except Exception as e:
            logger.error("Phenological model does not exist " + str(e))
            return -1

        try:
            for key in cfg.codigos_enfermedades_predichas.keys():
                datos_predict2=datos_predict[['time','longitude','latitude', 'pac_code','variedad']].copy()
                datos_predict2.rename(columns={'variedad':'variety'})
                with open(cfg.path_to_models+key.lower()+'_info.json', 'r') as fp:
                    info = json.load(fp)
                
                if (info['type']=='RF')|(info['type']=='GB'):
                    file = open(cfg.path_to_models+key.lower()+'.pkl', 'rb')
                    model = pickle.load(file)
                    file.close()
                    
                    preds=model.predict(datos_predict[info['measures'][:-1]])
                    probs=model.predict_proba(datos_predict[info['measures'][:-1]])[:,1]
                    
                elif (info['type']=='NN'):
                    json_file = open(cfg.path_to_models + key.lower() + '.json')
                    loaded_model_json = json_file.read()
                    json_file.close()
                    model = model_from_json(loaded_model_json)
                    model.load_weights(cfg.path_to_models + key.lower() + '.h5')
                    probs=model.predict(datos_predict[info['measures'][:-1]])
                    preds=np.rint(probs)
                
                datos_predict2['model']="ITA"
                
                datos_predict2['value']=preds
                datos_predict2['magnitude']=cfg.codigos_enfermedades_predichas[key]
                list_dfs.append(datos_predict2.copy())
                
                datos_predict2['value']=probs
                datos_predict2['magnitude']=cfg.codigos_enfermedades_predichas[key]+1
                list_dfs.append(datos_predict2.copy())
        
        except Exception as e:
            logger.error(key + " model does not exist " + str(e))
            return -2

        try:

            engine = create_engine('postgresql://' + cfg.postgress_Username + ':' + cfg.postgress_Password + '@' + cfg.postgress_Host + ':' + str(cfg.postgress_Port) + '/meteo')
            if cfg.all_preds:
                t = text("DELETE FROM " + '"ITAINNOVA"' + ".predictions WHERE magnitude>=3 AND model='ITA';") 
                result = engine.connect().execute(t)
                pd.concat(list_dfs).to_sql('predictions', engine, schema='ITAINNOVA', if_exists='replace',
                                                                    index=False, chunksize=1000, dtype=None, method=None)
            else:
                t = text("DELETE FROM  " + '"ITAINNOVA"' + ".predictions WHERE time>='"+str(datos_predict.fecha.min().year)+"-"+str(datos_predict.fecha.min().month)+"-"+str(datos_predict.fecha.min().day)+"';")
                result = engine.connect().execute(t)
                pd.concat(list_dfs).to_sql('predictions', engine, schema='ITAINNOVA', if_exists='append',
                                                                    index=False, chunksize=1000, dtype=None, method=None)
                                                            
        except Exception as e:
            logger.error("Error doing operation with the database " + str(e))
            return -3

    except Exception as e:
        logger.error(traceback.format_exc())
        return -4

    return 0

sys.exit(predictionAlgorithm())