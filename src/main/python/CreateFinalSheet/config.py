import logging
import os
import sys

paths2Libraries=['/projects/grapevine/GIT/src/src/main/python', '/projects/grapevine/GIT/src/src/main/python/Tools']
for path2Library in paths2Libraries:
    if os.path.isdir(path2Library):
      sys.path.append(path2Library)
# Try to import ENV.$PYTHONPATHS paths
paths2Libraries=os.getenv('PYTHONPATHS')
if paths2Libraries != None:
    paths2Libraries= paths2Libraries.split(',')
    for path2Library in paths2Libraries:
        if os.path.isdir(path2Library):
          sys.path.append(path2Library)

#logging
########
logger = logging.getLogger(__name__)
if not logger.handlers:
  realClimateDataCalculus_loggingPath='{}/{}.log'.format(os.getenv('LOGSPATH', '/logs'), 'climaticDataSiarWinkle')
  loggingLevel=os.getenv('LOGLEVEL', 'DEBUG').upper()
  logger.setLevel(loggingLevel)
  formatter= logging.Formatter("%(asctime)s;%(levelname)8s;%(filename)s[%(lineno)04d]: %(message)s","%Y%m%d-%H%M%S")
  fh = logging.FileHandler(realClimateDataCalculus_loggingPath, mode="w", encoding="utf-8")
  fh.setFormatter(formatter)
  # fh.setLevel(logging.DEBUG)
  logger.addHandler(fh)
  ch=logging.StreamHandler()
  formatter= logging.Formatter(">%(levelname)8s: %(message)s","%Y%m%d-%H%M%S")
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  logger.debug('Logs>={} sent to stdout & {}'.format(loggingLevel, realClimateDataCalculus_loggingPath))

#DATABASE:
postgress_Database='meteo'
postgress_Username='********' 
postgress_Password='**********'
postgress_Host='******.*******.****'
postgress_Port=*****

#CREATE SABANA
full_sabana=False
year_sabana=''
inicio_year=2016
variedades=['GARNACHA', 'CHARDONNAY', 'CABERNET SAUVIGNON', 'MAZUELA', 'SYRACH', 'TEMPRANILLO']
dic_variedades_replace={'CABERNET-SAUVIGNON':'CABERNET SAUVIGNON','GARNACHA TINTA':'GARNACHA'}
variables_diarias_min=['tmed_min']
variables_diarias_max=['tmed_max']
variables_diarias_mean=['tmed_mean', 'hr_mean', 'wind_N', 'wind_NE', 'wind_E','wind_SE', 'wind_S', 'wind_SW', 
                        'wind_W', 'wind_NW']
variables_semanales=['gdd_4.5_t0_Tbase_sum',
       'gdd_4.5_t0_TbaseMax_sum', 'gdd_4.5_1_Tbase_sum',
       'gdd_4.5_1_TbaseMax_sum', 'gdd_4.5_2_Tbase_sum',
       'gdd_4.5_2_TbaseMax_sum', 'gdd_10.0_t0_Tbase_sum',
       'gdd_10.0_t0_TbaseMax_sum', 'gdd_10.0_1_Tbase_sum',
       'gdd_10.0_1_TbaseMax_sum', 'gdd_10.0_2_Tbase_sum',
       'gdd_10.0_2_TbaseMax_sum', 'chillingDD_7.0_t0_Tbase_sum',
       'chillingDD_7.0_t0_Tbasemin_sum', 'chillingDD_7.0_t0_Utah_sum',
       'chillingDD_7.0_1_Tbase_sum', 'chillingDD_7.0_1_Tbasemin_sum',
       'chillingDD_7.0_1_Utah_sum', 'chillingDD_7.0_2_Tbase_sum',
       'chillingDD_7.0_2_Tbasemin_sum', 'chillingDD_7.0_2_Utah_sum', 'rad_sum',
       'precip_sum', 'winkler_4.5_Tbase', 'winkler_4.5_TbaseMax',
       'winkler_10.0_Tbase', 'winkler_10.0_TbaseMax',
       'gdd_4.5_t0_Tbase_sum_Cumm', 'gdd_4.5_t0_TbaseMax_sum_Cumm',
       'gdd_4.5_1_Tbase_sum_Cumm', 'gdd_4.5_1_TbaseMax_sum_Cumm',
       'gdd_4.5_2_Tbase_sum_Cumm', 'gdd_4.5_2_TbaseMax_sum_Cumm',
       'gdd_10.0_t0_Tbase_sum_Cumm', 'gdd_10.0_t0_TbaseMax_sum_Cumm',
       'gdd_10.0_1_Tbase_sum_Cumm', 'gdd_10.0_1_TbaseMax_sum_Cumm',
       'gdd_10.0_2_Tbase_sum_Cumm', 'gdd_10.0_2_TbaseMax_sum_Cumm',
       'chillingDD_7.0_t0_Tbase_sum_Cumm',
       'chillingDD_7.0_t0_Tbasemin_sum_Cumm',
       'chillingDD_7.0_t0_Utah_sum_Cumm', 'chillingDD_7.0_1_Tbase_sum_Cumm',
       'chillingDD_7.0_1_Tbasemin_sum_Cumm', 'chillingDD_7.0_1_Utah_sum_Cumm',
       'chillingDD_7.0_2_Tbase_sum_Cumm', 'chillingDD_7.0_2_Tbasemin_sum_Cumm',
       'chillingDD_7.0_2_Utah_sum_Cumm', 'rad__t0__Cumm', 'rad__1__Cumm',
       'rad__2__Cumm', 'precip__t0__Cumm', 'precip__1__Cumm',
       'precip__2__Cumm', 'winkler_4.5_t0_Tbase_Cumm',
       'winkler_4.5_t0_TbaseMax_Cumm', 'winkler_4.5_1_Tbase_Cumm',
       'winkler_4.5_1_TbaseMax_Cumm', 'winkler_4.5_2_Tbase_Cumm',
       'winkler_4.5_2_TbaseMax_Cumm', 'winkler_10.0_t0_Tbase_Cumm',
       'winkler_10.0_t0_TbaseMax_Cumm', 'winkler_10.0_1_Tbase_Cumm',
       'winkler_10.0_1_TbaseMax_Cumm', 'winkler_10.0_2_Tbase_Cumm',
       'winkler_10.0_2_TbaseMax_Cumm']

n_dias_atras=14
n_dias_alante=7

dos={1.:'Borja',4.:'Calatayud',6.:'Somontano',8.:'Cariñena'}

## TO LAUNCH THE PREDICTIONS:
# codigos_enfermedades_predichas={'Lobesia':3, 'Oidio':5} #OJO, estos nombres deben coincidir con los de los 
# #archivos (sin la extensión) ubicados en la carpeta definida en la variable 'path_to_models'
# all_preds=True #Change here to 'False' to launch just last 7 days predictions
# path_to_models='/data/proyectos/GRAPEVINE/Models/best_models/diseases/' ##Poner aquí la ruta de descompresión de la carpeta
#de NEXUS. En NEXUS está en https://argon-docker.itainnova.es/repository/war/PRO19_0383_GRAPEVINE/models/diseases.zip
