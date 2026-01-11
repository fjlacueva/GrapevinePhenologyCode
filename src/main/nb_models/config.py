#DATABASE:
db_database='meteo'
db_user='*******' 
db_password='*******'
db_host='artemisa.*******.****'
db_port=******

#CREATE SABANA
full_sabana=True
year_sabana=2021
variedades=['GARNACHA', 'CHARDONNAY', 'CABERNET SAUVIGNON', 'MAZUELA', 'SYRACH', 'TEMPRANILLO']
dic_variedades_replace={'CABERNET-SAUVIGNON':'CABERNET SAUVIGNON','GARNACHA TINTA':'GARNACHA'}
variables_diarias_min=['tmed_min']#, 'rad_min']
variables_diarias_max=['tmed_max']#, 'rad_max']
variables_diarias_mean=['tmed_mean', 'hr_mean', 'wind_N', 'wind_NE', 'wind_E','wind_SE', 'wind_S', 'wind_SW', 
                        'wind_W', 'wind_NW']#,'rad_mean']
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
codigos_enfermedades_predichas={'Oidio':5, 'Piral':7}#'Lobesia':3,  #OJO, estos nombres deben coincidir con los de los 
#archivos (sin la extensión) ubicados en la carpeta definida en la variable 'path_to_models'
all_preds=False #Change here to 'False' to launch just last 7 days predictions
path_to_models='/data/proyectos/GRAPEVINE/Models/best_models/diseases/' ##Poner aquí la ruta de descompresión de la carpeta
#de NEXUS. En NEXUS está en https://argon-docker.itainnova.es/repository/war/PRO19_0383_GRAPEVINE/models/diseases.zip
