# This scripts reads surface temperature data from the global forecasting model GFS's THREDDS server 
# makes a box of la lon and current time rerquest, then 
# passes it in python xarray format and saves it NETcdf fortmat TEMP_GFS.nc 
# To view the file, do: module load ncview and then ncview TEMP_GFS.nc
#from  netCDF4 import Dataset
#import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
from siphon.catalog import TDSCatalog
import csv


os.environ['OPENBLAS_NUM_THREADS'] = '1'

# More on : https://unidata.github.io/python-training/workshop/Bonus/downloading-gfs-with-siphon/

# 1.  construct an instance of TDSCatalog pointing to our dataset of interest
from siphon.catalog import TDSCatalog
best_gfs = TDSCatalog('http://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/'
                      'Global_0p25deg/catalog.xml?dataset=grib/NCEP/GFS/Global_0p25deg/Best')

best_gfs = TDSCatalog('http://193.144.42.171:8080/thredds/catalog.xml')
print(best_gfs.datasets)

#--- alterantive file 
#best_gfs = TDSCatalog('https://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p25deg/GFS_Global_0p25deg_20211026_0000.grib2/catalog.xml')
#print(best_gfs.datasets)

# 2.  pull out this dataset and call subset() to set up requesting a subset of the data.
best_ds = list(best_gfs.datasets.values())[0]
ncss = best_ds.subset()
# 3. set the ncss object to create a new query object, which facilitates asking for data from the server.
query = ncss.query()
#look at the ncss.variables object to see what variables are available from the dataset:
print(ncss.variables)
#exit()

# 4. construct a query asking for data corresponding to a latitude and longitude box where 43 lat is the northern extent, 35 lat is the southern extent, 260 long is the western extent and 249 is the eastern extent. Note that longitude values are the longitude distance from the prime meridian. We request the data for the current time. This request will return all surface temperatures for points in our bounding box for a single time. Note the string representation of the query is a properly encoded query string.

from datetime import datetime
date_time_str= '23/04/2021 12:00:00'
query.lonlat_box(north=55.9079, south=23.1275, east=57.1959, west=-31.1138).time(datetime.strptime(date_time_str, '%d/%m/%Y %H:%M:%S'))
query.accept('netcdf')

query.variables('Temperature_height_above_ground')

# 5. now request data from the server using this query. The NCSS class handles parsing this NetCDF data (using the netCDF4 module). If we print out the variable names, we see our requested variables, as well as a few others (more metadata information)

from xarray.backends import NetCDF4DataStore

import xarray as xr

data = ncss.get_data(query)
data = xr.open_dataset(NetCDF4DataStore(data))

#print(list(data))
# 6. pull out the temperature variable.
temp_3d = data['Temperature_height_above_ground']

#print(temp_3d.values)
#print(temp_3d.dims)
#print(temp_3d.coords)

temp_3d.to_netcdf('Temp_GFS.nc')
print("wrote surface temperature at Temp_GFS.nc")


exit()

# Sample code to write in a csv table :
#    data= Table()
#    var_arr=np.array(vars_f)
#    lat_arr=np.array(lats)
#    lon_arr=np.array(lons)
#    station_ID=np.array(test)
#    time_ts=np.array(ts)
#    elvtn=np.array(elev)
#
#    data['message type'] = message_type
#    data['station ID'] = station_ID
#    data['Valid_Time'] =time_ts
#    data['y'] = lat_arr
#    data['x'] = lon_arr
#    data['Elevation'] = elvtn
#    data['Grib code'] = GC
#    data['Level'] = a01
#    data['Height'] = hght
#    data['QC']=qc
#    data['var'] = var_arr
#    ascii.write(data, 'values_'+filename.dat,format='fixed_width_no_header', delimiter=' ', overwrite=True)

    #------------------------- END OF CALCULATIONS ----------------------------#










