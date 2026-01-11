import os
import datetime
import threading

from products_downloader import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

import copernicus_launcher_params as params

def invoke_ndvi_generator():
    pass

def invoke_datasets_generator():
    #check input folder until there is any image 
    pass


while 1:

   #calculating current hour, minute and second
   today = datetime.datetime.today()
   today = str(today)
   current_hour = today[11:13]
   current_minute = today[14:16]
   current_sec = today[17:19]

   if current_hour == '08' and current_minute == '00' and current_sec == '00':

       #TODO invoke products_donwloader

       #TODO fork or threading to run the tasks concurrents
        n = threading.Thread(name='ndvi_gen', target=invoke_ndvi_generator)
        d = threading.Thread(name='worker', target=invoke_datasets_generator)

        n.start()
        d.start()