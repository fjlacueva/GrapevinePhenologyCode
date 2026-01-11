import datetime
from products_downloader import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

from config import parameters_downloader, parameters_ndvi_generator, parameters_dataset_generator, logger

from datetime import datetime, timedelta


import sys

def downloadCopernicus(parameters_downloader):
    downloader = ProductsDownloader(parameters_downloader)

    print(downloader.download())
    downloaded=downloader.unzip()

    generator = NDVIGenerator(parameters_ndvi_generator)
    processed=generator.process_products(downloaded)

    d_generator = DatasetsGenerator(parameters_dataset_generator)
    d_generator.generate_datasets(processed)


dateToday = datetime.now().date()
dateToday = str(dateToday).replace('-', '')
yesterday = datetime.now() - timedelta(1)
parameters_downloader["startDate"]='20230701'#datetime.strftime(yesterday, '%Y%m%d')
parameters_downloader["endDate"]=dateToday
'''
if len(sys.argv) > 1:
    parameters_downloader["startDate"]=sys.argv[1]

if len(sys.argv) > 2:
    parameters_downloader["endDate"]=sys.argv[2]
    
'''

logger.debug( 'Pending invocation to downloadCopernicus( {}) method'.format(parameters_downloader))
downloadCopernicus(parameters_downloader)
