import datetime
from products_downloader_new import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

from config import parameters_downloader, parameters_ndvi_generator, parameters_dataset_generator, logger

import configDates as cf

import sys

def downloadCopernicus(parameters_downloader):
    downloader = ProductsDownloader(parameters_downloader)

    rDownload= downloader.download()
    logger.info("Copernicus download summary: {}".format(rDownload))
    
    downloaded=downloader.unzip()


    generator = NDVIGenerator(parameters_ndvi_generator)
    processed=generator.process_products(downloaded)

    d_generator = DatasetsGenerator(parameters_dataset_generator)
    d_generator.generate_datasets(processed)

for i in range(cf.initialYear,cf.actualYear+1):
    useMonths = cf.months
    if i == cf.actualYear:
        useMonths = cf.actualMonths
    for j in useMonths:
        if j < 10:
            useMonth = "0" + str(j)
        else:
            useMonth = str(j)
        initialDate = str(i) + useMonth + "01"
        day = 31
        if ((j==3) or (j==4) or (j==6) or (j==9) or (j==11)):
            day = 30
        else:
            if j == 2:
                day = 28
        finalDate = str(i) + useMonth + str(day)
        parameters_downloader["startDate"]=initialDate
        parameters_downloader["endDate"]=finalDate

        logger.debug( 'Invoking downloadCopernicus( {}) method'.format(parameters_downloader))
        downloadCopernicus(parameters_downloader)
