# It has to implement the integration process of the Copernicus.
# This test will be done with Francisco and Gorka.

import datetime
from products_downloader_new import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

from config import parameters_downloader, parameters_ndvi_generator, parameters_dataset_generator, logger

import configDates as cf

import sys

def downloadCopernicus(parameters_downloader):
    downloader = ProductsDownloader(parameters_downloader)

    #rDownload= downloader.download()
    rDownload= downloader.downloadCopernicusSentinel2Products()
    logger.info("Copernicus download summary: {}".format(rDownload))
    
    downloaded=downloader.unzip()
    
    logger.info("Copernicus unzip summary")
    
    generator = NDVIGenerator(parameters_ndvi_generator)
    processed=generator.process_products(downloaded)
    
    logger.info("Copernicus process_products summary")
    
    d_generator = DatasetsGenerator(parameters_dataset_generator)
    d_generator.generate_datasets(processed)
    
    #logger.info("Copernicus generate_datasets summary")

years = [2023]
for i in years:
    #useMonths = [11,12]
    useMonths = [11]
    if i == cf.actualYear:
        useMonths = cf.actualMonths
    for j in useMonths:
        if j < 10:
            useMonth = "0" + str(j)
        else:
            useMonth = str(j)
        initialDate = str(i) + "-" + useMonth + "-01"
        day = 15
        if ((j==3) or (j==4) or (j==6) or (j==9) or (j==11)):
            day = 30
        else:
            if j == 2:
                day = 28
        finalDate = str(i) + "-" + useMonth + "-" + str(day)
        #finalDate = str(i) + "-" + useMonth + "-0" + str(day)
        parameters_downloader["startDate"]=initialDate
        parameters_downloader["endDate"]=finalDate

        logger.debug( 'Invoking downloadCopernicus( {}) method'.format(parameters_downloader))
        downloadCopernicus(parameters_downloader)