import datetime
from products_downloader import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

from config import parameters_downloader, parameters_ndvi_generator, parameters_dataset_generator, logger

import configDates as cf

import sys

def downloadCopernicus(parameters_downloader):
    downloader = ProductsDownloader(parameters_downloader)

    print(downloader.download())
    downloaded=downloader.unzip()


    generator = NDVIGenerator(parameters_ndvi_generator)
    processed=generator.process_products(downloaded)

    d_generator = DatasetsGenerator(parameters_dataset_generator)
    d_generator.generate_datasets(processed)

def setYears(initialYear,finalYear):

    currentDateTime = datetime.datetime.now()
    date = currentDateTime.date()
    actualYear = int(date.strftime("%Y"))
    yearsRange = range(initialYear,finalYear+1)
    for i in yearsRange:
        useMonths = cf.months
        if i == actualYear:
            useMonths = range(1,int(date.strftime("%m"))+1)
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

            logger.debug( 'Pending invocation to downloadCopernicus( {}) method'.format(parameters_downloader))
            #downloadCopernicus( parameters_downloader)

setYears(2016,2016)