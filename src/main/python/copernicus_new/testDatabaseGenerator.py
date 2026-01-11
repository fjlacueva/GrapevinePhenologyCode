# It has to implement the integration process of the Copernicus.
# This test will be done with Francisco and Gorka.

import datetime
from products_downloader_new import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

from config import parameters_downloader, parameters_ndvi_generator, parameters_dataset_generator, logger

import configDates as cf

import sys

def downloadCopernicus():
   
    processed = None
    d_generator = DatasetsGenerator(parameters_dataset_generator)
    d_generator.generate_datasets(processed)
    logger.info("Copernicus generate_datasets summary")
    
downloadCopernicus()