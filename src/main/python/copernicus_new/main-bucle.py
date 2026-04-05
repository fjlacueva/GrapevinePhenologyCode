from products_downloader import ProductsDownloader
from ndvi_generator import NDVIGenerator
from datasets_generator import DatasetsGenerator

while 1:
    parameters = {
        "boundaryGeojsonPath": r'./aragon_polygon.geojson',
        "parcelsGeojsonPath": r'/data/proyectos/GRAPEVINE/20210602controlparcels.geojson',#r'./notebooks/20210602controlparcels.geojson', #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson',
        "outputFolder": r'/data/proyectos/GRAPEVINE/modules/results',#r'./notebooks/modules/results',
        "startDate": "20160101",
        "endDate": "20230101"
    }
    downloader = ProductsDownloader(parameters)

    print(downloader.download())
    downloaded=downloader.unzip()

    parameters = {
        "inputFolder": r'/data/proyectos/GRAPEVINE/modules/results/products/', #r'./notebooks/modules/results/products/',
        "outputFolder": r'/data/proyectos/GRAPEVINE/modules/ndvis', #r'./notebooks/modules/ndvis',
        "boundaryGeojsonPath": r'aragon_polygon.geojson'
    }

    generator = NDVIGenerator(parameters)
    processed=generator.process_products(downloaded)

    parameters = {
        "inputFolder": r'/data/proyectos/GRAPEVINE/modules/ndvis/ndvi_images/',#r'./notebooks/modules/ndvis/ndvi_images/',
        "outputFolder": r'/data/proyectos/GRAPEVINE/modules/datasets/DO_Somontano/',#r'./notebooks/modules/datasets/DO_Somontano/',
        "parcelsGeojsonPath": r'/data/proyectos/GRAPEVINE/20210602controlparcels.geojson' #r'./notebooks/Teselas/A.ES.SDGC.CP.22001.cadastralparcel.geojson'
    }

    d_generator = DatasetsGenerator(parameters)
    d_generator.generate_datasets(processed)