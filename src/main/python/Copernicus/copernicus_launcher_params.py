#Parameters for the launchers
DOWNLOADER_PARAMS = {
    "boundaryGeojsonPath": r'aragon_polygon.geojson',
    "parcelsGeojsonPath": r'./notebooks/20210602controlparcels.geojson',
    "outputFolder": r'./notebooks/modules/results',
    "startDate": "20160101", #change to 2016
    "endDate": "20220111"
}

NDVI_PARAMS = {
    "inputFolder": r'./notebooks/modules/results/products/',
    "outputFolder": r'./notebooks/modules/ndvis',
    "boundaryGeojsonPath": r'aragon_polygon.geojson'
}

DATASETS_PARAMS = {
    "inputFolder": r'./notebooks/modules/ndvis/ndvi_images/',
    "outputFolder": r'./notebooks/modules/datasets/control_parcels/',
    "parcelsGeojsonPath": r'./notebooks/20210602controlparcels.geojson'
}