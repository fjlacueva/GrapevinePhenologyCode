from ndvi_generator import NDVIGenerator
import copernicus_launcher_params as params

parameters = params.NDVI_PARAMS

generator = NDVIGenerator(parameters)
generator.process_products()