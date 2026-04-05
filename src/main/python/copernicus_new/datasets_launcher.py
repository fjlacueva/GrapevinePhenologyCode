from datasets_generator import DatasetsGenerator
import copernicus_launcher_params as params

parameters = params.DATASETS_PARAMS

d_generator = DatasetsGenerator(parameters)
d_generator.generate_datasets()