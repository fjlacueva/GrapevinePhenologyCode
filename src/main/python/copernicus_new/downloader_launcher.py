from products_downloader import ProductsDownloader
import copernicus_launcher_params as params

parameters = params.DOWNLOADER_PARAMS
downloader = ProductsDownloader(parameters)

#print(downloader.download())
print(downloader.unzip())