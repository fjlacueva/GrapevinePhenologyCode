import geopandas as gpd
import rasterio 
from rasterio import plot
from rasterio.plot import show
from rasterio.mask import mask
from osgeo import gdal
import os
import config
import glob
import shutil

logger= config.logger
class NDVIGenerator():

    def __init__(self, parameters:dict=None):

        self.__PARAMETERSKEYS = ["inputFolder", "outputFolder", "boundaryGeojsonPath"]

        if parameters is None or not isinstance(parameters, dict):
            raise Exception ("NDVIGenerator.exist name parameter parameters must be a non empty dictionary." )
        
        self.parameters = parameters

        keys = parameters.keys()

        if len(keys) == 0:
            raise Exception ("NDVIGenerator.exist name parameter parameters must be a non empty dictionary." )
        for key in self.__PARAMETERSKEYS:
            if not key in keys:
                raise Exception ("NDVIGenerator.exist name parameter parameters does not contain parameter: "+ str(key) + " review your code" )
            value = parameters[key]

            if value is None:
                raise Exception ("NDVIGenerator.exist name parameter parameters[ "+ str(key) + "] dose not provide any value" )

        self.inputFolder = parameters["inputFolder"]
        if not os.path.exists(self.inputFolder) or not os.path.isdir(self.inputFolder):
            raise Exception ("NDVIGenerator.exist name parameters['inputFolder'] does not provide the path to a valid folder:"+ self.inputFolder )

        self.outputFolder = parameters["outputFolder"]
        '''if not os.path.exists(self.outputFolder) or not os.path.isdir(self.outputFolder):
            raise Exception ("NDVIGenerator.exist name parameters['outputFolder'] does not provide the path to a valid folder:"+ self.outputFolder )'''

        self.boundaryPath = parameters["boundaryGeojsonPath"]
        if not os.path.exists(self.boundaryPath):
            raise Exception ("NDVIGenerator.exist name parameters['boundaryGeojsonPath'] does not provide the path to a geojson:"+ self.boundaryPath )


    def __calculate_ndvi(self, nir, red):

        nir = nir.astype('f4')
        red = red.astype('f4')
        ndvi = (nir - red) / (nir + red)
        return ndvi

    def __format_tiff(self,path, pic):
        with rasterio.open(path,'w',driver='Gtiff', width=pic.width, height=pic.height, count=1, crs=pic.crs,transform=pic.transform, dtype=pic.dtypes[0]) as rgb:
            rgb.write(pic.read(1),1) 
            rgb.close()
            
        boundary = gpd.read_file(self.boundaryPath)
        bound_crs = boundary.to_crs({'init': pic.crs})
        with rasterio.open(path) as src:
            out_image, out_transform = mask(src,
            bound_crs.geometry,crop=True)
            out_meta = src.meta.copy()
            out_meta.update({"driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform})

        with rasterio.open(path, "w", **out_meta) as final:
            final.write(out_image)

    def __read_pic(self, path):
        with rasterio.open(path) as src:
            content = src.read(1)
        return content

    def __make_ndvi_tiff(self, nir, red, pic, path):
        
        ndvi = self.__calculate_ndvi(nir,red)
        
        with rasterio.open(path, 'w', driver='Gtiff', width=ndvi.shape[0], height=ndvi.shape[1], count=1, crs=pic.crs,transform=pic.transform, dtype=ndvi.dtype) as pic: 
            pic.write(ndvi,1)
        
        input_raster = gdal.Open(path)
        output_raster = path
        warp = gdal.Warp(output_raster,input_raster,dstSRS='EPSG:4326')
        warp = None # Closes the files
        
        
    #create tiff images with ndvi index from the products
    def __generate_tiff(self,path):
        
        dirs = os.listdir(self.inputFolder + path + '/GRANULE/')
        
        bands = self.inputFolder +path+'/GRANULE/'+dirs[0]+'/IMG_DATA'
        
        if 'S2A_MSIL2A' in path:
            bands=bands+'/R10m'
        
        nir_check = False
        red_check = False

        if not os.path.exists(self.outputFolder + config.TIFFS_PATH):
            os.makedirs(self.outputFolder + config.TIFFS_PATH)
            
        if not os.path.exists(self.outputFolder + config.NDVIS_PATH ):
            os.makedirs(self.outputFolder + config.NDVIS_PATH)
        
        for pic in os.listdir(bands):
            
            if "_B04" in pic:
                red = rasterio.open(bands+'/'+pic)
                red_pic = pic
                
                red_check = True
                
            elif "_B08" in pic:
                
                nir = rasterio.open(bands+'/'+pic)
                nir_pic = pic
                
                nir_check = True
        
        if (not nir_check) | (not red_check):
            
            logger.debug("It is not possible to create ndvi image. NIR or RED bands needed")
            return False
        
        red_path = self.outputFolder + config.TIFFS_PATH + '/' + red_pic[:-4] + '.tiff'
        
        nir_path = self.outputFolder + config.TIFFS_PATH + '/' + nir_pic[:-4] + '.tiff'
        
        
        self.__format_tiff(red_path, red)
        self.__format_tiff(nir_path, nir)
        
        red_i = self.__read_pic(red_path)
        nir_i = self.__read_pic(nir_path)
        
        self.__make_ndvi_tiff(nir_i, red_i, red,self.outputFolder + config.NDVIS_PATH +'/'+ red_pic[:22] + "ndvi.tiff" )
        
        return self.outputFolder + config.NDVIS_PATH +'/'+ red_pic[:22] + "ndvi.tiff"
        
    def process_products(self, downloaded):
        #prev_processed=glob.glob(self.outputFolder + config.NDVIS_PATH +'/*.tiff')  
        
        if os.path.exists(self.outputFolder +"/processed.txt"):
            txt = open(self.outputFolder +"/processed.txt", "r")
            ids = txt.read().split('\n')
        else:
            ids=[]
        
        new_files=[]
        try:
            files = os.listdir(self.inputFolder)#
            files = [file for file in files if file not in ids]
            for file in files:
                
                new_files.append(self.__generate_tiff(file))
                
                with open(self.outputFolder + "/processed.txt", "a+") as txt:
                    txt.write(file)
                    txt.write('\n')
                shutil.rmtree(self.inputFolder + '/' + file)
                
        except Exception as e:
            raise Exception("NDVIGenerator.process_products catches exception:"+ str(e))
        #last_processed=glob.glob(self.outputFolder + config.NDVIS_PATH +'/*.tiff') 
        return new_files#[file for file in last_processed if file not in prev_processed]