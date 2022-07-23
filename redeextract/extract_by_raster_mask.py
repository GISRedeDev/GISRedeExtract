"""
Class to extract global rasters to extent and mask of WorldPop Mastergrid

GDAL command line utilities should be installed on the working
 computer and the path to GDAL should be set in the environment variables
"""
from concurrent.futures import ThreadPoolExecutor
import fiona
import geopandas as gpd
import subprocess
import threading
import rasterio
import numpy as np
import pandas.api.types as ptypes
import os

MAX_WORKERS = os.cpu_count() * 5  # Number of threads to use concurrently


# Lookups for rasterio vs numpy datatypes (THIS DOESN'T INCLUDE ALL TYPES)
DATA_TYPES = {
    'int32': np.int32,
    'uint32': np.uint32,
    'float64': np.float64,
    'int16': np.int16,
    'uint16': np.uint16,
    'float32': np.float32,
    'ubyte': np.ubyte,
    'byte': np.byte
}


class ExtractByRasterMask:

    def __init__(self, glob_raster, mask_raster, out_raster,
                 nodata=-99999, dtype='float32',
                 resampling='bilinear'
                 ):
        """
        Parameters:
        glob_raster (Path/str) :: Path to input global raster from
        which to extract
        mask_raster (Path/str) :: Path to raster to use as mask
        out_raster (Path/str) :: Path to output extracted raster
        nodata (int) :: Nodata value to set in output (Default = -99999)
        dtype (str) :: Dtype of output raster
        resampling (str) :: Method to use for resampling (Default =
        bilinear) --options see https://gdal.org/programs/gdalwarp.html
        """
        self.glob_raster = glob_raster
        self.mask_raster = mask_raster
        self.out_raster = out_raster
        if not self.out_raster.parent.exists():
            self.out_raster.parent.mkdir(parents=True)
        self.nodata = nodata
        self.dtype = dtype
        self.resampling = resampling
        self.original_nodata = None
        self.extent_str, self.dimensions_str, self.profile = \
            self.get_extent_dims()
        self.extract_to_extent()
        self.extract_to_mask()
        self.calc_stats()

    def get_extent_dims(self):
        """
        Returns
        extent_str, dimensions_str (Tuple) : Returns tuple of strings
        representing extent and dimensions
        """
        src = rasterio.open(self.mask_raster)
        profile = src.profile.copy()
        self.original_nodata = profile['nodata']
        profile.update(dtype=self.dtype, nodata=self.nodata,
                       compress='LZW', bigtiff='IF_SAFER')
        profile.update(tiled=True)
        profile.update(blockxsize=512, blockysize=512)
        extent_str = f'{src.bounds.left} {src.bounds.bottom} \
         {src.bounds.right} {src.bounds.top}'
        dimension_str = f'{src.width} {src.height}'
        src.close()
        return extent_str, dimension_str, profile

    def extract_to_extent(self):
        """
        Saves tmp file in same folder as out_raster as extraction of
        global raster to extent of mask raster
        """
        cmd = f'gdalwarp -ot Float32 -te {self.extent_str} -r \
            {self.resampling} -srcnodata {self.original_nodata} \
                -dstnodata {self.nodata} -co "COMPRESS=LZW" -co \
                "PREDICTOR=2" -co "BIGTIFF=YES" -co "BLOCKXSIZE=512" \
                -co "BLOCKYSIZE=512" -co "TILED=YES" \
                -ts {self.dimensions_str} {str(self.glob_raster)} \
                {str(self.out_raster.parent.joinpath("tmp.tif"))}'
        subprocess.run(cmd, stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT)

    def extract_to_mask(self):
        """
        Extracts tmp raster by mask using mask raster
        """
        with rasterio.open(self.mask_raster) as mst,\
            rasterio.open(self.out_raster.parent.joinpath('tmp.tif')) as src, \
                rasterio.open(self.out_raster, 'w', **self.profile) as dst:
            windows = [window for ji, window in src.block_windows()]
            read_lock = threading.Lock()
            write_lock = threading.Lock()

            def process(window):
                with read_lock:
                    data = src.read(window=window)
                with read_lock:
                    data_mst = mst.read(window=window)
                data[data_mst == mst.nodata] = self.nodata
                data[data == self.original_nodata] = self.nodata
                data[data < -9999999999] = self.nodata
                data = data.astype(DATA_TYPES[self.dtype])
                with write_lock:
                    dst.write(data, window=window)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(process, windows)

    def calc_stats(self):
        """Calls gdalinfo to calculate stats on output raster"""
        cmd = f"gdalinfo -stats {self.out_raster}"
        subprocess.call(cmd, stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT)
        # REMOVE TMP RASTER
        self.out_raster.parent.joinpath('tmp.tif').unlink()


class RasteriseToMastergrid:
    """Class with functions that call gdal commands to rasterise
    shapefiles or geopackages so that the output extent and resolution
    and transform match the input mastergrid"""
    def __init__(self, vector, mastergrid, out_raster, field,
                 dtype="int32", nodata=9999, layer=None):
        """
        Initialisation:
        ---------------
        vector :    (Path/str)
            Path to shapefile or geopackage to rasterise

        mastergrid :    (Path/str)
            Path to mastergrid to use as a template for extent/
            transform and resolution

        out_raster :    (Path/str)
            Path to output raster

        field : (str)
            Attribute field in shapefile to use as pixel value. Values
            in this field should be numeric - preferrably integer

        dtype : (str)
            Datatype/pixel depth of output raster
                (DEFAULT = "int32")
                OPTIONS = ['uint32','float64','int16','uint16',
                'float32','ubyte','byte']

        nodata :    (int)
            Output nodata value

        layer : (None or str)
            If geopackage with multiple layers, layer name should be
            set. This is not required if shapefile or geopackage with
            one layer

        Returns:
        --------
        None
        """
        self.vector = vector
        self.mastergrid = mastergrid
        self.out_raster = out_raster
        self.field = field
        self.dtype = dtype
        self.nodata = nodata
        self.layer = layer
        if self.layer:
            try:
                assert self.layer in fiona.listlayers(self.vector)
            except AssertionError:
                raise LayerNotFoundError(f"{self.layer} not found in \
                    {self.vector}.")
        if not self.attribute_field_valid():
            raise AttributeFieldInvalidError(f'{self.field} is not a \
                valid attribute in the shapefile/geopackage. Please \
                retry with a valid integer field.')
        self.extent, self.dims = self.get_extent_dims_string()

    def attribute_field_valid(self,):
        """Returns True is valid integer field in self.vector, else false.

        Parameters:
        -----------
        None

        Returns:
        --------
        Boolean
        """
        gdf = gpd.read_file(self.vector, layer=self.layer)
        try:
            assert self.field in gdf.columns
            assert ptypes.is_numeric_dtype(gdf[self.field])
            return True
        except AssertionError:
            return False

    def get_extent_dims_string(self):
        """
        Returns
        extent_str, dimensions_str (Tuple) : Returns tuple of strings \
            representing extent and dimensions
        """
        src = rasterio.open(self.mastergrid)
        extent_str = f'{src.bounds.left} {src.bounds.bottom} \
            {src.bounds.right} {src.bounds.top}'
        dimension_str = f'{src.width} {src.height}'
        src.close()
        return extent_str, dimension_str

    def rasterise(self):
        """
        Calls gdal_rasterise command to rasterise vector file to dims \
            and resolution of mastergrid

        Parameters:
        -----------
        None

        Returns:
        ---------
        None

        """
        cmd = f'gdal_rasterize -a {self.field} -co "COMPRESS=LZW" -co \
            "PREDICTOR=2" -co "BIGTIFF=YES" -co "BLOCKXSIZE=512" -co \
            "BLOCKYSIZE=512" -co "TILED=YES" -ts {self.dims} -te \
            {self.extent} -a_nodata {self.nodata} -ot {self.dtype} \
            {str(self.vector)} {str(self.out_raster)}'
        subprocess.run(cmd, stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT)


class AttributeFieldInvalidError(Exception):
    pass


class LayerNotFoundError(Exception):
    pass
