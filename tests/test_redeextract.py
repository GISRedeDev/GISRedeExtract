"""Unit tests for redeextract testing rasterisation AND extraction to mastergrid"""
import pytest
from pathlib import Path
import rasterio

import redeextract

BASE = Path(__file__).resolve().parent.joinpath('data/test_extract')
GLOB_RASTER = BASE.joinpath('global_test.tif')
MASTERGRID = BASE.joinpath('LSO_426_L0_WP_1km.tif')

TEST_SHP = BASE.joinpath('gadm36_LSO_0.shp')
TEST_GPKG = BASE.joinpath('gadm36_LSO.gpkg')
TEST_SHP_TO_RASTER = BASE.joinpath('test_to_rasterize.shp')
TEST_GPKG_TO_RASTER = BASE.joinpath('test_to_rasterize.gpkg')

OUT_RASTER = BASE.joinpath('TEST_RASTER.tif')
OUT_EXTRACT = BASE.joinpath('TEST_EXTRACT.tif')

@pytest.fixture
def extr():
    x = redeextract.ExtractByRasterMask(GLOB_RASTER, MASTERGRID, OUT_EXTRACT, resampling="nearest")
    yield x

@pytest.fixture
def shp():
    x = redeextract.RasteriseToMastergrid(TEST_SHP, MASTERGRID, OUT_RASTER, 'GID')
    yield x 

@pytest.fixture
def gpkg():
    y = redeextract.RasteriseToMastergrid(TEST_GPKG, MASTERGRID, OUT_RASTER, 'GID', layer='gadm36_LSO')
    yield y


#----------------------------------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
#--------------------RASETERIZE----------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
def test_class_instantiation(shp, gpkg):    
    assert isinstance(shp, redeextract.RasteriseToMastergrid)
    assert isinstance(gpkg, redeextract.RasteriseToMastergrid)

def test_incorrect_field_type_raises_exception():
    with pytest.raises(redeextract.AttributeFieldInvalidError) as exc_info:        
        x = redeextract.RasteriseToMastergrid(TEST_SHP_TO_RASTER, MASTERGRID, OUT_RASTER, field='GID_0')
    assert str(exc_info.value) == 'GID_0 is not a valid attribute in the shapefile/geopackage. Please retry with a valid integer field.'

def test_extent_and_dimensions(gpkg):
    src = rasterio.open(MASTERGRID)
    expected_extent = f'{src.bounds.left} {src.bounds.bottom} {src.bounds.right} {src.bounds.top}'
    expected_dims = f'{src.width} {src.height}'
    src.close()
    assert gpkg.extent == expected_extent 
    assert gpkg.dims == expected_dims

def test_rasterise(gpkg):
    gpkg.rasterise()
    src = rasterio.open(OUT_RASTER)
    expected_extent = f'{src.bounds.left} {src.bounds.bottom} {src.bounds.right} {src.bounds.top}'
    expected_dims = f'{src.width} {src.height}'
    assert OUT_RASTER.exists()
    assert gpkg.extent == expected_extent #CHECK OUTPUT matches master
    assert gpkg.dims == expected_dims #CHECK OUTPUT matches master


#----------------------------------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
#--------------------RASETERIZE----------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#



#----------------------------------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
#--------------------EXTRACT-------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
def test_extract_by_raster_mask(extr):
    src = rasterio.open(OUT_EXTRACT)
    extent = f'{src.bounds.left} {src.bounds.bottom} {src.bounds.right} {src.bounds.top}'
    dims = f'{src.width} {src.height}'
    src.close()

    src_mst = rasterio.open(MASTERGRID)
    mst_extent = f'{src_mst.bounds.left} {src_mst.bounds.bottom} {src_mst.bounds.right} {src_mst.bounds.top}'
    dims_mst = f'{src_mst.width} {src_mst.height}'
    src_mst.close()
    assert OUT_EXTRACT.exists()
    assert mst_extent == extent
    assert dims_mst == dims


#----------------------------------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#
#--------------------EXTRACT-------------------------------#
#----------------------------------------------------------#
#----------------------------------------------------------#



