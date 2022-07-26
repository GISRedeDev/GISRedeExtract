"""Unit tests for redeextract testing rasterisation AND extraction to \
    mastergrid"""

import pytest
from pathlib import Path
import rasterio

import redeextract

BASE = Path(__file__).resolve().parent.joinpath('data')
GLOB_RASTER = BASE.joinpath('global_test.tif')
TEMPLATE = BASE.joinpath('template.tif')

TEST_SHP = BASE.joinpath('gadm41_DMA_shp/gadm41_DMA_0.shp')
TEST_GPKG = BASE.joinpath('gadm41_DMA.gpkg')

OUT_RASTER = BASE.joinpath('TEST_RASTER.tif')
OUT_EXTRACT = BASE.joinpath('TEST_EXTRACT.tif')


@pytest.fixture
def extr():
    x = redeextract.ExtractByRasterMask(GLOB_RASTER, TEMPLATE,
                                        OUT_EXTRACT,
                                        resampling="nearest")
    yield x


@pytest.fixture
def shp():
    x = redeextract.RasteriseToMastergrid(TEST_SHP,
                                          OUT_RASTER,
                                          'val',
                                          template=GLOB_RASTER)
    yield x


@pytest.fixture
def gpkg():
    y = redeextract.RasteriseToMastergrid(TEST_GPKG,
                                          OUT_RASTER, 'val',
                                          template=GLOB_RASTER,
                                          layer='ADM_ADM_0')
    yield y


# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# --------------------RASETERIZE----------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#


def test_class_instantiation(shp, gpkg):
    assert isinstance(shp, redeextract.RasteriseToMastergrid)
    assert isinstance(gpkg, redeextract.RasteriseToMastergrid)


def test_incorrect_field_type_raises_exception():
    with pytest.raises(redeextract.AttributeFieldInvalidError) as \
            exc_info:
        _ = redeextract.RasteriseToMastergrid(TEST_SHP,
                                              OUT_RASTER,
                                              field='GID_0',
                                              template=GLOB_RASTER)
    assert str(exc_info.value) == \
        ('GID_0 is not a valid attribute in '
         'the shapefile/geopackage. Please retry with a valid'
         ' integer field.')


def test_extent_and_dimensions(extr):
    src = rasterio.open(TEMPLATE)
    expected_extent = (src.bounds.left, src.bounds.bottom,
                       src.bounds.right, src.bounds.top)
    expected_width = src.width
    expected_height = src.height
    src.close()
    assert extr.extent == expected_extent
    assert extr.height == expected_height
    assert extr.width == expected_width


def test_rasterise_gpkg(gpkg):
    gpkg.rasterise()
    assert OUT_RASTER.exists()


def test_rasterise_shp(shp):
    shp.rasterise()
    assert OUT_RASTER.exists()


# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# --------------------RASETERIZE----------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#

# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# --------------------EXTRACT-------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
def test_extract_by_raster_mask(extr):
    src = rasterio.open(OUT_EXTRACT)
    extent = (f'{src.bounds.left} {src.bounds.bottom} '
              f'{src.bounds.right} {src.bounds.top}')
    dims = f'{src.width} {src.height}'
    src.close()

    src_mst = rasterio.open(TEMPLATE)
    mst_extent = \
        (f'{src_mst.bounds.left} {src_mst.bounds.bottom} '
         f'{src_mst.bounds.right} {src_mst.bounds.top}')
    dims_mst = f'{src_mst.width} {src_mst.height}'
    src_mst.close()
    assert OUT_EXTRACT.exists()
    assert mst_extent == extent
    assert dims_mst == dims


# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# --------------------EXTRACT-------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
