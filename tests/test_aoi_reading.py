"""
Author: Wenyu Ouyang
Date: 2023-10-06 21:17:17
LastEditTime: 2023-10-22 16:51:42
LastEditors: Wenyu Ouyang
Description: Test reading according to AOI
FilePath: \hydro_opendata\tests\test_aoi_reading.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import os
import pytest
import geopandas as gpd

from hydro_opendata.reader.minio import ERA5LReader, GPMReader, GFSReader
import numpy as np


@pytest.fixture()
def geo_file():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "test.geojson")


@pytest.fixture()
def aoi_shp(geo_file):
    return gpd.read_file(geo_file)


@pytest.fixture()
def start_time():
    return np.datetime64("2021-06-01T00:00:00.000000000")


@pytest.fixture()
def end_time():
    return np.datetime64("2021-06-30T23:00:00.000000000")


@pytest.fixture()
def creation_date():
    return np.datetime64("2021-06-01")


def test_read_era5(aoi_shp, start_time, end_time):
    era5 = ERA5LReader()
    ds1 = era5.from_aoi(
        data_variables=["Total precipitation"],
        start_time=start_time,
        end_time=end_time,
        aoi=aoi_shp,
    )
    print(ds1)


def test_read_gpm(aoi_shp, start_time, end_time):
    gpm = GPMReader()
    ds2 = gpm.from_aoi(start_time=start_time, end_time=end_time, aoi=aoi_shp)
    print(ds2)


def test_read_gfs(aoi_shp, creation_date):
    gfs = GFSReader()
    ds3 = gfs.from_aoi(creation_date=creation_date, creation_time="00", aoi=aoi_shp)
    print(ds3)
