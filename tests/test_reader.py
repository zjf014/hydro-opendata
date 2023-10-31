"""
Author: Wenyu Ouyang
Date: 2023-10-31 09:28:05
LastEditTime: 2023-10-31 11:39:53
LastEditors: Wenyu Ouyang
Description: Test for reader
FilePath: \hydro_opendata\tests\test_reader.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import os
from minio import Minio
import hydrodataset as hds
from hydro_opendata.reader.grdc import GRDCDataHandler
from hydro_opendata.reader.reader import (
    AOI,
    GPMDataHandler,
    GFSDataHandler,
    LocalFileReader,
    MinioFileReader,
)


def test_reader_interface(minio_paras):
    # 初始化Minio客户端
    minio_server = minio_paras["endpoint_url"]
    minio_client = Minio(
        minio_server.replace("http://", ""),
        access_key=minio_paras["access_key"],
        secret_key=minio_paras["secret_key"],
        secure=False,
    )

    gpm_handler = GPMDataHandler()
    gfs_handler = GFSDataHandler()
    aoi = AOI("grid", {"lat": 0, "lon": 0, "size": 1})

    local_gpm_reader = LocalFileReader(gpm_handler)
    local_gfs_reader = LocalFileReader(gfs_handler)
    local_gpm_reader.read("path/to/file", aoi)

    # Assume you have initialized the minio_client somewhere
    minio_gpm_reader = MinioFileReader(minio_client, gpm_handler)
    minio_gfs_reader = MinioFileReader(minio_client, gfs_handler)
    minio_gpm_reader.read("path/to/file", aoi)


def test_reader_grdc():
    grdc_handler = GRDCDataHandler()
    aoi = AOI(
        "station",
        {"station_id": "2181200", "start_time": "1980-01-01", "end_time": "2001-01-01"},
    )

    local_grdc_reader = LocalFileReader(grdc_handler)
    grdc_data = local_grdc_reader.read(
        os.path.join(hds.CACHE_DIR.joinpath("grdc_daily_data"), "grdc_daily_data.nc"),
        aoi,
    )
