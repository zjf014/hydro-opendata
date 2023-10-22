"""
该模块用于从minio中读取gfs数据，主要方法包括：

- `open_dataset` - 普通读取数据方法
- `from_shp` - 通过已有矢量范围读取数据方法
- `from_aoi` - 通过已有GeoDataFrame范围读取数据方法
"""

import os
import numpy as np
import s3fs
import xarray as xr
import calendar
from datetime import date
import json

from ..common import minio_paras, fs, ro
from ..utils import regen_box

bucket_name = minio_paras["bucket_name"]

# 后期从minio读取
start = np.datetime64("2016-07-10")
end = np.datetime64("2023-08-17")
change = np.datetime64("2022-09-01")
# change_date = date(2022,9,1)
box = (115, 38, 136, 54)


# 2t  ----- temperature_2m_above_ground
# 2sh ----- specific_humidity_2m_above_ground
# 2r  ----- relative_humidity_2m_above_ground
# 10u ----- u_component_of_wind_10m_above_ground
# 10v ----- v_component_of_wind_10m_above_ground
# tp  ----- total_precipitation_surface
# pwat----- precipitable_water_entire_atmosphere
# tcc ----- total_cloud_cover_entire_atmosphere
# dswrf ----- downward_shortwave_radiation_flux

variables = {
    "dswrf": "downward_shortwave_radiation_flux",
    "pwat": "precipitable_water_entire_atmosphere",
    "2r": "relative_humidity_2m_above_ground",
    "2sh": "specific_humidity_2m_above_ground",
    "2t": "temperature_2m_above_ground",
    "tcc": "total_cloud_cover_entire_atmosphere",
    "tp": "total_precipitation_surface",
    "10u": "u_component_of_wind_10m_above_ground",
    "10v": "v_component_of_wind_10m_above_ground",
}


def open_dataset(
    data_variable="tp",
    creation_date=np.datetime64("2022-09-01"),
    creation_time="00",
    bbox=box,
    time_chunks=24,
):
    """
    从minio服务器读取gfs数据

    Args:
        data_variables (str): 数据变量，目前只支持tp，即降雨
        creation_date (datetime64): 创建日期
        creation_time (datetime64): 创建时间，即00\06\12\18之一
        bbox (list|tuple): 四至范围
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """

    if data_variable in variables.keys():
        short_name = data_variable
        full_name = variables[data_variable]

        with fs.open(f"{bucket_name}/geodata/gfs/gfs.json") as f:
            cont = json.load(f)
            start = np.datetime64(cont[short_name][0]["start"])
            end = np.datetime64(cont[short_name][-1]["end"])

        if creation_date < start or creation_date > end:
            print("超出时间范围！")
            return

        if creation_time not in ["00", "06", "12", "18"]:
            print("creation_time必须是00、06、12、18之一！")
            return

        year = str(creation_date.astype("object").year)
        month = str(creation_date.astype("object").month).zfill(2)
        day = str(creation_date.astype("object").day).zfill(2)

        if creation_date < change:
            json_url = f"s3://{bucket_name}/geodata/gfs/gfs_history/{year}/{month}/{day}/gfs{year}{month}{day}.t{creation_time}z.0p25.json"
        else:
            json_url = f"s3://{bucket_name}/geodata/gfs/{short_name}/{year}/{month}/{day}/gfs{year}{month}{day}.t{creation_time}z.0p25.json"

        chunks = {"valid_time": time_chunks}
        ds = xr.open_dataset(
            "reference://",
            engine="zarr",
            chunks=chunks,
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    "fo": fs.open(json_url),
                    "remote_protocol": "s3",
                    "remote_options": ro,
                },
            },
        )

        if creation_date < change:
            ds = ds[full_name]

        # ds = ds.filter_by_attrs(long_name=lambda v: v in data_variables)
        ds = ds.rename({"longitude": "lon", "latitude": "lat"})
        # ds = ds.transpose('time','valid_time','lon','lat')

        bbox = regen_box(bbox, 0.25, 0)

        if bbox[0] < box[0]:
            left = box[0]
        else:
            left = bbox[0]

        if bbox[1] < box[1]:
            bottom = box[1]
        else:
            bottom = bbox[1]

        if bbox[2] > box[2]:
            right = box[2]
        else:
            right = bbox[2]

        if bbox[3] > box[3]:
            top = box[3]
        else:
            top = bbox[3]

        longitudes = slice(left - 0.00001, right + 0.00001)
        latitudes = slice(bottom - 0.00001, top + 0.00001)

        ds = ds.sortby("lat", ascending=True)
        ds = ds.sel(lon=longitudes, lat=latitudes)

        return ds

    else:
        print("变量名不存在！")


import geopandas as gpd


def from_shp(
    data_variable="tp",
    creation_date=np.datetime64("2022-09-01"),
    creation_time="00",
    shp=None,
    time_chunks=24,
):
    """
    通过已有的矢量数据范围从minio服务器读取gfs数据

    Args:
        data_variables (str): 数据变量，目前只支持tp，即降雨
        creation_date (datetime64): 创建日期
        creation_time (datetime64): 创建时间，即00\06\12\18之一
        shp (str): 矢量数据路径
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """

    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = regen_box(
        (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]), 0.1, 0
    )

    return open_dataset(data_variable, creation_date, creation_time, bbox, time_chunks)


def from_aoi(
    data_variable="tp",
    creation_date=np.datetime64("2022-09-01"),
    creation_time="00",
    aoi: gpd.GeoDataFrame = None,
    time_chunks=24,
):
    """
    通过已有的GeoPandas.GeoDataFrame对象从minio服务器读取gfs数据

    Args:
        data_variables (str): 数据变量，目前只支持tp，即降雨
        creation_date (datetime64): 创建日期
        creation_time (datetime64): 创建时间，即00\06\12\18之一
        aoi (GeoDataFrame): 已有的GeoPandas.GeoDataFrame对象
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """
    b = aoi.bounds
    bbox = regen_box(
        (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]), 0.1, 0
    )

    return open_dataset(data_variable, creation_date, creation_time, bbox, time_chunks)
