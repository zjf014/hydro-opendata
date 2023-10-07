"""
该模块用于从minio中读取era5-land数据，主要方法包括：

- `open_dataset` - 普通读取数据方法
- `from_shp` - 通过已有矢量范围读取数据方法
- `from_aoi` - 通过已有GeoDataFrame范围读取数据方法
- `to_netcdf` - 保存到本地文件
"""

from ..common import minio_paras, fs, ro
from ..utils import regen_box
import os
import s3fs
import numpy as np
import xarray as xr

bucket_name = minio_paras["bucket_name"]

start = np.datetime64("2015-01-01T00:00:00.000000000")
end = np.datetime64("2021-12-31T23:00:00.000000000")
box = (115, 38, 136, 54)
variables = [
    "10 metre U wind component",
    "10 metre V wind component",
    "2 metre dewpoint temperature",
    "2 metre temperature",
    "Evaporation",
    "Evaporation from bare soil",
    "Evaporation from open water surfaces excluding oceans",
    "Evaporation from the top of canopy",
    "Evaporation from vegetation transpiration",
    "Forecast albedo",
    "Lake bottom temperature",
    "Lake ice total depth",
    "Lake ice surface temperature",
    "Lake mix-layer depth",
    "Lake mix-layer temperature",
    "Lake shape factor",
    "Lake total layer temperature",
    "Leaf area index, high vegetation",
    "Leaf area index, low vegetation",
    "Potential evaporation",
    "Runoff",
    "Skin reservoir content",
    "Skin temperature",
    "Snow albedo",
    "Snow cover",
    "Snow density",
    "Snow depth",
    "Snow depth water equivalent",
    "Snow evaporation",
    "Snowfall",
    "Snowmelt",
    "Soil temperature level 1",
    "Soil temperature level 2",
    "Soil temperature level 3",
    "Soil temperature level 4",
    "Sub-surface runoff",
    "Surface latent heat flux",
    "Surface net solar radiation",
    "Surface net thermal radiation",
    "Surface pressure",
    "Surface runoff",
    "Surface sensible heat flux",
    "Surface solar radiation downwards",
    "Surface thermal radiation downwards",
    "Temperature of snow layer",
    "Total precipitation",
    "Volumetric soil water layer 1",
    "Volumetric soil water layer 2",
    "Volumetric soil water layer 3",
    "Volumetric soil water layer 4",
]
accumulated = [
    # '10 metre U wind component',
    # '10 metre V wind component',
    # '2 metre dewpoint temperature',
    # '2 metre temperature',
    "Evaporation",
    "Evaporation from bare soil",
    "Evaporation from open water surfaces excluding oceans",
    "Evaporation from the top of canopy",
    "Evaporation from vegetation transpiration",
    # 'Forecast albedo',
    # 'Lake bottom temperature',
    # 'Lake ice total depth',
    # 'Lake ice surface temperature',
    # 'Lake mix-layer depth',
    # 'Lake mix-layer temperature',
    # 'Lake shape factor',
    # 'Lake total layer temperature',
    # 'Leaf area index, high vegetation',
    # 'Leaf area index, low vegetation',
    "Potential evaporation",
    "Runoff",
    # 'Skin reservoir content',
    # 'Skin temperature',
    # 'Snow albedo',
    # 'Snow cover',
    # 'Snow density',
    # 'Snow depth',
    # 'Snow depth water equivalent',
    "Snow evaporation",
    "Snowfall",
    "Snowmelt",
    # 'Soil temperature level 1',
    # 'Soil temperature level 2',
    # 'Soil temperature level 3',
    # 'Soil temperature level 4',
    "Sub-surface runoff",
    "Surface latent heat flux",
    "Surface net solar radiation",
    "Surface net thermal radiation",
    # 'Surface pressure',
    "Surface runoff",
    "Surface sensible heat flux",
    "Surface solar radiation downwards",
    "Surface thermal radiation downwards",
    # 'Temperature of snow layer',
    "Total precipitation"
    # 'Volumetric soil water layer 1',
    # 'Volumetric soil water layer 2',
    # 'Volumetric soil water layer 3',
    # 'Volumetric soil water layer 4'
]


def open_dataset(
    data_variables=variables, start_time=start, end_time=end, bbox=box, time_chunks=24
):
    """
    从minio服务器读取era5-land数据

    Args:
        data_variables (list): 数据变量列表
        start_time (datetime64): 开始时间
        end_time (datetime64): 结束时间
        bbox (list|tuple): 四至范围
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """

    chunks = {"time": time_chunks}
    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        chunks=chunks,
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fs.open(f"s3://{bucket_name}/geodata/era5_land/era5_land_.json"),
                "remote_protocol": "s3",
                "remote_options": ro,
            },
        },
    )

    ds = ds.filter_by_attrs(long_name=lambda v: v in data_variables)
    ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    ds = ds.transpose("time", "lon", "lat")

    start_time = max(start_time, start)
    end_time = min(end_time, end)
    times = slice(start_time, end_time)
    ds = ds.sel(time=times)

    bbox = regen_box(bbox, 0.1, 0)

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


import geopandas as gpd


def from_shp(
    data_variables=variables, start_time=start, end_time=end, shp=None, time_chunks=24
):
    """
    通过已有的矢量数据范围从minio服务器读取era5-land数据

    Args:
        data_variables (list): 数据变量列表
        start_time (datetime64): 开始时间
        end_time (datetime64): 结束时间
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

    return open_dataset(data_variables, start_time, end_time, bbox, time_chunks)


def from_aoi(
    data_variables=variables,
    start_time=start,
    end_time=end,
    aoi: gpd.GeoDataFrame = None,
    time_chunks=24,
):
    """
    用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取era5-land数据

    Args:
        data_variables (list): 数据变量列表
        start_time (datetime64): 开始时间
        end_time (datetime64): 结束时间
        aoi (GeoDataFrame): 已有的GeoPandas.GeoDataFrame对象
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """

    b = aoi.bounds
    bbox = regen_box(
        (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]), 0.1, 0
    )

    return open_dataset(data_variables, start_time, end_time, bbox, time_chunks)


from netCDF4 import Dataset, date2num, num2date
import time
from datetime import datetime, timedelta


def _creatspinc(value, data_vars, lats, lons, starttime, filename, resolution):
    gridspi = Dataset(filename, "w", format="NETCDF4")

    # dimensions
    gridspi.createDimension("time", value[0].shape[0])
    gridspi.createDimension("lat", value[0].shape[2])  # len(lat)
    gridspi.createDimension("lon", value[0].shape[1])

    # Create coordinate variables for dimensions
    times = gridspi.createVariable("time", np.float64, ("time",))
    latitudes = gridspi.createVariable("lat", np.float32, ("lat",))
    longitudes = gridspi.createVariable("lon", np.float32, ("lon",))

    # Create the actual variable
    for var, attr in data_vars.items():
        gridspi.createVariable(
            var,
            np.float32,
            (
                "time",
                "lon",
                "lat",
            ),
        )

    # Global Attributes
    gridspi.description = "var"
    gridspi.history = f"Created {time.ctime(time.time())}"
    gridspi.source = "netCDF4 python module tutorial"

    # Variable Attributes
    latitudes.units = "degree_north"
    longitudes.units = "degree_east"
    times.units = "days since 1970-01-01 00:00:00"
    times.calendar = "gregorian"

    # data
    latitudes[:] = lats
    longitudes[:] = lons

    # Fill in times
    dates = []
    if resolution == "daily":
        dates.extend(starttime + n for n in range(value[0].shape[0]))
        times[:] = dates[:]

    elif resolution == "6-hourly":
        # for n in range(value[0].shape[0]):
        #     dates.append(starttime + (n+1) * np.timedelta64(6, 'h'))

        dates.extend(
            starttime + (n + 1) * timedelta(hours=6) for n in range(value[0].shape[0])
        )
        times[:] = date2num(dates, units=times.units, calendar=times.calendar)
        # print 'time values (in units %s): ' % times.units +'\n', times[:]
        dates = num2date(times[:], units=times.units, calendar=times.calendar)

    # Fill in values
    i = 0
    for var, attr in data_vars.items():
        gridspi.variables[var].long_name = attr["long_name"]
        gridspi.variables[var].units = attr["units"]
        gridspi.variables[var][:] = value[i][:]
        i = i + 1

    gridspi.close()


def to_netcdf(
    data_variables=variables,
    start_time=start,
    end_time=end,
    shp=None,
    resolution="hourly",
    save_file="era5.nc",
    time_chunks=24,
):
    """
    读取数据并保存为本地nc文件

    Args:
        data_variables (list): 数据变量列表
        start_time (datetime64): 开始时间
        end_time (datetime64): 结束时间
        shp (str): 已有的矢量数据路径
        resolution (str): 输出的时间分辨率
        save_file (str): 输出的文件路径
        time_chunks (int): 分块数量

    Returns:
        dataset (Dataset): 读取结果
    """

    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = regen_box(
        (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]), 0.1, 0
    )

    if resolution == "hourly":
        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)

        if ds.to_netcdf(save_file) is None:
            print(save_file, "已生成")
            ds = xr.open_dataset(save_file)
            return ds

    if resolution == "daily":
        start_time = np.datetime64(f"{str(start_time)[:10]}T01:00:00.000000000")
        end_time = np.datetime64(str(end_time)[:10]) + 1
        end_time = np.datetime64(f"{str(end_time)}T00:00:00.000000000")

        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)

        days = ds["time"].size // 24

        data_vars = {k: v.attrs for k, v in ds.data_vars.items()}
        daily_arr = []

        for var, attr in data_vars.items():
            a = ds[var].to_numpy()

            if attr["long_name"] in accumulated:
                xlist = [x for x in range(a.shape[0]) if x % 24 != 23]
                _a = np.delete(a, xlist, axis=0)

                daily_arr.append(_a)

            else:
                r = np.split(a, days, axis=0)
                _r = [
                    np.expand_dims(np.mean(r[i], axis=0), axis=0) for i in range(len(r))
                ]
                __r = np.concatenate(_r)

                daily_arr.append(__r)

        lats = ds["lat"].to_numpy()
        lons = ds["lon"].to_numpy()

        start_time = np.datetime64(str(start_time)[:10])

        _creatspinc(daily_arr, data_vars, lats, lons, start_time, save_file, "daily")

        new = xr.open_dataset(save_file)
        print(save_file, "已生成")
        return new

    if resolution == "6-hourly":
        start_time = np.datetime64(f"{str(start_time)[:10]}T01:00:00.000000000")
        end_time = np.datetime64(str(end_time)[:10]) + 1
        end_time = np.datetime64(f"{str(end_time)}T00:00:00.000000000")

        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)

        days = ds["time"].size // 6

        data_vars = {k: v.attrs for k, v in ds.data_vars.items()}
        daily_arr = []

        for var, attr in data_vars.items():
            a = ds[var].to_numpy()

            if attr["long_name"] in accumulated:
                xlist = [x for x in range(a.shape[0]) if x % 6 != 5]
                _a = np.delete(a, xlist, axis=0)

                daily_arr.append(_a)

            else:
                r = np.split(a, days, axis=0)
                _r = [
                    np.expand_dims(np.mean(r[i], axis=0), axis=0) for i in range(len(r))
                ]
                __r = np.concatenate(_r)

                daily_arr.append(__r)

        lats = ds["lat"].to_numpy()
        lons = ds["lon"].to_numpy()

        # start_time = np.datetime64(f'{str(start_time)[:10]}')
        year = int(f"{str(start_time)[:4]}")
        month = int(f"{str(start_time)[5:7]}")
        day = int(f"{str(start_time)[8:10]}")
        dt = datetime(year, month, day, 0, 0, 0)

        _creatspinc(daily_arr, data_vars, lats, lons, dt, save_file, "6-hourly")

        new = xr.open_dataset(save_file)
        print(save_file, "已生成")
        return new
