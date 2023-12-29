import os
import numpy as np
import xarray as xr
from datetime import datetime
import calendar
import dask
import json
import geopandas as gpd

from ..common import minio_paras, fs, ro
from ..utils import regen_box, creatspinc

bucket_name = minio_paras["bucket_name"]
dask.config.set({"array.slicing.split_large_chunks": False})


class ERA5LReader:
    """
    用于从minio中读取era5-land数据

    Methods:
        open_dataset(data_variables, start_time, end_time, dataset, bbox): 从minio中读取era5-land数据
        from_shp(data_variables, start_time, end_time, dataset, shp): 通过已有的矢量数据范围从minio服务器读取era5-land数据
        from_aoi(data_variables, start_time, end_time, dataset, aoi): 用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取era5-land数据
        to_netcdf(data_variables, start_time, end_time, dataset, shp, resolution, save_file): 读取数据并保存为本地nc文件
    """

    def __init__(self):
        self._variables = [
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

        self._accumulated = [
            "Evaporation",
            "Evaporation from bare soil",
            "Evaporation from open water surfaces excluding oceans",
            "Evaporation from the top of canopy",
            "Evaporation from vegetation transpiration",
            "Potential evaporation",
            "Runoff",
            "Snow evaporation",
            "Snowfall",
            "Snowmelt",
            "Sub-surface runoff",
            "Surface latent heat flux",
            "Surface net solar radiation",
            "Surface net thermal radiation",
            "Surface runoff",
            "Surface sensible heat flux",
            "Surface solar radiation downwards",
            "Surface thermal radiation downwards",
            "Total precipitation",
        ]

    def open_dataset(
        self,
        data_variables=["Total precipitation"],
        start_time=None,
        end_time=None,
        dataset="wis",
        bbox=None,
        time_chunks=24,
    ):
        """
        从minio服务器读取era5-land数据

        Args:
            data_variables (list): 数据变量列表
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            bbox (list|tuple): 四至范围
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        if end_time <= start_time:
            raise Exception("结束时间不能早于开始时间")

        if bbox[0] > bbox[2] or bbox[1] > bbox[3]:
            raise Exception("四至范围格式错误")

        if dataset != "wis" and dataset != "camels":
            raise Exception("dataset参数错误")

        if dataset == "wis":
            self._dataset = "geodata"
        elif dataset == "camels":
            self._dataset = "camdata"

        with fs.open(
            os.path.join(bucket_name, f"{self._dataset}/era5_land/era5l.json")
        ) as f:
            cont = json.load(f)
            self._starttime = np.datetime64(cont["start"])
            self._endtime = np.datetime64(cont["end"])
            self._bbox = cont["bbox"]

        chunks = {"time": time_chunks}
        ds = xr.open_dataset(
            "reference://",
            engine="zarr",
            chunks=chunks,
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    # no matter you run code in windows or linux, the bucket's format should be Linux style
                    # so we don't use os.join.path
                    "fo": f"s3://{bucket_name}/{self._dataset}/era5_land/era5_land_.json",
                    "target_protocol": "s3",
                    "target_options": ro,
                    "remote_protocol": "file",
                    # "remote_options": ro,
                },
            },
        )

        ds = ds.filter_by_attrs(long_name=lambda v: v in data_variables)
        ds = ds.rename({"longitude": "lon", "latitude": "lat"})
        ds = ds.transpose("time", "lon", "lat")

        start_time = max(start_time, self._starttime)
        end_time = min(end_time, self._endtime)
        times = slice(start_time, end_time)
        ds = ds.sel(time=times)

        bbox = regen_box(bbox, 0.1, 0)

        if bbox[0] < self._bbox[0]:
            left = self._bbox[0]
        else:
            left = bbox[0]

        if bbox[1] < self._bbox[1]:
            bottom = self._bbox[1]
        else:
            bottom = bbox[1]

        if bbox[2] > self._bbox[2]:
            right = self._bbox[2]
        else:
            right = bbox[2]

        if bbox[3] > self._bbox[3]:
            top = self._bbox[3]
        else:
            top = bbox[3]

        longitudes = slice(left - 0.00001, right + 0.00001)
        latitudes = slice(bottom - 0.00001, top + 0.00001)

        ds = ds.sortby("lat", ascending=True)
        ds = ds.sel(lon=longitudes, lat=latitudes)

        return ds

    def from_shp(
        self,
        data_variables=["Total precipitation"],
        start_time=None,
        end_time=None,
        dataset="wis",
        shp=None,
        time_chunks=24,
    ):
        """
        通过已有的矢量数据范围从minio服务器读取era5-land数据

        Args:
            data_variables (list): 数据变量列表
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            shp (str): 矢量数据路径
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        gdf = gpd.GeoDataFrame.from_file(shp)
        b = gdf.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0,
        )

        ds = self.open_dataset(
            data_variables, start_time, end_time, dataset, bbox, time_chunks
        )

        return self.open_dataset(
            data_variables, start_time, end_time, dataset, bbox, time_chunks
        )

    def from_aoi(
        self,
        data_variables=["Total precipitation"],
        start_time=None,
        end_time=None,
        dataset="wis",
        aoi: gpd.GeoDataFrame = None,
        time_chunks=24,
    ):
        """
        用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取era5-land数据

        Args:
            data_variables (list): 数据变量列表
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            aoi (GeoDataFrame): 已有的GeoPandas.GeoDataFrame对象
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        b = aoi.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0,
        )

        ds = self.open_dataset(
            data_variables, start_time, end_time, dataset, bbox, time_chunks
        )

        return ds

    def to_netcdf(
        self,
        data_variables=["Total precipitation"],
        start_time=None,
        end_time=None,
        dataset="wis",
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
            dataset (str): wis或camels
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
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0,
        )

        if resolution == "hourly":
            ds = self.open_dataset(
                data_variables, start_time, end_time, dataset, bbox, time_chunks
            )

            if ds.to_netcdf(save_file) == None:
                print(save_file, "已生成")
                ds = xr.open_dataset(save_file)
                return ds

        if resolution == "daily":
            start_time = np.datetime64(f"{str(start_time)[:10]}T01:00:00.000000000")
            end_time = np.datetime64(str(end_time)[:10]) + 1
            end_time = np.datetime64(f"{str(end_time)}T00:00:00.000000000")

            ds = self.open_dataset(
                data_variables, start_time, end_time, dataset, bbox, time_chunks
            )

            days = ds["time"].size // 24

            data_vars = {}
            for k, v in ds.data_vars.items():
                data_vars[k] = v.attrs

            daily_arr = []

            for var, attr in data_vars.items():
                a = ds[var].to_numpy()

                if attr["long_name"] in self._accumulated:
                    xlist = [x for x in range(a.shape[0]) if x % 24 != 23]
                    _a = np.delete(a, xlist, axis=0)

                    daily_arr.append(_a)

                else:
                    r = np.split(a, days, axis=0)
                    _r = [
                        np.expand_dims(np.mean(r[i], axis=0), axis=0)
                        for i in range(len(r))
                    ]
                    __r = np.concatenate(_r)

                    daily_arr.append(__r)

            lats = ds["lat"].to_numpy()
            lons = ds["lon"].to_numpy()

            start_time = np.datetime64(str(start_time)[:10])

            creatspinc(daily_arr, data_vars, lats, lons, start_time, save_file, "daily")

            new = xr.open_dataset(save_file)
            print(save_file, "已生成")
            return new

        if resolution == "6-hourly":
            start_time = np.datetime64(f"{str(start_time)[:10]}T01:00:00.000000000")
            end_time = np.datetime64(str(end_time)[:10]) + 1
            end_time = np.datetime64(f"{str(end_time)}T00:00:00.000000000")

            ds = self.open_dataset(
                data_variables, start_time, end_time, dataset, bbox, time_chunks
            )

            days = ds["time"].size // 6

            data_vars = {}
            for k, v in ds.data_vars.items():
                data_vars[k] = v.attrs

            daily_arr = []

            for var, attr in data_vars.items():
                a = ds[var].to_numpy()

                if attr["long_name"] in self._accumulated:
                    xlist = [x for x in range(a.shape[0]) if x % 6 != 5]
                    _a = np.delete(a, xlist, axis=0)

                    daily_arr.append(_a)

                else:
                    r = np.split(a, days, axis=0)
                    _r = [
                        np.expand_dims(np.mean(r[i], axis=0), axis=0)
                        for i in range(len(r))
                    ]
                    __r = np.concatenate(_r)

                    daily_arr.append(__r)

            lats = ds["lat"].to_numpy()
            lons = ds["lon"].to_numpy()

            # start_time = np.datetime64(f'{str(start_time)[:10]}')
            year = int(f"{str(start_time)[0:4]}")
            month = int(f"{str(start_time)[5:7]}")
            day = int(f"{str(start_time)[8:10]}")
            dt = datetime(year, month, day, 0, 0, 0)

            creatspinc(daily_arr, data_vars, lats, lons, dt, save_file, "6-hourly")

            new = xr.open_dataset(save_file)
            print(save_file, "已生成")
            return new


class GPMReader:
    """
    用于从minio中读取gpm数据

    Methods:
        open_dataset(start_time, end_time, dataset, bbox, time_resolution): 从minio中读取gpm数据
        from_shp(start_time, end_time, dataset, shp, time_resolution): 通过已有的矢量数据范围从minio服务器读取gpm数据
        from_aoi(start_time, end_time, dataset, aoi, time_resolution): 用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取gpm数据
    """

    def __init__(self):
        pass

    def _get_dataset(self, scale, start_time, end_time, bbox, time_chunks):
        year = str(start_time)[:4]
        month = str(start_time)[5:7].zfill(2)
        day = str(self._endtime)[8:10].zfill(2)

        if scale == "Y":
            minio_path = f"s3://{bucket_name}/{self._dataset}/gpm{self._time_resolution}/{year}/gpm{year}_inc.json"

        elif scale == "M":
            minio_path = f"s3://{bucket_name}/{self._dataset}/gpm{self._time_resolution}/{year}/{month}/gpm{year}{month}_inc.json"

        chunks = {"time": time_chunks}
        ds = xr.open_dataset(
            "reference://",
            engine="zarr",
            chunks=chunks,
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    "fo": minio_path,
                    "target_protocol": "s3",
                    "target_options": ro,
                    "remote_protocol": "s3",
                    "remote_options": ro,
                },
            },
        )

        # if self._time_resolution == '1d':
        #     ds = cf2datetime(ds)

        ds = ds["precipitationCal"]
        # ds.to_dataframe().filter(['precipitationCal','precipitationUncal']).to_xarray()

        # ds = ds.rename({"longitude": "lon", "latitude": "lat"})
        ds = ds.transpose("time", "lon", "lat")

        times = slice(start_time, end_time)
        ds = ds.sel(time=times)

        left = bbox[0]
        right = bbox[2]
        bottom = bbox[1]
        top = bbox[3]

        longitudes = slice(left - 0.00001, right + 0.00001)
        latitudes = slice(bottom - 0.00001, top + 0.00001)
        
        ds = ds.sortby("lat", ascending=True)
        ds = ds.sel(lon=longitudes, lat=latitudes)

        return ds

    def open_dataset(
        self,
        start_time=np.datetime64("2023-01-01T00:00:00.000000000"),
        end_time=np.datetime64("2023-01-02T00:00:00.000000000"),
        dataset="wis",
        bbox=(121, 39, 122, 40),
        time_resolution="1d",
        time_chunks=48,
    ):
        """
        从minio服务器读取gpm数据

        Args:
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            bbox (list|tuple): 四至范围
            time_resolution (str): 1d或30m
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        if end_time <= start_time:
            raise Exception("结束时间不能早于开始时间")

        if bbox[0] > bbox[2] or bbox[1] > bbox[3]:
            raise Exception("四至范围错误")

        if dataset != "wis" and dataset != "camels":
            raise Exception("dataset参数错误")

        if time_resolution != "1d" and time_resolution != "30m":
            raise Exception("time_resolution参数错误")

        # dataset_name = get_dataset_name()
        if dataset == "wis":
            self._dataset = "geodata"
        elif dataset == "camels":
            self._dataset = "camdata"

        if time_resolution == "1d":
            self._time_resolution = "1d"
        elif time_resolution == "30m":
            self._time_resolution = ""

        with fs.open(
            os.path.join(
                bucket_name,
                f"{self._dataset}/gpm{self._time_resolution}/gpm{self._time_resolution}.json",
            )
        ) as f:
            cont = json.load(f)
            self._starttime = np.datetime64(cont["start"])
            self._endtime = np.datetime64(cont["end"])
            self._bbox = cont["bbox"]

        if start_time < self._starttime:
            start_time = self._starttime

        if end_time > self._endtime:
            end_time = self._endtime

        bbox = regen_box(bbox, 0.1, 0.05)

        if bbox[0] < self._bbox[0]:
            bbox[0] = self._bbox[0]
        if bbox[1] < self._bbox[1]:
            bbox[1] = self._bbox[1]
        if bbox[2] > self._bbox[2]:
            bbox[2] = self._bbox[2]
        if bbox[3] > self._bbox[3]:
            bbox[3] = self._bbox[3]

        year_start = int(str(start_time)[:4])
        year_end = int(str(end_time)[:4])
        end_year = int(str(self._endtime)[:4])

        if year_end < end_year:
            if year_start == year_end:
                ds = self._get_dataset(
                    scale="Y",
                    start_time=start_time,
                    end_time=end_time,
                    bbox=bbox,
                    time_chunks=time_chunks,
                )
                return ds

            elif year_start < year_end:
                dss = []
                years = range(year_start, year_end + 1)
                for year in years:
                    if year == year_start:
                        dss.append(
                            self._get_dataset(
                                scale="Y",
                                start_time=start_time,
                                end_time=np.datetime64(
                                    f"{year}-12-31T23:30:00.000000000"
                                ),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )

                    elif year == year_end:
                        dss.append(
                            self._get_dataset(
                                scale="Y",
                                start_time=np.datetime64(
                                    f"{year}-01-01T00:00:00.000000000"
                                ),
                                end_time=end_time,
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )

                    else:
                        dss.append(
                            self._get_dataset(
                                scale="Y",
                                start_time=np.datetime64(
                                    f"{year}-01-01T00:00:00.000000000"
                                ),
                                end_time=np.datetime64(
                                    f"{year}-12-31T23:30:00.000000000"
                                ),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )
                return xr.merge(dss)

        else:
            month_end = int(str(end_time)[5:7])
            end_month = int(str(self._endtime)[5:7])

            if year_start == year_end:
                month_start = int(str(start_time)[5:7])
                if month_start == month_end:
                    return self._get_dataset(
                        scale="M",
                        start_time=start_time,
                        end_time=end_time,
                        bbox=bbox,
                        time_chunks=time_chunks,
                    )
                dss = []
                for m in range(month_start, month_end + 1):
                    if m == month_start:
                        d = calendar.monthrange(year_start, m)[1]
                        dss.append(
                            self._get_dataset(
                                scale="M",
                                start_time=start_time,
                                end_time=np.datetime64(
                                    f"{year_start}-{str(m).zfill(2)}-{str(d).zfill(2)}T23:30:00.000000000"
                                ),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )
                    elif m == month_end:
                        dss.append(
                            self._get_dataset(
                                scale="M",
                                start_time=np.datetime64(
                                    f"{year_start}-{str(m).zfill(2)}-01T00:00:00.000000000"
                                ),
                                end_time=end_time,
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )
                    else:
                        d = calendar.monthrange(year_start, m)[1]
                        dss.append(
                            self._get_dataset(
                                scale="M",
                                start_time=np.datetime64(
                                    f"{year_start}-{str(m).zfill(2)}-01T00:00:00.000000000"
                                ),
                                end_time=np.datetime64(
                                    f"{year_start}-{str(m).zfill(2)}-{str(d).zfill(2)}T23:30:00.000000000"
                                ),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )

            else:
                dss = []

                for y in range(year_start, year_end + 1):
                    if y == year_start:
                        dss.append(
                            self._get_dataset(
                                scale="Y",
                                start_time=start_time,
                                end_time=np.datetime64(f"{y}-12-31T23:30:00.000000000"),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )

                    elif y == year_end:
                        for m in range(1, month_end + 1):
                            if m == month_end:
                                dss.append(
                                    self._get_dataset(
                                        scale="M",
                                        start_time=np.datetime64(
                                            f"{str(y).zfill(4)}-{str(m).zfill(2)}-01T00:00:00.000000000"
                                        ),
                                        end_time=end_time,
                                        bbox=bbox,
                                        time_chunks=time_chunks,
                                    )
                                )
                            else:
                                d = calendar.monthrange(y, m)[1]
                                dss.append(
                                    self._get_dataset(
                                        scale="M",
                                        start_time=np.datetime64(
                                            f"{str(y).zfill(4)}-{str(m).zfill(2)}-01T00:00:00.000000000"
                                        ),
                                        end_time=np.datetime64(
                                            f"{str(y).zfill(4)}-{str(m).zfill(2)}-{str(d).zfill(2)}T23:30:00.000000000"
                                        ),
                                        bbox=bbox,
                                        time_chunks=time_chunks,
                                    )
                                )

                    else:
                        dss.append(
                            self._get_dataset(
                                scale="Y",
                                start_time=np.datetime64(
                                    f"{y}-01-01T00:00:00.000000000"
                                ),
                                end_time=np.datetime64(f"{y}-12-31T23:30:00.000000000"),
                                bbox=bbox,
                                time_chunks=time_chunks,
                            )
                        )

            return xr.merge(dss)

    def from_shp(
        self,
        start_time=np.datetime64("2023-01-01T00:00:00.000000000"),
        end_time=np.datetime64("2023-01-02T00:00:00.000000000"),
        dataset="wis",
        shp=None,
        time_resolution="1d",
        time_chunks=48,
    ):
        """
        通过已有的矢量数据范围从minio服务器读取gpm数据

        Args:
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            shp (str): 矢量数据路径
            time_resolution (str): 1d或30m
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        gdf = gpd.GeoDataFrame.from_file(shp)
        b = gdf.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0.05,
        )

        ds = self.open_dataset(
            start_time, end_time, dataset, bbox, time_resolution, time_chunks
        )
        return ds

    def from_aoi(
        self,
        start_time=np.datetime64("2023-01-01T00:00:00.000000000"),
        end_time=np.datetime64("2023-01-02T00:00:00.000000000"),
        dataset="wis",
        aoi: gpd.GeoDataFrame = None,
        time_resolution="1d",
        time_chunks=48,
    ):
        """
        用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取gpm数据

        Args:
            start_time (datetime64): 开始时间
            end_time (datetime64): 结束时间
            dataset (str): wis或camels
            aoi (GeoDataFrame): 已有的GeoPandas.GeoDataFrame对象
            time_resolution (str): 1d或30m
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        b = aoi.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0.05,
        )
        ds = self.open_dataset(
            start_time, end_time, dataset, bbox, time_resolution, time_chunks
        )
        return ds


class GFSReader:
    """
    用于从minio中读取gpm数据

    Attributes:
        variables (dict): 变量名称及缩写

    Methods:
        open_dataset(data_variables, creation_date, creation_time, bbox): 从minio中读取gfs数据
        from_shp(data_variables, creation_date, creation_time, shp): 通过已有的矢量数据范围从minio服务器读取gfs数据
        from_aoi(data_variables, creation_date, creation_time, aoi): 用过已有的GeoPandas.GeoDataFrame对象从minio服务器读取gfs数据
    """

    def __init__(self):
        self._variables = {
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

        self._default = "tp"

    @property
    def variables(self):
        return self._variables

    @property
    def default_variable(self):
        return self._default

    def set_default_variable(self, short_name):
        if short_name in self._variables.keys():
            self._default = short_name
        else:
            raise Exception("变量设置错误")

    def open_dataset(
        self,
        creation_date=np.datetime64("2022-09-01"),
        creation_time="00",
        dataset="wis",
        bbox=(115, 38, 136, 54),
        time_chunks=24,
    ):
        """
        从minio服务器读取gfs数据

        Args:
            creation_date (datetime64): 创建日期
            creation_time (datetime64): 创建时间，即00\06\12\18之一
            dataset (str): wis或camels
            bbox (list|tuple): 四至范围
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        if bbox[0] > bbox[2] or bbox[1] > bbox[3]:
            raise Exception("四至范围格式错误")

        if dataset != "wis" and dataset != "camels":
            raise Exception("dataset参数错误")

        if dataset == "wis":
            self._dataset = "geodata"
        elif dataset == "camels":
            self._dataset = "camdata"

        with fs.open(os.path.join(bucket_name, f"{self._dataset}/gfs/gfs.json")) as f:
            cont = json.load(f)
            self._paras = cont

        short_name = self._default
        full_name = self._variables[short_name]

        start = np.datetime64(self._paras[short_name][0]["start"])
        end = np.datetime64(self._paras[short_name][-1]["end"])

        if creation_date < start or creation_date > end:
            print("超出时间范围！")
            return

        if creation_time not in ["00", "06", "12", "18"]:
            print("creation_time必须是00、06、12、18之一！")
            return

        year = str(creation_date.astype("object").year)
        month = str(creation_date.astype("object").month).zfill(2)
        day = str(creation_date.astype("object").day).zfill(2)

        change = np.datetime64("2022-09-01")
        if creation_date < change:
            json_url = f"s3://{bucket_name}/{self._dataset}/gfs/gfs_history/{year}/{month}/{day}/gfs{year}{month}{day}.t{creation_time}z.0p25.json"
        else:
            json_url = f"s3://{bucket_name}/{self._dataset}/gfs/{short_name}/{year}/{month}/{day}/gfs{year}{month}{day}.t{creation_time}z.0p25.json"

        chunks = {"valid_time": time_chunks}
        ds = xr.open_dataset(
            "reference://",
            engine="zarr",
            chunks=chunks,
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    "fo": json_url,
                    "target_protocol": "s3",
                    "target_options": ro,
                    "remote_protocol": "s3",
                    "remote_options": ro,
                },
            },
        )

        if creation_date < change:
            ds = ds[full_name]
            box = self._paras[short_name][0]["bbox"]
        else:
            box = self.paras[short_name][-1]["bbox"]

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

    def from_shp(
        self,
        creation_date=np.datetime64("2022-09-01"),
        creation_time="00",
        dataset="wis",
        shp=None,
        time_chunks=24,
    ):
        """
        通过已有的矢量数据范围从minio服务器读取gfs数据

        Args:
            creation_date (datetime64): 创建日期
            creation_time (datetime64): 创建时间，即00\06\12\18之一
            dataset (str): wis或camels
            shp (str): 矢量数据路径
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """

        gdf = gpd.GeoDataFrame.from_file(shp)
        b = gdf.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0,
        )

        ds = self.open_dataset(creation_date, creation_time, dataset, bbox, time_chunks)

        return ds

    def from_aoi(
        self,
        creation_date=np.datetime64("2022-09-01"),
        creation_time="00",
        dataset="wis",
        aoi: gpd.GeoDataFrame = None,
        time_chunks=24,
    ):
        """
        通过已有的GeoPandas.GeoDataFrame对象从minio服务器读取gfs数据

        Args:
            creation_date (datetime64): 创建日期
            creation_time (datetime64): 创建时间，即00\06\12\18之一
            dataset (str): wis或camels
            aoi (GeoDataFrame): 已有的GeoPandas.GeoDataFrame对象
            time_chunks (int): 分块数量

        Returns:
            dataset (Dataset): 读取结果
        """
        b = aoi.bounds
        bbox = regen_box(
            (b.loc[0]["minx"], b.loc[0]["miny"], b.loc[0]["maxx"], b.loc[0]["maxy"]),
            0.1,
            0,
        )

        ds = self.open_dataset(creation_date, creation_time, dataset, bbox, time_chunks)

        return ds
