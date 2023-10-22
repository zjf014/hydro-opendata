"""
该模块用于获取minio服务器中的数据列表，包括：

- `Era5_land`
- `GPM_IMERG_Early`
- `GFS_atmos`
"""

from ..common import minio_paras, fs
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import json

bucket_name = minio_paras["bucket_name"]


class ERA5LCatalog:
    """
    用于获取era5-land的数据源信息，并搜索minio服务器中的数据范围

    Attributes:
        collection_id (str): 数据集名称
        data_sources (str): 数据源
        description (str): 数据源链接
        spatial_resolution (str): 空间分辨率
        temporal_resolution (str): 时间分辨率
        datasets (dict): minio服务器中的已有数据集

    Method:
        search(aoi, start_time, end_time): 搜索minio服务器中的数据范围
    """

    def __init__(self):
        self._collection_id = "era5-land"
        self._datasources = "ECMWF"
        self._description = "https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview"
        self._spatialresolution = "0.1 x 0.1; Native resolution is 9 km."
        self._temporalresolution = "hourly"

        self._datasets = self._get_datasets()

    def _get_datasets(self):
        dss = {}

        ds = {}
        with fs.open(os.path.join(bucket_name, "geodata/era5_land/era5l.json")) as f:
            era5 = json.load(f)
            ds["start_time"] = np.datetime64(era5["start"])
            ds["end_time"] = np.datetime64(era5["end"])
            ds["bbox"] = era5["bbox"]
        dss["wis"] = ds

        return dss

    @property
    def collection_id(self):
        return self._collection_id

    @property
    def data_sources(self):
        return self._datasources

    @property
    def description(self):
        return self._description

    @property
    def spatial_resolution(self):
        return self._spatialresolution

    @property
    def temporal_resolution(self):
        return self._temporalresolution

    @property
    def datasets(self):
        return self._datasets

    def search(self, aoi, start_time=None, end_time=None):
        """
        查询并获取数据清单

        Args:
            aoi (GeoDataFrame): 矢量数据范围
            strt_time (datatime64): 查询的起始时间
            end_time (datatime64): 查询的终止时间

        Returns:
            datalist (GeoDataFrame): 符合条件的数据清单
        """

        clips = []

        for key, value in self._datasets.items():
            start = start_time
            end = end_time
            if start_time is None:
                start = value["start_time"]
            if end_time is None:
                end = value["end_time"]

            if start < value["start_time"]:
                start = value["start_time"]
            if end > value["end_time"]:
                end = value["end_time"]

            if start <= end:
                df = pd.DataFrame(
                    {
                        "id": [self._collection_id],
                        "dataset": [key],
                        "start_time": [str(start)],
                        "end_time": [str(end)],
                        "geometry": [
                            f"POLYGON(({value['bbox'][0]} {value['bbox'][3]},{value['bbox'][0]} {value['bbox'][1]},\
                                     {value['bbox'][2]} {value['bbox'][1]},{value['bbox'][2]} {value['bbox'][3]},{value['bbox'][0]} {value['bbox'][3]}))"
                        ],
                    }
                )
                df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
                gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

                clips.append(gdf.clip(aoi))

        return pd.concat(clips)


class GPMCatalog:
    """
    用于获取gpm的数据源信息，并搜索minio服务器中的数据范围

    Attributes:
        collection_id (str): 数据集名称
        data_sources (str): 数据源
        description (str): 数据源链接
        spatial_resolution (str): 空间分辨率
        temporal_resolution (str): 时间分辨率
        datasets (dict): minio服务器中的已有数据集

    Method:
        search(aoi, start_time, end_time): 搜索minio服务器中的数据范围
    """

    def __init__(self):
        self._collection_id = "gpm-imerg-early"
        self._datasources = "NASA & JAXA"
        self._description = (
            "https://disc.gsfc.nasa.gov/datasets/GPM_3IMERGHHE_06/summary"
        )
        self._spatialresolution = "0.1 x 0.1; Native resolution is 9 km. (60°S-60°N)"
        self._temporalresolution = "half-hourly; 1 day"

        self._datasets = self._get_datasets()

    def _get_datasets(self):
        dss = {}
        lds = []
        ds = {}
        with fs.open(os.path.join(bucket_name, "geodata/gpm/gpm.json")) as f:
            gpm = json.load(f)
            ds["time_resolution"] = "30 minutes"
            ds["start_time"] = np.datetime64(gpm["start"])
            ds["end_time"] = np.datetime64(gpm["end"])
            ds["bbox"] = gpm["bbox"]
        lds.append(ds)

        ds = {}
        with fs.open(os.path.join(bucket_name, "geodata/gpm1d/gpm1d.json")) as f:
            gpm = json.load(f)
            ds["time_resolution"] = "1 day"
            ds["start_time"] = np.datetime64(gpm["start"])
            ds["end_time"] = np.datetime64(gpm["end"])
            ds["bbox"] = gpm["bbox"]
        lds.append(ds)
        dss["wis"] = lds

        lds = []
        ds = {}
        with fs.open(os.path.join(bucket_name, "camdata/gpm/gpm.json")) as f:
            gpm = json.load(f)
            ds["time_resolution"] = "30 minutes"
            ds["start_time"] = np.datetime64(gpm["start"])
            ds["end_time"] = np.datetime64(gpm["end"])
            ds["bbox"] = gpm["bbox"]
        lds.append(ds)

        ds = {}
        with fs.open(os.path.join(bucket_name, "camdata/gpm1d/gpm1d.json")) as f:
            gpm = json.load(f)
            ds["time_resolution"] = "1 day"
            ds["start_time"] = np.datetime64(gpm["start"])
            ds["end_time"] = np.datetime64(gpm["end"])
            ds["bbox"] = gpm["bbox"]
        lds.append(ds)
        dss["camels"] = lds

        return dss

    @property
    def collection_id(self):
        return self._collection_id

    @property
    def data_sources(self):
        return self._datasources

    @property
    def description(self):
        return self._description

    @property
    def spatial_resolution(self):
        return self._spatialresolution

    @property
    def temporal_resolution(self):
        return self._temporalresolution

    @property
    def datasets(self):
        return self._datasets

    def search(self, aoi, start_time=None, end_time=None):
        """
        查询并获取数据清单

        Args:
            aoi (GeoDataFrame): 矢量数据范围
            strt_time (datatime64): 查询的起始时间
            end_time (datatime64): 查询的终止时间

        Returns:
            datalist (GeoDataFrame): 符合条件的数据清单
        """

        clips = []

        for key, value in self._datasets.items():
            for v in value:
                start = start_time
                end = end_time
                if start_time is None:
                    start = v["start_time"]
                if end_time is None:
                    end = v["end_time"]

                if start < v["start_time"]:
                    start = v["start_time"]
                if end > v["end_time"]:
                    end = v["end_time"]

                if start <= end:
                    df = pd.DataFrame(
                        {
                            "id": [self._collection_id],
                            "dataset": [key],
                            "time_resolution": v["time_resolution"],
                            "start_time": [str(start)],
                            "end_time": [str(end)],
                            "geometry": [
                                f"POLYGON(({v['bbox'][0]} {v['bbox'][3]},{v['bbox'][0]} {v['bbox'][1]},\
                                         {v['bbox'][2]} {v['bbox'][1]},{v['bbox'][2]} {v['bbox'][3]},{v['bbox'][0]} {v['bbox'][3]}))"
                            ],
                        }
                    )
                    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
                    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

                    clips.append(gdf.clip(aoi))

        return pd.concat(clips)


class GFSCatalog:
    """
    用于获取gfs的数据源信息，并搜索minio服务器中的数据范围

    Attributes:
        collection_id (str): 数据集名称
        data_sources (str): 数据源
        description (str): 数据源链接
        spatial_resolution (str): 空间分辨率
        temporal_resolution (str): 时间分辨率
        datasets (dict): minio服务器中的已有数据集

    Method:
        search(aoi, start_time, end_time): 搜索minio服务器中的数据范围
    """

    def __init__(self, variable="tp"):
        self._variable = variable
        self._collection_id = f"gfs_atmos.{variable}"
        self._datasources = "NOAA"
        self._description = (
            "https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php"
        )
        self._spatialresolution = "0.25 x 0.25"
        self._temporalresolution = "hourly; 1-120h"

        self._datasets = self._get_datasets()

    def _get_datasets(self):
        dss = {}

        ds = {}
        with fs.open(os.path.join(bucket_name, "geodata/gfs/gfs.json")) as f:
            gfs = json.load(f)
        dss["wis"] = gfs[self._variable]

        return dss

    @property
    def variable(self):
        return self._variable

    @property
    def collection_id(self):
        return self._collection_id

    @property
    def data_sources(self):
        return self._datasources

    @property
    def description(self):
        return self._description

    @property
    def spatial_resolution(self):
        return self._spatialresolution

    @property
    def temporal_resolution(self):
        return self._temporalresolution

    @property
    def datasets(self):
        return self._datasets

    def search(self, aoi, start_time=None, end_time=None):
        """
        查询并获取数据清单

        Args:
            aoi (GeoDataFrame): 矢量数据范围
            start_time (datatime64): 查询的起始时间
            end_time (datatime64): 查询的终止时间

        Returns:
            datalist (GeoDataFrame): 符合条件的数据清单
        """

        clips = []

        for key, value in self._datasets.items():
            for v in value:
                start = start_time
                end = end_time
                if start_time is None:
                    start = np.datetime64(v["start"])
                if end_time is None:
                    end = np.datetime64(v["end"])

                if start < np.datetime64(v["start"]):
                    start = np.datetime64(v["start"])

                if end > np.datetime64(v["end"]):
                    end = np.datetime64(v["end"])

                if start <= end:
                    df = pd.DataFrame(
                        {
                            "id": [self._collection_id],
                            "dataset": [key],
                            "start_time": [str(start)],
                            "end_time": [str(end)],
                            "geometry": [
                                f"POLYGON(({v['bbox'][0]} {v['bbox'][3]},{v['bbox'][0]} {v['bbox'][1]},{v['bbox'][2]} {v['bbox'][1]},{v['bbox'][2]} {v['bbox'][3]},{v['bbox'][0]} {v['bbox'][3]}))"
                            ],
                        }
                    )
                    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
                    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

                    clips.append(gdf.clip(aoi))

        return pd.concat(clips)
