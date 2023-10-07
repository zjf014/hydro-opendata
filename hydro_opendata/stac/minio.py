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

        start_time (str): minio服务器中数据起始时间
        end_time (str): minio服务器中数据结束时间
        bbox (str): minio服务器中数据空间范围

    Method:
        search(aoi, start_time, end_time): 搜索minio服务器中的数据范围
    """

    def __init__(self):
        self._collection_id = "era5-land"
        self._datasources = "ECMWF"
        self._description = "https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview"
        self._spatialresolution = "0.1 x 0.1; Native resolution is 9 km."
        self._temporalresolution = "hourly"

        # with fs.open('test/geodata/era5_land.json/')
        self._starttime = np.datetime64("2015-07-01T00:00:00")
        self._endtime = np.datetime64("2021-12-31T23:00:00")
        self._bbox = (115, 38, 136, 54)

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
    def start_time(self):
        return self._starttime

    @property
    def end_time(self):
        return self._endtime

    @property
    def bbox(self):
        return self._bbox

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

        if start_time is None:
            start_time = self._starttime
        if end_time is None:
            end_time = self._endtime

        if start_time < end_time:
            if start_time < self._starttime:
                start_time = self._starttime
            if end_time > self._endtime:
                end_time = self._endtime

            df = pd.DataFrame(
                {
                    "id": [self._collection_id],
                    "start_time": [start_time],
                    "end_time": [end_time],
                    "geometry": ["POLYGON((115 54,115 38,136 38,136 54,115 54))"],
                }
            )
            df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

            clip = gdf.clip(aoi)

            return clip

        else:
            return None


class GPMCatalog:
    """
    用于获取gpm的数据源信息，并搜索minio服务器中的数据范围

    Attributes:
        collection_id (str): 数据集名称
        data_sources (str): 数据源
        description (str): 数据源链接
        spatial_resolution (str): 空间分辨率
        temporal_resolution (str): 时间分辨率

        start_time (str): minio服务器中数据起始时间
        end_time (str): minio服务器中数据结束时间
        bbox (str): minio服务器中数据空间范围

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
        self._temporalresolution = "half-hourly"

        with fs.open(f"{bucket_name}/geodata/gpm/gpm.json") as f:
            gpm = json.load(f)
            self._starttime = np.datetime64(gpm["start"])
            self._endtime = np.datetime64(gpm["end"])
            self._bbox = gpm["bbox"]

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
    def start_time(self):
        return self._starttime

    @property
    def end_time(self):
        return self._endtime

    @property
    def bbox(self):
        return self._bbox

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

        if start_time is None:
            start_time = self._starttime
        if end_time is None:
            end_time = self._endtime

        if start_time < end_time:
            if start_time < self._starttime:
                start_time = self._starttime
            if end_time > self._endtime:
                end_time = self._endtime

            df = pd.DataFrame(
                {
                    "id": [self._collection_id],
                    "start_time": [start_time],
                    "end_time": [end_time],
                    "geometry": [
                        f"POLYGON(({self._bbox[0]} {self._bbox[3]},{self._bbox[0]} {self._bbox[1]},{self._bbox[2]} {self._bbox[1]},{self._bbox[2]} {self._bbox[3]},{self._bbox[0]} {self._bbox[3]}))"
                    ],
                }
            )
            df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

            clip = gdf.clip(aoi)

            return clip

        else:
            return None


class GFSCatalog:
    """
    用于获取gfs的数据源信息，并搜索minio服务器中的数据范围

    Attributes:
        collection_id (str): 数据集名称
        data_sources (str): 数据源
        description (str): 数据源链接
        spatial_resolution (str): 空间分辨率
        temporal_resolution (str): 时间分辨率

        start_time (str): minio服务器中数据起始时间
        end_time (str): minio服务器中数据结束时间

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

        with fs.open(f"{bucket_name}/geodata/gfs/gfs.json") as f:
            gfs = json.load(f)
            self._starttime = np.datetime64(gfs[variable][0]["start"])
            self._endtime = np.datetime64(gfs[variable][-1]["end"])
            # self.bbox = gpm['bbox']

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
    def start_time(self):
        return self._starttime

    @property
    def end_time(self):
        return self._endtime

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

        if start_time is None:
            start_time = self._starttime
        if end_time is None:
            end_time = self._endtime

        if start_time < end_time:
            stime = []
            etime = []
            bbox = []
            ids = []

            with fs.open(f"{bucket_name}/geodata/gfs/gfs.json") as f:
                cont = json.load(f)
                clist = cont[self._variable]

            for c in clist:
                if start_time < np.datetime64(c["start"]):
                    stime.append(np.datetime64(c["start"]))
                else:
                    stime.append(start_time)

                if end_time > np.datetime64(c["end"]):
                    etime.append(np.datetime64(c["end"]))
                else:
                    etime.append(end_time)

                bb = c["bbox"]
                bbox.append(
                    f"POLYGON(({bb[0]} {bb[3]},{bb[0]} {bb[1]},{bb[2]} {bb[1]},{bb[2]} {bb[3]},{bb[0]} {bb[3]}))"
                )
                ids.append(self._collection_id)

            df = pd.DataFrame(
                {"id": ids, "start_time": stime, "end_time": etime, "geometry": bbox}
            )
            df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

            clip = gdf.clip(aoi)

            return clip

        else:
            return None
