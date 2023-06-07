# hydro-opendata


<!-- [![image](https://img.shields.io/pypi/v/hydro-opendata.svg)](https://pypi.python.org/pypi/hydro-opendata)
[![image](https://img.shields.io/conda/vn/conda-forge/hydro-opendata.svg)](https://anaconda.org/conda-forge/hydro-opendata) -->


**可用于水文学科学计算的开放数据的获取、管理和使用路径及方法。**


-   Free software: MIT license
-   Documentation: 
 
## 背景

在人工智能的大背景下，数据驱动的水文模型已得到广泛研究和应用。同时，得益于遥测技术发展和数据开放共享，获取数据变得容易且选择更多了。对于研究者而言，需要什么数据？能获取什么数据？从哪下载？怎么读取？如何处理？等一系列问题尤为重要，也是源平台数据中心建设需要解决的问题。

本仓库主要基于外部开放数据，梳理数据类别和数据清单，构建能够实现数据“下载-存储-处理-读写-可视化”的数据流及其技术栈。


## 数据流

![数据框架图](images/framework.png)

## 主要数据源

从现有认识来看，可用于水文建模的外部数据包括但不限于以下几类：

| **一级分类** | **二级分类** | **更新频率** | **数据结构** | **示例** |
| --- | --- | --- | --- | --- |
| 基础地理 | 水文要素 | 静态 | 矢量 | 流域边界、站点 |
|  | 地形地貌 | 静态 | 栅格 | [DEM](https://github.com/DahnJ/Awesome-DEM)、流向、土地利用 |
| 天气气象 | 再分析 | 动态 | 栅格 | ERA5 |
|  | 近实时 | 动态 | 栅格 | GPM |
|  | 预测 | 滚动 | 栅格 | GFS |
| 图像影像 | 卫星遥感 | 动态 | 栅格 | Landsat、Sentinel、MODIS |
|  | 街景图片 | 静态 | 多媒体 |  |
|  | 监控视频 | 动态 | 多媒体 |  |
|  | 无人机视频 | 动态 | 多媒体 |  |
| 众包数据 | POI | 静态 | 矢量 | 百度地图 |
|  | 社交网络 | 动态 | 多媒体 | 微博 |

从数据更新频率上来看，分为静态数据和动态数据。

从数据结构上看，分为矢量、栅格和多媒体数据等非结构化数据。

近期暂时只考虑现有的已下载的数据和卫星遥感影像。

目前，wis服务器已部署了[DEM](./wis-stac/catalog/README.md#digital-elevation/surface-model)、[ERA5-Land](./wis-stac/catalog/README.md#ecmwf-reanalysis-v5)、[GFS](./wis-stac/catalog/README.md#the-global-forecast-system)、[GPM](./wis-stac/catalog/README.md#global-precipitation-measurement)等数据。

## 代码仓

![代码仓](images/repos.jpg)

### wis-stac

数据清单及其元数据，能够根据AOI返回数据列表。


### wis-downloader

从外部数据源下载数据。根据数据源不同，下载方法不尽相同，主要包括：

- 通过集成官方提供的api，如[bmi_era5](https://github.com/gantian127/bmi_era5)
- 通过获取数据的下载链接，如[Herbie](https://github.com/blaylockbk/Herbie)、[MultiEarth](https://github.com/bair-climate-initiative/multiearth)、[Satpy](https://github.com/pytroll/satpy)，大部分云数据平台如Microsoft、AWS等数据组织的方式大多为[stac](https://github.com/radiantearth/stac-spec)

目前，[wis-downloader](./wis_downloader/)可从AWS、GEE、NCEP、ECCMWF等下载dem、gfs等数据。

### wis-processor

对数据进行预处理，如流域平局、提取特征值等。

目前，数据下载后上传到[MinIO](https://github.com/minio/minio)服务器中。

```
MinIO提供高性能、与S3兼容的对象存储系统。
可以使用Minio SDK，Minio Client，AWS SDK和 AWS CLI访问Minio服务器。
```

此阶段，数据在[MinIO](https://github.com/minio/minio)仍然以文件的形式存储。

- **存在问题：**
1. 如果文件很大读取效率低。
2. 跨文件读取不方便；

- **解决思路：**
写块（chunk）

- **实现目标：**
将数据转化为[zarr](https://zarr.readthedocs.io/en/stable/)格式

- **实现方法：**
使用[kerchunk](https://fsspec.github.io/kerchunk/)

```
简单说，kerchunk能够更高效地读取本地或s3（如minio）上的数据，
支持如NetCDF/HDF5, GRIB2, TIFF等部分格式的高效读取（解决问题1），
并且能够跨文件创建虚拟数据集（解决问题2）。
```
- [kerchunk](https://fsspec.github.io/kerchunk/)是通过[写JSON文件](./docs/examples/era5/step3%3A%20kerchunk.ipynb)的形式完成上述功能的。

### wis-s3api

数据在MinIO中经过kerchunk写块处理后，即可跨文件读取。只需要提供数据的类别、时间范围和空间范围等参数即可读取数据。

以[ERA5-Land](./data_catalog/README.md#ecmwf-reanalysis-v5)数据为例，[wis-s3api](./data_api/)提供了直接从MinIO读取数据的接口：
```python
from wis_s3api import era5
import numpy as np

start_time = np.datetime64('2021-01-01T01:00:00.000000000')
end_time = np.datetime64('2021-01-31T00:00:00.000000000')
bbox = (121,38,122,40)

ds = era5.open(
    ['Total precipitation','10 metre U wind component'],
    start_time=start_time,
    end_time=end_time,
    bbox=bbox
)
```

在[wis-s3api](http://gitlab.waterism.com:8888/zhujianfeng/wis-s3api)使用[xarray](https://github.com/pydata/xarray)等直接读取的数据量较大时容易造成内存溢出，建议分块读取, 例如：
```python
import xarray as xr

ds = xr.open_dataset(
        "reference://", 
        engine="zarr", 
        chunks=chunks,
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fs.open('s3://' + bucket_path + 'era5_land/era5_land.json'), 
                "remote_protocol": "s3",
                "remote_options": {
                    'client_kwargs': {'endpoint_url': endpoint_url}, 
                    'key': access_key, 
                    'secret': secret_key}
            }
        }      
    )
```

对于遥感影像数据，数据量大且多，无法逐一下载后读取。可以采用[stac+stackstac](./data_api/examples/RSImages.ipynb)直接将Sentinel或Landsat数据读入到xarray的dataset中。


### wis-gistools

集成一些常用的GIS工具，如克里金插值、泰森多边形等。

- 克里金插值
    - [PyKrige](https://github.com/GeoStat-Framework/PyKrige)
- 泰森多边形
    - [WhiteboxTools.VoronoiDiagram](https://whiteboxgeo.com/manual/wbt_book/available_tools/gis_analysis.html?highlight=voro#voronoidiagram)
    - [scipy.spatial.Voronoi](https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.Voronoi.html)
- 流域划分
    - [Rapid Watershed Delineation using an Automatic Outlet Relocation Algorithm](https://github.com/xiejx5/watershed_delineation)
    - [High-performance watershed delineation algorithm for GPU using CUDA and OpenMP](https://github.com/bkotyra/watershed_delineation_gpu)
- 流域平均
    - [plotting and creation of masks of spatial regions](https://github.com/regionmask/regionmask)

## 可视化

在Jupyter平台中使用[leafmap](https://github.com/giswqs/leafmap)展示地理空间数据。

## 其它

- [hydro-GIS资源目录](./resources/README.md)
