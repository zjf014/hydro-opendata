# Stac模块

数据清单及其元数据，根据AOI返回数据列表。

## 功能

通过给定的AOI范围：
- 返回[MinIO](http://minio.waterism.com:9090/)服务器上可用的数据列表及数据清单；
- 通过[STAC](https://stacspec.org/en/)获取部分遥感数据（如landsat、sentinel）列表及数据清单

## 使用

- 通过给定aoi获取可用数据列表
```python
from hydro_opendata.stac.minio import ERA5LCatalog, GPMCatalog, GFSCatalog

era5 = ERA5LCatalog()
e = era5.search(aoi=aoi)

gpm = GPMCatalog()
g = gpm.search(aoi=aoi)

gfs = GFSCatalog('tp')    // 目前只支持降雨数据
f = gfs.search(aoi=aoi)
```

- 通过给定aoi和时间范围获取landast数据清单

- 通过给定aoi和时间范围获取sentinel数据清单

## 部分数据详情

### ECMWF Reanalysis v5
- [ERA5-Land](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5-land) -> era5: 来自European Centre for Medium-Range Weather Forecasts(ECMWF)提供的Copernicus Climate Change Service，是一种再分析(reanalysis)数据集
    - 空间分辨率：0.1°，约9km
    - 空间范围: 全球
    - 时间分辨率：逐小时、逐日、逐月
    - 时间范围：1950年至最近2-3月
    - 变量类型：包括地表面热辐射、温度、降雨、风、土壤、蒸发等[50个变量](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview)

### The Global Forecast System
- [GFS](https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php) -> gfs: 来自National Centers for Environmental Prediction (NCEP)的全球气象预测数据
    - 空间分辨率：0.25°、0.5°、1°
    - 空间范围: 全球
    - 时间分辨率：每天发布4次数据，分别于0点、6点、12点和18点发布；每次发布未来0-384小时的预测数据，其中，前5天为逐小时，6-16天小时为每3小时间隔
    - 时间范围：NCEP官网为近10天完整数据，Google Earth Engine(GEE)有2015年7月1日至今的数据，不过从2016年7月10日后是完整时间序列的
    - 变量类型：完整数据包括地表至平流层各层的气象数据[变量](https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml)，由于变量较多，GEE选择了其中的[9个](https://blog.csdn.net/qq_31988139/article/details/120589149)

### Global Precipitation Measurement
- [GPM](https://www.nasa.gov/mission_pages/GPM/main/index.html) -> gpm: 来自NASA和JAXA的全球降水测量计划
    - 空间分辨率：0.1°
    - 空间范围: 全球
    - 时间分辨率：0.5h、逐日、逐月
    - 时间范围：2000年6月以来
    - 变量类型：GPM IMERG是综合反演产品，包括Early，Late和Final Run，这三者的区别在于发布时间。Early Run表示获得观测数据后6h发布，Late和Final Run分别在18小时以及4个月后发布。Early和Late Run的IMERG产品主要是几乎实时的观测，并且用气候站进行检验校准。Final Run则是利用全球降水气候中心的月平均站点资料进行误差订正。