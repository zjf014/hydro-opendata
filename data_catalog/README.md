# 数据目录

归纳总结了wis中外部数据的数据来源、空间范围和时间范围等信息，目前包括：

- [Digital Elevation/Surface Model](#digital-elevation/surface-model)
- [ECMWF Reanalysis v5 - Land](#ecmwf-reanalysis-v5)
- [The Global Forecast System](#the-global-forecast-system)
- [Global Precipitation Measurement](#global-precipitation-measurement)



## Digital Elevation/Surface Model
- [ALOS World 3D - 30m](https://www.eorc.jaxa.jp/ALOS/en/dataset/aw3d30/aw3d30_e.htm) -> [alos-dem](./geodata/alos30/): 来自日本宇宙航空航天所(JAXA)的全球DEM数据，空间分辨率约30m，目前已下载松辽流域数据
- [Copernicus DEM GLO-30](https://spacedata.copernicus.eu/explore-more/news-archive/-/asset_publisher/Ye8egYeRPLEs/blog/id/434960) -> [copernicus-dem](./geodata/clo30/): 来自欧洲航天局(ESA)的全球dem数据，空间分辨率约30m，目前已下载松辽流域数据
- [SRTM DEM](https://www.earthdata.nasa.gov/sensors/srtm)
    - [SRTMGL1](https://lpdaac.usgs.gov/products/srtmgl1v003/) -> [srtm30](./geodata/srtm30/): 来自美国航空航天局(NASA)和美国国防部国家测绘局(NIMA)的全球dem数据，空间分辨率约30m，目前已下载全国范围数据
    - [SRTMGL3](https://lpdaac.usgs.gov/products/srtmgl3v003/) -> [srtm90](./geodata/srtm90/): 来自美国航空航天局(NASA)和美国国防部国家测绘局(NIMA)的全球dem数据，空间分辨率约90m，目前已下载全国范围数据
- [MERIT Hydro Flow Direction Map](http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/) -> [dir90](./geodata/dir90/): 来自东京大学的全球水文流向数据，空间分辨率90m，目前已下载全国范围数据，可用于生成子流域


## ECMWF Reanalysis v5
- [ERA5-Land](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5-land) -> [era5](./geodata/era5/): 来自European Centre for Medium-Range Weather Forecasts(ECMWF)提供的Copernicus Climate Change Service，是一种再分析(reanalysis)数据集
    - 空间分辨率：0.1°，约9km
    - 空间范围: 全球
    - 时间分辨率：逐小时、逐日、逐月
    - 时间范围：1950年至最近2-3月
    - 变量类型：包括地表面热辐射、温度、降雨、风、土壤、蒸发等[50个变量](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview)
    - 目前已下载松辽流域2015年7月-2022年4月的全变量ERA5-Land数据

## The Global Forecast System
- [GFS](https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php) -> [gfs](./geodata/gfs/): 来自National Centers for Environmental Prediction (NCEP)的全球气象预测数据
    - 空间分辨率：0.25°、0.5°、1°
    - 空间范围: 全球
    - 时间分辨率：每天发布4次数据，分别于0点、6点、12点和18点发布；每次发布未来0-384小时的预测数据，其中，前5天为逐小时，6-16天小时为每3小时间隔
    - 时间范围：NCEP官网为近10天完整数据，Google Earth Engine(GEE)有2015年7月1日至今的数据，不过从2016年7月10日后是完整时间序列的
    - 变量类型：完整数据包括地表至平流层各层的气象数据[变量](https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml)，由于变量较多，GEE选择了其中的[9个](https://blog.csdn.net/qq_31988139/article/details/120589149)
    - 目前已下载松辽流域2016年7月至今的9个变量GFS气象预测数据，每组预测序列为0-120小时

## Global Precipitation Measurement
- [GPM](https://www.nasa.gov/mission_pages/GPM/main/index.html) -> [gpm](./geodata/gpm/): 来自NASA和JAXA的全球降水测量计划
    - 空间分辨率：0.1°
    - 空间范围: 全球
    - 时间分辨率：0.5h、逐日、逐月
    - 时间范围：2000年6月以来
    - 变量类型：GPM IMERG是综合反演产品，包括Early，Late和Final Run，这三者的区别在于发布时间。Early Run表示获得观测数据后6h发布，Late和Final Run分别在18小时以及4个月后发布。Early和Late Run的IMERG产品主要是几乎实时的观测，并且用气候站进行检验校准。Final Run则是利用全球降水气候中心的月平均站点资料进行误差订正。
    - 目前已下载松辽流域2015年7月至今GFS气象预测数据