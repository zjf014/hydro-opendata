# Hydro-GIS

水文水资源的科研和工程实践中一定缺不了GIS工具的使用，熟悉一些常用GIS工具十分有必要。

GIS工具类型繁多，有成套的GIS软件（ArcGIS, QGIS），遥感软件（ENVI），有成熟的3D应用、地图服务、GIS云计算（Google earth engine）等，也有基础的工具，如PostGIS数据库，leafletweb前端可视化工具等，还有最基本的地理空间计算库，如GDAL等。

本repo主要归纳和整理与水文水资源相关的GIS软件、工具和资源，内容大致如下：

## Table Of Contents:
- [桌面端软件](#桌面端软件)
- [云计算平台](#云计算平台)
- [PYTHON库](#PYTHON库)
- [GIS数据](#GIS数据)
- [其它](#其它)

## 桌面端软件
- [ArcGIS Desktop](https://www.esri.com/zh-cn/arcgis/products/arcgis-desktop/overview)：GIS软件的行业标杆，包括一系列软件，如ArcMap、ArcCatalog、ArcGlobe等，功能强大，有专门针对水文的工具。正版很贵，破解版也很流行。新一代桌面版为[ArcGIS Pro](https://www.esri.com/zh-cn/arcgis/products/arcgis-pro/overview)，加入了一些机器学习、云计算的东西。还有国产化版本[GeoScene](https://www.geoscene.cn/)。
- [QGIS](https://www.qgis.org/en/site/)：基本可以认为是免费版的ArcGIS，因为是开源的，所以不会像商业软件那样做的非常详尽，但是应对水文专业GIS应用还是足够的。
- [SuperMap iDesktop](http://support.supermap.com.cn/product/iDesktop.aspx)：国产超图软件，可以看作是ArcGIS的国产版本，功能也很强大。

## 云计算平台
- [Google Earth Engine](https://developers.google.cn/earth-engine/)：基于Google平台提供的一站式地理空间处理工具，数据丰富，功能强大，同时需要科学上网。
- [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/)：微软推出的GEE竞品，处于内测阶段。
- [AI Earth地球科学云平台](https://engine-aiearth.aliyun.com/)：国产GEE，刚上线不久。

## PYTHON库
- AutoGIS：主要参考[Automating GIS-processes](https://github.com/Automating-GIS-processes/site)，了解Python GIS 常用的开源库，并给一些实例，方便简单的GIS计算
    - [GDAL/OGR](https://gdal.org/index.html)：处理栅格、矢量数据的基础库。
    - [Shapely](https://github.com/shapely/shapely)：处理空间分析的基础库。
    - [Proj](https://proj.org/index.html)：处理地理投影的基础库。
    - [Fiona](https://github.com/Toblerity/Fiona)：相当于GDAL/GGR在处理矢量数据方面针对Python的优化版本。
    - [PyShp](https://github.com/GeospatialPython/pyshp)：针对shapefile格式数据的读写操作。
    - [NumPy](https://github.com/numpy/numpy)：科学计算基础包，处理矩阵，是GDAL、xarray、PyTorch等的基础。
    - [matplotlib](https://github.com/matplotlib/matplotlib)：强大的绘图工具。
    - [GeoPandas](https://github.com/geopandas/geopandas)：pandas的空间扩展，一般的矢量数据用它就够了。
    - [Rasterio](https://github.com/rasterio/rasterio)：栅格数据处理利器。
    - [xarray](https://github.com/pydata/xarray)：用于处理多维数组，支持netcd、grib等格式数据。
    - [xarray-spatial](https://github.com/makepath/xarray-spatial)：基于Numba，用于栅格数据的空间分析。
    - [PyKrige](https://github.com/GeoStat-Framework/PyKrige)：可实现各种克里金插值。
    - [WhiteboxTools](https://github.com/jblindsay/whitebox-tools)：提供地质、地貌、水文、GIS等处理工具。有ArcGIS、QGIS等平台插件。
- 源平台：依托源平台已开发、集成的python库
    - [wis-processor](http://124.93.160.156:18888/zhujianfeng/wis-processor)：常用的GIS工具，如普通克里金插值、泰森多边形等。
    - [wis-ftpapi](http://124.93.160.156:18888/zhujianfeng/wis-ftpapi)：源平台FTP服务器中数据读取的统一接口。
- 其它
    - [geemap](https://github.com/giswqs/geemap)：用于与GEE进行地图交互式编程的Python包。
    - [leafmap](https://github.com/giswqs/leafmap)：用于Jupyter环境的可交互式地理空间分析包。
    - [ArcGIS API for Python](https://developers.arcgis.com/python/)：ArcGIS的Python包，不是ArcPy，有点像leafmap。
    - [SuperMap iClient Python](https://iclientpy.supermap.io/)：超图的Python包。

## GIS数据
- 数据目录
    - 地形：如[DEM](https://github.com/DahnJ/Awesome-DEM)，包括[SRTM](https://www.earthdata.nasa.gov/sensors/srtm)、[ASTER](https://lpdaac.usgs.gov/products/astgtmv003/)、[ALOS](https://www.eorc.jaxa.jp/ALOS/en/dataset/aw3d30/aw3d30_e.htm)等；水文相关的[流向](http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/)等数据。
    - 遥感影像：如[Landsat](https://www.usgs.gov/landsat-missions)、[MODIS](https://modis.gsfc.nasa.gov/)、[Sentinel](https://sentinel.esa.int/web/sentinel/missions/)等。
    - 土地利用：如[ESA Land Cover](http://maps.elie.ucl.ac.be/CCI/viewer/)、[GlobeLand30](http://globeland30.org/)、[Global land cover](http://data.ess.tsinghua.edu.cn/)、[OSM Land Use](https://osmlanduse.org/)、[中国30米年度土地覆盖及其变化](https://zenodo.org/record/5816591#.Y1DaIvxBxPY)等。
    - 气候气象：如[ECMWF Reanalysis v5](https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5)、[Global Forecast System](https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast)、[Global Precipitation Measurement](https://gpm.nasa.gov/)等。
    - 水文：如[MERIT Hydro](http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/)、[HydroSHEDS](https://www.hydrosheds.org/)等。
    - 夜间灯光：如[DMSP/OLS](https://www.ngdc.noaa.gov/eog/dmsp.html)、[NPP/VIIRS](https://www.ngdc.noaa.gov/eog/viirs/index.html)、[珞珈一号](http://59.175.109.173:8888/app/login.html)等。
    - 其它：如[Open Street Map](https://www.openstreetmap.org/)、[DataV.GeoAtlas](http://datav.aliyun.com/portal/school/atlas/)等。

- 数据平台
    - [nasa](https://www.nasa.gov/)：美国国家航空航天局
    - [usgs](https://www.usgs.gov/)：美国地质勘探局
    - [esa](https://www.esa.int/)：欧洲航天局
    - [jaxa](https://global.jaxa.jp/)：日本宇宙航空航天所
    - [noaa](https://www.noaa.gov/)：美国国家海洋和大气管理局
    - [ecmwf](https://www.ecmwf.int/)：European Centre for Medium-Range Weather Forecasts
    - [gee](https://developers.google.cn/earth-engine/datasets)：Google Earth Engine
    - [aws](https://registry.opendata.aws/)：Amazon Web Services
    - [mpc](https://planetarycomputer.microsoft.com/catalog)：Microsoft Planetary Computer
    - [国家综合地球观测数据共享平台](http://old.chinageoss.cn/dsp/home/index.jsp)
    - [地理空间数据云](http://www.gscloud.cn/)
    - [wis](http://124.93.160.156:18888/zhujianfeng/wis-data-catalog)：源平台，已下载、集成了部分数据

## 其它
- 前端
    - [ArcGIS API for Javascript](https://developers.arcgis.com/javascript/latest/): ArcGIS的前端JS包，目前主要版本是3.x和4.x，其中4.x支持三维可视化。
    - [OpenLayers](https://openlayers.org/)：开源的JS包，用于二维WebGIS开发。
    - [leaflet](https://leafletjs.com/)：一个比较轻量级的前端JS包。
    - [CesiumJS](https://cesium.com/platform/cesiumjs/)：主要的三维WebGIS开发开源工具。
    - [天地图JS](http://lbs.tianditu.gov.cn/home.html)：国家队，用起来有点像ArcGIS API for JS
    - [百度地图JS](https://lbsyun.baidu.com/index.php?title=jspopularGL)：可以用百度地图的服务，点个点、定位一下、导航一记还是可以的。
    - [高德地图JS](https://lbs.amap.com/api/jsapi-v2/summary/)：同上。
    - [腾讯地图JS](https://lbs.qq.com/)：同上。
- 地图服务
    - [OSM]()
    - [MapBox]()
    - [天地图](http://lbs.tianditu.gov.cn/home.html)：地图API还是不错的，提供各种底图，还能加到比如QGIS或ArcGIS中。
    - [百度地图](https://lbsyun.baidu.com/)：地图API提供了一些酷炫的效果，Web服务能爬一些数据比如POI、街景等。
    - [高德地图](https://lbs.amap.com/)：同上。
    - [腾讯地图](https://lbs.qq.com/)：同上。
    - [ArcGIS Online](https://www.arcgis.com)：云GIS产品，可试用，提供一些常用的制图和空间分析功能。
    - [SuperMap Online](https://www.supermapol.com/)：云GIS产品，目前可免费使用，简单的制图和分析还是挺方便的。
- 空间数据库
    - [PostGIS](https://postgis.net)：基于PostgreSQL数据库的空间数据拓展插件，数据库和插件都开源。
    - [MySQL](https://www.mysql.com/)：最流行的关系数据库之一，目前也支持空间数据的存储。
    - [Oracle Spatial](https://www.oracle.com/cn/database/spatial/)：Oracle数据库的空间拓展，当然首先得买Oracle。
    - [ArcSDE]()：空间数据库引擎，传统的空间数据存储解决方案，支持在各种关系数据库的基础上储存和管理空间数据。
- GIS服务器
    - [ArcGIS Server](https://enterprise.arcgis.com/zh-cn/server/)：现在是ArcGIS Enterprise的一部分，用于发布地图服务等WebGIS服务，与ArcGIS Desktop配合使用，价格更贵。
    - [GeoServer](https://geoserver.org/)：开源的WebGIS服务器，基于J2EE实现。
    - [MapServer](https://mapserver.org/)：开源，NASA出品，功能可能弱于GeoServer。
    - [SuperMap iServer](http://support.supermap.com.cn:8090/iserver/)：和超图桌面端一样，集成了很多功能，支持机器学习服务。
- 在线资源
    - 公众号：GIScience
    - B站：[geemap](https://space.bilibili.com/527404442)、[Coursera公开课](https://space.bilibili.com/100880105)（有关于GIS的课程）
    - CSDN：[GEE数据集专栏](https://blog.csdn.net/qq_31988139/category_11336093.html)

## 参考
- [awesome-gis](https://github.com/sshuair/awesome-gis)