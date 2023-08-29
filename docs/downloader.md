# Downloader模块

提供通用下载器和部分数据的下载功能。


## 功能

- 通用下载器，可下载url链接
- 可下载dem、gfs等数据


## 使用

- 通用单任务下载器
```python
from hydro_opendata.downloader import downloader
# 下载url链接至本地
downloader.download_sigletasking(url,file_name)
```

- 下载alos30数据

```python
from hydro_opendata.downloader.dem import Alos_DEM
# 通过四至范围获取dem链接
dem = Alos_DEM()
hrefs = dem.search(bbox=[121,38,121.5,39])
# 下载所有dem链接
dem.download(save_dir='.')
```

- 下载nfs数据

```python
from hydro_opendata.downloader import ncep_gfs
# 通过日期、时间、预测序列及范围下载gfs数据
ncep_gfs.get_gfs_from_ncep(date='20220101',creation_time='06',forecast_time=120,bbox=[115,38,136,54])
```