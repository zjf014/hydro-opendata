# wis-s3api


[![image](https://img.shields.io/pypi/v/wis-s3api.svg)](https://pypi.python.org/pypi/wis-s3api)
[![image](https://img.shields.io/conda/vn/conda-forge/wis-s3api.svg)](https://anaconda.org/conda-forge/wis-s3api)


**获取minio中的grid数据**


-   Free software: MIT license
-   Documentation: https://github.com/zjf014/hydro-opendata.git
    
## 安装

- 通过pip

```shell
pip install wis-s3api
```

- 通过源代码

```shell
# 下载源码
git clone http://gitlab.waterism.com:8888/zhujianfeng/wis-s3api.git
# 进入到源码目录
cd wis-s3api
# 安装依赖
pip install -r requirements.txt
# 安装wis-s3api
python setup.py install
```


## 功能及使用

直接从内网minio获取grid数据，返回xarray.Dataset或DataArray格式。数据包括：

- 获取[ERA5](http://gitlab.waterism.com:8888/zhujianfeng/wis-data-catalog#ecmwf-reanalysis-v5)数据
```python
from wis_s3api import era5

# 起止时间和四至范围
start_time = np.datetime64('2021-01-01T01:00:00.000000000')
end_time = np.datetime64('2021-02-01T00:00:00.000000000')
bbox = (121,38,122,40)

# 获取Dataset
ds = era5.open(['Total precipitation','10 metre U wind component'],start_time=start_time,end_time=end_time,bbox=bbox)
```


- 获取[GPM](http://gitlab.waterism.com:8888/zhujianfeng/wis-data-catalog#global-precipitation-measurement)数据
```python
from wis_s3api import gpm

# 起止时间和四至范围
start_time = np.datetime64('2021-01-01T01:00:00.000000000')
end_time = np.datetime64('2021-02-01T00:00:00.000000000')
bbox = (121,38,122,40)

# 获取Dataset
ds = gpm.open(start_time=start_time,end_time=end_time,bbox=bbox)
```


- 获取[GFS](http://gitlab.waterism.com:8888/zhujianfeng/wis-data-catalog#the-global-forecast-system)数据