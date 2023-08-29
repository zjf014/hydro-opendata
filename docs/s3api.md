# S3api模块

读取数据。

## 功能

通过给定的AOI范围：
- 读取[MinIO](http://minio.waterism.com:9090/)服务器上的数据，如era5、gpm、gfs等；
- 通过[STAC](https://stacspec.org/en/)读取landsat、sentinel等遥感数据。

## 使用

- 读取era5数据

```python
from hydro_opendata.s3api import era5
import numpy as np

start_time=np.datetime64("2023-06-01T00:00:00.000000000")
end_time=np.datetime64("2023-06-30T23:00:00.000000000")

// 通过指定四至范围读取
bbox=(121,39,123,40)
ds1 = era5.open_dataset(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, bbox=bbox)

// 通过矢量数据文件读取
shp = 'basin.shp'
ds2 = era5.from_shp(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, shp=shp)

// 通过已有aoi对象读取
aoi = gpd.read_file(shp)
ds3 = era5.from_aoi(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, aoi=aoi)
```

- 读取gpm数据

```python
from hydro_opendata.s3api import gpm
import numpy as np

start_time=np.datetime64("2023-06-01T00:00:00.000000000")
end_time=np.datetime64("2023-06-30T23:30:00.000000000")

// 通过指定四至范围读取
bbox=(121,39,123,40)
ds1 = gpm.open_dataset(start_time=start_time, end_time=end_time, bbox=bbox)

// 通过矢量数据文件读取
shp = 'basin.shp'
ds2 = era5.from_shp(start_time=start_time, end_time=end_time, shp=shp)

// 通过已有aoi对象读取
aoi = gpd.read_file(shp)
ds3 = era5.from_aoi(start_time=start_time, end_time=end_time, aoi=aoi)
```

- 读取gpm数据

```python
from hydro_opendata.s3api import gfs
import numpy as np

creation_date=np.datetime64("2023-06-01")

// 通过指定四至范围读取
bbox=(121,39,123,40)
ds1 = gpm.open_dataset(data_variable='tp', creation_date=creation_date, creation_time='00', bbox=bbox)

// 通过矢量数据文件读取
shp = 'basin.shp'
ds2 = era5.from_shp(data_variable='tp', creation_date=creation_date, creation_time='00', shp=shp)

// 通过已有aoi对象读取
aoi = gpd.read_file(shp)
ds3 = era5.from_aoi(data_variable='tp', creation_date=creation_date, creation_time='00', aoi=aoi)
```

