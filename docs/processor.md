# Processor模块

进行写chunk等预处理操作。

## 原理

目前，数据下载后上传到[MinIO](http://minio.waterism.com:9090/)服务器中。

```
MinIO提供高性能、与S3兼容的对象存储系统。
可以使用Minio SDK，Minio Client，AWS SDK和 AWS CLI访问Minio服务器。
```

此阶段，数据在[MinIO](http://minio.waterism.com:9090/)仍然以文件的形式存储。

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

## 使用

代码尚未整理……

