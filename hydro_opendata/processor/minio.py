import ujson
from ..common import fs, ro
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr
import kerchunk.netCDF3
import geopandas as gpd
import os
import shutil
import boto3
from hydroutils.hydro_s3 import boto3_upload_file, boto3_download_file

so = dict(
    mode="rb", storage_options=ro, default_fill_cache=False, default_cache_type="first"
)  # args to fs.open()
# default_fill_cache=False avoids caching data in between file chunks to lowers memory usage.


class HDFProcessor:
    """
    适用于gpm等Hdf5格式数据。

    Attributes:

    Method:
    """

    def __init__(self):
        pass

    def nc_to_zarr(self, nc_path, json_path):
        with fs.open(nc_path, **so) as infile:
            try:
                h5chunks = kerchunk.hdf.SingleHdf5ToZarr(infile, nc_path)
            except Exception:
                print(nc_path, "未生成！")
                return

            with fs.open(json_path, "wb") as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())

    def multi_to_zarr(
        self, json_paths, file_path, concat_dims=None, identical_dims=None
    ):
        if concat_dims is None:
            concat_dims = ["time"]
        if identical_dims is None:
            identical_dims = ["lat", "lon"]
        json_list = fs.glob(json_paths)
        json_list = [f"s3://{str}" for str in json_list]

        mzz = MultiZarrToZarr(
            json_list,
            target_options=ro,
            remote_protocol="s3",
            remote_options=ro,
            concat_dims=concat_dims,
            identical_dims=identical_dims,
        )

        d = mzz.translate()

        with fs.open(file_path) as f:
            f.write(ujson.dumps(d).encode())


class NC3Processor:
    """
    适用于era5等netCDF3格式数据。

    Attributes:

    Method:
    """

    def __init__(self):
        pass

    def nc_to_zarr(self, nc_path, json_path):
        try:
            h5chunks = kerchunk.netCDF3.netcdf_recording_file(
                f"s3://{nc_path}", storage_options=ro
            )
        except Exception:
            print(nc_path, "未生成！")
            return

        with fs.open(json_path, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())

    def multi_to_zarr(
        self, json_paths, file_path, concat_dims=None, identical_dims=None
    ):
        if concat_dims is None:
            concat_dims = ["time"]
        if identical_dims is None:
            identical_dims = ["latitude", "longitude"]
        json_list = fs.glob(json_paths)
        json_list = [f"s3://{str}" for str in json_list]

        mzz = MultiZarrToZarr(
            json_list,
            target_options=ro,
            remote_protocol="s3",
            remote_options=ro,
            concat_dims=concat_dims,
            identical_dims=identical_dims,
        )

        d = mzz.translate()

        with fs.open(file_path) as f:
            f.write(ujson.dumps(d).encode())


def geojson_to_shp(input_geojson, output_folder=None, keep_folder=True):
    """Trans geojson to shp and zip it, return the path of zip file"""
    gdf = gpd.read_file(input_geojson)

    # 根据输入的GeoJSON文件确定输出路径
    if not output_folder:
        output_folder = os.path.join(os.path.dirname(input_geojson), "shp_output")

    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 写入Shapefile
    base_name = os.path.splitext(os.path.basename(input_geojson))[0]
    output_shp = os.path.join(output_folder, f"{base_name}.shp")
    gdf.to_file(output_shp)

    # 将Shapefile及其相关文件打包成ZIP文件
    archive_path = shutil.make_archive(
        os.path.join(os.path.dirname(output_folder), base_name), "zip", output_folder
    )

    # 根据参数决定是否删除子文件夹
    if not keep_folder:
        shutil.rmtree(output_folder)

    return archive_path


class GeoProcessor:
    def __init__(self, minio_paras):
        self.boto = boto3.client(
            "s3",
            endpoint_url=minio_paras["endpoint_url"],
            aws_access_key_id=minio_paras["access_key"],
            aws_secret_access_key=minio_paras["secret_key"],
        )
        self.bucket_name = minio_paras["bucket_name"]
        self.endpoint = minio_paras["endpoint_url"].replace("http://", "")

    def upload_geojson(self, gj_local_path, gj_mo_name=None, upload_shp=True):
        """Upload geojson file and optionally its shpfile version."""
        # 如果gj_mo_name没有提供，则使用gj_local_path的文件名
        if gj_mo_name is None:
            gj_mo_name = os.path.basename(gj_local_path)

        if upload_shp:
            shp_zip_path = geojson_to_shp(gj_local_path)
            zip_name = os.path.basename(shp_zip_path)
            boto3_upload_file(self.boto, self.bucket_name, zip_name, shp_zip_path)

    def read_shp(self, shp_name):
        """Read shpfile from minio."""
        return gpd.read_file(
            f"zip+http://{self.endpoint}/{self.bucket_name}/{shp_name}"
        )

    def download_shp(self, shp_name, local_path):
        """Download shpfile from minio."""
        boto3_download_file(self.boto, self.bucket_name, shp_name, local_path)
