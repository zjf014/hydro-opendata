import ujson
from ..common import fs, ro
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr
import kerchunk.netCDF3

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
