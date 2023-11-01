"""
Author: Jianfeng Zhu
Date: 2023-10-06 20:50:41
LastEditTime: 2023-10-27 20:48:08
LastEditors: Wenyu Ouyang
Description: Main module
FilePath: /hydro_opendata/hydro_opendata/common.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import pathlib
import os
import s3fs
from enum import Enum


def minio_cfg(bucket_name="test"):
    minio_paras = {
        "endpoint_url": "",
        "access_key": "",
        "secret_key": "",
        "bucket_name": bucket_name,
    }

    home_path = str(pathlib.Path.home())

    if os.path.exists(os.path.join(home_path, ".wisminio")):
        for line in open(os.path.join(home_path, ".wisminio")):
            key = line.split("=")[0].strip()
            value = line.split("=")[1].strip()
            # print(key,value)
            if key == "endpoint_url":
                minio_paras["endpoint_url"] = value
            elif key == "access_key":
                minio_paras["access_key"] = value
            elif key == "secret_key":
                minio_paras["secret_key"] = value
    return minio_paras


minio_paras = minio_cfg()

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": minio_paras["endpoint_url"]},
    key=minio_paras["access_key"],
    secret=minio_paras["secret_key"],
)


ro = {
    "client_kwargs": {"endpoint_url": minio_paras["endpoint_url"]},
    "key": minio_paras["access_key"],
    "secret": minio_paras["secret_key"],
}
