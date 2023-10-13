"""
Author: Jianfeng Zhu
Date: 2023-10-06 20:50:41
LastEditTime: 2023-10-10 10:20:30
LastEditors: Wenyu Ouyang
Description: Top-level package for hydro_opendata
FilePath: \hydro_opendata\hydro_opendata\__init__.py
Copyright (c) 2023-2024 Jianfeng Zhu. All rights reserved.
"""

import pathlib
import os

from .common import minio_paras


__author__ = """Jianfeng Zhu"""
__email__ = "zjf014@gmail.com"
__version__ = "0.0.1"

home_path = str(pathlib.Path.home())

if not os.path.exists(os.path.join(home_path, ".wisminio")):
    access_key = input("minio_access_key:")
    secret_key = input("minio_secret_key:")

    with open(os.path.join(home_path, ".wisminio"), "w") as f:
        f.write("endpoint_url = " + minio_paras["endpoint_url"])
        f.write("\naccess_key = " + access_key)
        f.write("\nsecret_key = " + secret_key)
        f.write("\nbucket_name = " + minio_paras["bucket_name"])
