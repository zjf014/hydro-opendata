"""
Author: Jeff Zhu
Date: 2023-10-06 20:50:41
LastEditTime: 2023-10-14 19:30:14
LastEditors: Wenyu Ouyang
Description: Top-level package for wis-downloader
FilePath: \hydro_opendata\hydro_opendata\downloader\__init__.py
Copyright (c) 2023-2024 Jeff Zhu. All rights reserved.
"""

__author__ = """Jeff Zhu"""
__email__ = "zjf014@gmail.com"
__version__ = "0.0.1"

import hydrodataset as hds

GRDC_DAILY_DATA_DIR = hds.CACHE_DIR.joinpath("grdc_daily_data")
