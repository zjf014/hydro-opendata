"""
Author: Wenyu Ouyang
Date: 2023-10-14 11:51:22
LastEditTime: 2023-10-14 11:51:47
LastEditors: Wenyu Ouyang
Description: Reading streamflow/water level data from hydrostation data source
FilePath: /hydro_opendata/hydro_opendata/downloader/hydrostation.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
from downloader.downloader import unzip_file, wget_download


import numpy as np
import pandas as pd


import os


def catalogue_grdc(save_dir):
    # URL for the GRDC catalogue
    file_url = "ftp://ftp.bafg.de/pub/REFERATE/GRDC/catalogue/grdc_stations.zip"

    # Download the zip file using FTP
    save_path = wget_download(file_url, save_dir)
    # Extract the Excel file from the zip
    unzip_folder = unzip_file(save_path)
    # Read the Excel file
    station_file = os.path.join(unzip_folder, "GRDC_Stations.xlsx")
    grdc_catalogue = pd.read_excel(station_file, sheet_name="station_catalogue")

    # Cleanup the data
    grdc_catalogue.replace(["n.a.", "-999.0"], np.nan, inplace=True)

    # Convert certain columns to integer or numeric data types
    cols_to_int = [
        "wmo_reg",
        "sub_reg",
        "d_start",
        "d_end",
        "d_yrs",
        "m_start",
        "m_end",
        "m_yrs",
        "t_start",
        "t_end",
        "t_yrs",
    ]
    grdc_catalogue[cols_to_int] = grdc_catalogue[cols_to_int].astype(pd.Int64Dtype)

    cols_to_numeric = [
        "lat",
        "long",
        "area",
        "altitude",
        "d_miss",
        "m_miss",
        "lta_discharge",
        "r_volume_yr",
        "r_height_yr",
    ]
    grdc_catalogue[cols_to_numeric] = grdc_catalogue[cols_to_numeric].astype(float)

    return grdc_catalogue