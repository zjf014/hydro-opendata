"""
Author: Wenyu Ouyang
Date: 2023-10-14 11:51:22
LastEditTime: 2023-10-14 18:12:09
LastEditors: Wenyu Ouyang
Description: Reading streamflow/water level data from hydrostation data source
FilePath: \hydro_opendata\hydro_opendata\downloader\hydrostation.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import numpy as np
import pandas as pd
import os
import warnings
from hydro_opendata.downloader import GRDC_DAILY_DATA_DIR
from hydro_opendata.downloader.downloader import (
    download_ftp_file,
    unzip_file,
    wget_download,
)


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


def download_grdc_month_data(id, save_dir):
    """
    Interface with the Global Runoff Data Centre database of Monthly Time Series.

    For daily data, you have to manually apply using your email in the following link:
    https://portal.grdc.bafg.de/applications/public.html?publicuser=PublicUser#dataDownload/Stations

    Parameters
    -----------
    id (str)
        Identifier for a station. It's called "grdc no" in the catalogue.
                it will be transformed to int type.

    Returns
    -------
    dict
        A dictionary containing tables file path.
    """

    # catalogue_grdc to get the catalogue.
    catalogue = catalogue_grdc(save_dir)

    # Filter the catalogue based on the provided id
    catalogue_filtered = catalogue[catalogue["grdc_no"] == int(id)]

    # Retrieve the WMO region from the filtered catalogue
    wmo_region = catalogue_filtered["wmo_reg"].iloc[0]

    # Load the grdcLTMMD.csv file
    this_dir = os.path.dirname(os.path.abspath(__file__))
    grdc_ltmmd_file = os.path.join(this_dir, "grdcLTMMD.csv")
    grdc_ltmmd = pd.read_csv(grdc_ltmmd_file)
    # Retrieve ftp server location based on the WMO region
    zip_file_url = grdc_ltmmd.loc[
        grdc_ltmmd["WMO_Region"] == wmo_region, "Archive"
    ].values[0]

    # Download the ZIP file from FTP server
    file_path = download_ftp_file(zip_file_url, save_dir)

    data_dir = unzip_file(file_path)

    # Table names
    tablenames = ["LTVD", "LTVM", "PVD", "PVM", "YVD", "YVM"]

    # Assuming 'id' is already defined
    fname = [f"{id}_Q_{table}.csv" for table in tablenames]

    # Initialise empty dictionary of tables
    tables = {}

    # Populate tables
    for j, fpath in enumerate(fname):
        full_path = os.path.join(data_dir, fpath)
        if os.path.exists(full_path):
            tables[tablenames[j]] = full_path
        else:
            print(f"{tablenames[j]} data are not available at this station")
            tables[tablenames[j]] = None
    return tables


def download_grdc_daily_data(station_id):
    file_path = os.path.join(GRDC_DAILY_DATA_DIR, f"{station_id}_Q_Day.Cmd.txt")
    if not os.path.exists(file_path):
        message = (
            "For GRDC stations, easily use table-view to see; "
            "Then, when you download the data, you can select all the station ids you need to download the data. "
            "Remember to choose the export format as 'GRDC Export Format (daily data only)'."
        )
        warnings.warn(message, UserWarning)
    return file_path
