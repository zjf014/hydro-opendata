"""
Author: Wenyu Ouyang
Date: 2023-10-13 20:50:01
LastEditTime: 2023-10-31 11:40:59
LastEditors: Wenyu Ouyang
Description: Test downloading function
FilePath: \hydro_opendata\tests\test_downloader.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import os
import pandas as pd

import hydrodataset as hds
from hydro_opendata.downloader.hydrostation import (
    catalogue_grdc,
    download_grdc_month_data,
    download_grdc_daily_data,
    download_nwis_daily_flow,
)


def test_catalogue_grdc():
    grdc_catalogue = catalogue_grdc(hds.CACHE_DIR)
    assert isinstance(grdc_catalogue, pd.DataFrame)
    assert grdc_catalogue.shape == (10707, 24)
    assert "grdc_no" in grdc_catalogue.columns
    assert "station" in grdc_catalogue.columns
    assert "lat" in grdc_catalogue.columns
    assert "long" in grdc_catalogue.columns
    assert "d_start" in grdc_catalogue.columns
    assert "d_end" in grdc_catalogue.columns
    assert "d_yrs" in grdc_catalogue.columns
    assert "m_start" in grdc_catalogue.columns
    assert "m_end" in grdc_catalogue.columns
    assert "m_yrs" in grdc_catalogue.columns
    assert "t_start" in grdc_catalogue.columns
    assert "t_end" in grdc_catalogue.columns
    assert "t_yrs" in grdc_catalogue.columns
    assert "area" in grdc_catalogue.columns
    assert "altitude" in grdc_catalogue.columns
    assert "r_volume_yr" in grdc_catalogue.columns
    assert "r_height_yr" in grdc_catalogue.columns
    assert "lta_discharge" in grdc_catalogue.columns
    assert "d_miss" in grdc_catalogue.columns
    assert "m_miss" in grdc_catalogue.columns
    assert "wmo_reg" in grdc_catalogue.columns
    assert "sub_reg" in grdc_catalogue.columns


def test_download_grdc_ts(id="2181200"):
    data = download_grdc_month_data(id, hds.CACHE_DIR)

    # Check that the returned data is a dictionary
    assert isinstance(data, dict)

    # Check that the dictionary contains tables with river discharge data
    assert all(isinstance(table, str) for table in data.values())


def test_download_grdc_daily_data(tmp_path):
    station_id = "12345"
    file_path = download_grdc_daily_data(tmp_path, station_id)
    assert os.path.exists(file_path) == False


def test_download_usgs_streamflow():
    camels = hds.Camels()
    sites_id = camels.read_object_ids().tolist()
    date_range = ("2020-10-01", "2021-10-01")
    gage_dict = camels.camels_sites
    save_dir = os.path.join("test_data", "camels_streamflow_2021")
    unit = "cfs"
    qobs = download_nwis_daily_flow(sites_id, date_range, gage_dict, save_dir, unit)
    print(qobs)
