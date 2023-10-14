"""
Author: Wenyu Ouyang
Date: 2023-10-13 20:37:36
LastEditTime: 2023-10-14 21:22:52
LastEditors: Wenyu Ouyang
Description: Test reading GRDC data
FilePath: \hydro_opendata\tests\test_hydrostation_reading.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import os
import xarray as xr
from downloader import GRDC_DAILY_DATA_DIR

from s3api.grdc import read_grdc_daily_data, dailygrdc2netcdf


def test_read_grdc():
    """Test reading GRDC data"""
    df, metadata = read_grdc_daily_data(
        station_id="2181200",
        start_time="1980-01-01T00:00Z",
        end_time="2001-01-01T00:00Z",
    )
    assert df.shape == (7305, 1)


def test_dailygrdc2netcdf():
    # Define start and end dates
    start_date = "1980-01-01"
    end_date = "2022-12-31"

    # Create a temporary directory to store the NetCDF file
    # Call the dailygrdc2netcdf function
    dailygrdc2netcdf(start_date, end_date)

    # Load the NetCDF file into an xarray Dataset
    ds = xr.open_dataset(os.path.join(GRDC_DAILY_DATA_DIR, "grdc_daily_data.nc"))
    # Check that the dimensions and variables are correct
    assert set(ds.variables) == {"time", "station", "streamflow"}
