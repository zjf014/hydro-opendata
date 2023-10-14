"""
Author: Wenyu Ouyang
Date: 2023-10-13 20:37:36
LastEditTime: 2023-10-14 11:57:40
LastEditors: Wenyu Ouyang
Description: Test reading GRDC data
FilePath: /hydro_opendata/tests/test_grdc_reading.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
from pathlib import Path
import hydrodataset as hds

from s3api.grdc import get_grdc_data


def test_read_grdc():
    """Test reading GRDC data"""
    df, metadata = get_grdc_data(
        station_id="2181200",
        start_time="1980-01-01T00:00Z",
        end_time="2001-01-01T00:00Z",
        data_home=Path(hds.CACHE_DIR, "grdc"),
    )
    df.to_csv(Path(hds.CACHE_DIR, "grdc", "2181200.csv"))
