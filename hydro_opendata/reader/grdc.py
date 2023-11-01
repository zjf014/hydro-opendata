"""
Author: Wenyu Ouyang
Date: 2023-01-02 22:23:24
LastEditTime: 2023-10-31 11:27:38
LastEditors: Wenyu Ouyang
Description: read the Global Runoff Data Centre (GRDC) daily data
FilePath: \hydro_opendata\hydro_opendata\reader\grdc.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
# Global Runoff Data Centre module from ewatercycle: https://github.com/eWaterCycle/ewatercycle/blob/main/src/ewatercycle/observation/grdc.py
import datetime
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple, Union
from dateutil.parser import parse
import pandas as pd
import xarray as xr

from hydro_opendata.downloader.hydrostation import catalogue_grdc

logger = logging.getLogger(__name__)

MetaDataType = Dict[str, Union[str, int, float]]


def get_time(time_iso: str) -> datetime.datetime:
    """Return a datetime in UTC.
    Convert a date string in ISO format to a datetime
    and check if it is in UTC.
    """
    time = parse(time_iso)
    if time.tzname() != "UTC":
        raise ValueError(
            "The time is not in UTC. The ISO format for a UTC time "
            "is 'YYYY-MM-DDTHH:MM:SSZ'"
        )
    return time


def to_absolute_path(
    input_path: str,
    parent: Optional[Path] = None,
    must_exist: bool = False,
    must_be_in_parent=True,
) -> Path:
    """Parse input string as :py:class:`pathlib.Path` object.
    Args:
        input_path: Input string path that can be a relative or absolute path.
        parent: Optional parent path of the input path
        must_exist: Optional argument to check if the input path exists.
        must_be_in_parent: Optional argument to check if the input path is
            subpath of parent path
    Returns:
        The input path that is an absolute path and a :py:class:`pathlib.Path` object.
    """
    pathlike = Path(input_path)
    if parent:
        pathlike = parent.joinpath(pathlike)
        if must_be_in_parent:
            try:
                pathlike.relative_to(parent)
            except ValueError as e:
                raise ValueError(
                    f"Input path {input_path} is not a subpath of parent {parent}"
                ) from e

    return pathlike.expanduser().resolve(strict=must_exist)


def read_grdc_daily_data(
    station_id: str,
    start_time: str,
    end_time: str,
    data_home: Optional[str],
    parameter: str = "Q",
    column: str = "streamflow",
) -> Tuple[pd.core.frame.DataFrame, MetaDataType]:
    """read daily river discharge data from Global Runoff Data Centre (GRDC).

    Requires the GRDC daily data files in a local directory. The GRDC daily data
    files can be ordered at
    https://www.bafg.de/GRDC/EN/02_srvcs/21_tmsrs/riverdischarge_node.html

    Parameters
    ----------
        station_id: The station id to get. The station id can be found in the
            catalogues at
            https://www.bafg.de/GRDC/EN/02_srvcs/21_tmsrs/212_prjctlgs/project_catalogue_node.html
        start_time: Start time of model in UTC and ISO format string e.g.
            'YYYY-MM-DDTHH:MM:SSZ'.
        end_time: End time of model in  UTC and ISO format string e.g.
            'YYYY-MM-DDTHH:MM:SSZ'.
        parameter: optional. The parameter code to get, e.g. ('Q') discharge,
            cubic meters per second.
        data_home : optional. The directory where the daily grdc data is
            located. If left out will use the grdc_location in the eWaterCycle
            configuration file.
        column: optional. Name of column in dataframe. Default: "streamflow".

    Returns:
        grdc data in a dataframe and metadata.

    Examples:
        .. code-block:: python

            from ewatercycle.observation.grdc import get_grdc_data

            df, meta = get_grdc_data('6335020',
                                    '2000-01-01T00:00Z',
                                    '2001-01-01T00:00Z')
            df.describe()
                     streamflow
            count   4382.000000
            mean    2328.992469
            std	    1190.181058
            min	     881.000000
            25%	    1550.000000
            50%	    2000.000000
            75%	    2730.000000
            max	   11300.000000

            meta
            {'grdc_file_name': '/home/myusername/git/eWaterCycle/ewatercycle/6335020_Q_Day.Cmd.txt',
            'id_from_grdc': 6335020,
            'file_generation_date': '2019-03-27',
            'river_name': 'RHINE RIVER',
            'station_name': 'REES',
            'country_code': 'DE',
            'grdc_latitude_in_arc_degree': 51.756918,
            'grdc_longitude_in_arc_degree': 6.395395,
            'grdc_catchment_area_in_km2': 159300.0,
            'altitude_masl': 8.0,
            'dataSetContent': 'MEAN DAILY DISCHARGE (Q)',
            'units': 'm³/s',
            'time_series': '1814-11 - 2016-12',
            'no_of_years': 203,
            'last_update': '2018-05-24',
            'nrMeasurements': 'NA',
            'UserStartTime': '2000-01-01T00:00Z',
            'UserEndTime': '2001-01-01T00:00Z',
            'nrMissingData': 0}
    """  # noqa: E501
    if data_home:
        data_path = to_absolute_path(data_home)
    else:
        raise ValueError(
            "Provide the grdc path using `data_home` argument"
            "or using `grdc_location` in ewatercycle configuration file."
        )

    if not data_path.exists():
        raise ValueError(f"The grdc directory {data_path} does not exist!")

    # Read the raw data
    raw_file = data_path / f"{station_id}_{parameter}_Day.Cmd.txt"
    if not raw_file.exists():
        raise ValueError(f"The grdc file {raw_file} does not exist!")

    # Convert the raw data to an xarray
    metadata, df = _grdc_read(
        raw_file,
        start=get_time(start_time).date(),
        end=get_time(end_time).date(),
        column=column,
    )

    # Add start/end_time to metadata
    metadata["UserStartTime"] = start_time
    metadata["UserEndTime"] = end_time

    # Add number of missing data to metadata
    metadata["nrMissingData"] = _count_missing_data(df, column)

    # Show info about data
    _log_metadata(metadata)

    return df, metadata


def _grdc_read(grdc_station_path, start, end, column):
    with grdc_station_path.open("r", encoding="cp1252", errors="ignore") as file:
        data = file.read()

    metadata = _grdc_metadata_reader(grdc_station_path, data)

    all_lines = data.split("\n")
    header = next(
        (i + 1 for i, line in enumerate(all_lines) if line.startswith("# DATA")),
        0,
    )
    # Import GRDC data into dataframe and modify dataframe format
    grdc_data = pd.read_csv(
        grdc_station_path,
        encoding="cp1252",
        skiprows=header,
        delimiter=";",
        parse_dates=["YYYY-MM-DD"],
        na_values="-999",
    )
    grdc_station_df = pd.DataFrame(
        {column: grdc_data[" Value"].array},
        index=grdc_data["YYYY-MM-DD"].array,
    )
    grdc_station_df.index.rename("time", inplace=True)

    # Create a continuous date range based on the given start and end dates
    full_date_range = pd.date_range(start=start, end=end)
    full_df = pd.DataFrame(index=full_date_range)
    full_df.index.rename("time", inplace=True)

    # Merge the two dataframes, so the dates without data will have NaN values
    merged_df = full_df.merge(
        grdc_station_df, left_index=True, right_index=True, how="left"
    )

    return metadata, merged_df


def _grdc_metadata_reader(grdc_station_path, all_lines):
    """
    Initiating a dictionary that will contain all GRDC attributes.
    This function is based on earlier work by Rolf Hut.
    https://github.com/RolfHut/GRDC2NetCDF/blob/master/GRDC2NetCDF.py
    DOI: 10.5281/zenodo.19695
    that function was based on earlier work by Edwin Sutanudjaja from Utrecht University.
    https://github.com/edwinkost/discharge_analysis_IWMI
    Modified by Susan Branchett
    """

    # split the content of the file into several lines
    all_lines = all_lines.replace("\r", "")
    all_lines = all_lines.split("\n")

    # get grdc ids (from files) and check their consistency with their
    # file names
    id_from_file_name = int(
        os.path.basename(grdc_station_path).split(".")[0].split("_")[0]
    )
    id_from_grdc = None
    if id_from_file_name == int(all_lines[8].split(":")[1].strip()):
        id_from_grdc = int(all_lines[8].split(":")[1].strip())
    else:
        print(
            f"GRDC station {id_from_file_name} ({str(grdc_station_path)}) is NOT used."
        )

    attribute_grdc = {}
    if id_from_grdc is not None:
        attribute_grdc["grdc_file_name"] = str(grdc_station_path)
        attribute_grdc["id_from_grdc"] = id_from_grdc

        try:
            attribute_grdc["file_generation_date"] = str(
                all_lines[6].split(":")[1].strip()
            )
        except (IndexError, ValueError):
            attribute_grdc["file_generation_date"] = "NA"

        try:
            attribute_grdc["river_name"] = str(all_lines[9].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["river_name"] = "NA"

        try:
            attribute_grdc["station_name"] = str(all_lines[10].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["station_name"] = "NA"

        try:
            attribute_grdc["country_code"] = str(all_lines[11].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["country_code"] = "NA"

        try:
            attribute_grdc["grdc_latitude_in_arc_degree"] = float(
                all_lines[12].split(":")[1].strip()
            )
        except (IndexError, ValueError):
            attribute_grdc["grdc_latitude_in_arc_degree"] = "NA"

        try:
            attribute_grdc["grdc_longitude_in_arc_degree"] = float(
                all_lines[13].split(":")[1].strip()
            )
        except (IndexError, ValueError):
            attribute_grdc["grdc_longitude_in_arc_degree"] = "NA"

        try:
            attribute_grdc["grdc_catchment_area_in_km2"] = float(
                all_lines[14].split(":")[1].strip()
            )
            if attribute_grdc["grdc_catchment_area_in_km2"] <= 0.0:
                attribute_grdc["grdc_catchment_area_in_km2"] = "NA"
        except (IndexError, ValueError):
            attribute_grdc["grdc_catchment_area_in_km2"] = "NA"

        try:
            attribute_grdc["altitude_masl"] = float(all_lines[15].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["altitude_masl"] = "NA"

        try:
            attribute_grdc["dataSetContent"] = str(all_lines[21].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["dataSetContent"] = "NA"

        try:
            attribute_grdc["units"] = str(all_lines[23].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["units"] = "NA"

        try:
            attribute_grdc["time_series"] = str(all_lines[24].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["time_series"] = "NA"

        try:
            attribute_grdc["no_of_years"] = int(all_lines[25].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["no_of_years"] = "NA"

        try:
            attribute_grdc["last_update"] = str(all_lines[26].split(":")[1].strip())
        except (IndexError, ValueError):
            attribute_grdc["last_update"] = "NA"

        try:
            attribute_grdc["nrMeasurements"] = int(
                str(all_lines[34].split(":")[1].strip())
            )
        except (IndexError, ValueError):
            attribute_grdc["nrMeasurements"] = "NA"

    return attribute_grdc


def _count_missing_data(df, column):
    """Return number of missing data."""
    return df[column].isna().sum()


def _log_metadata(metadata):
    """Print some information about data."""
    coords = (
        metadata["grdc_latitude_in_arc_degree"],
        metadata["grdc_longitude_in_arc_degree"],
    )
    message = (
        f"GRDC station {metadata['id_from_grdc']} is selected. "
        f"The river name is: {metadata['river_name']}."
        f"The coordinates are: {coords}."
        f"The catchment area in km2 is: {metadata['grdc_catchment_area_in_km2']}. "
        f"There are {metadata['nrMissingData']} missing values during "
        f"{metadata['UserStartTime']}_{metadata['UserEndTime']} at this station. "
        f"See the metadata for more information."
    )
    logger.info("%s", message)


def dailygrdc2netcdf(start_date, end_date, data_dir=None, station_ids=None):
    """
    Parameters
    ----------
    start_date : _type_
        a startDate provided in YYYY-MM-DD
    end_date : _type_
        a endDate provided in YYYY-MM-DD
    """
    nc_file = os.path.join(data_dir, "grdc_daily_data.nc")
    if os.path.exists(nc_file):
        return

    catalogue = catalogue_grdc(data_dir)
    # Create empty lists to store data and metadata
    data_list = []
    meta_list = []

    if station_ids is None:
        # Filter the catalogue based on the provided station IDs
        filenames = os.listdir(data_dir)
        # Extract station IDs from filenames that match the pattern
        station_ids = [
            int(fname.split("_")[0])
            for fname in filenames
            if fname.endswith("_Q_Day.Cmd.txt")
        ]
    catalogue = catalogue[catalogue["grdc_no"].isin(station_ids)]

    # Loop over each station in the catalogue
    for station_id in catalogue["grdc_no"]:
        try:
            st = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            et = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            df, meta = read_grdc_daily_data(str(station_id), st, et, data_dir)
        except Exception as e:
            print(f"Error reading data for station {station_id}: {e}")
            # Create an empty DataFrame with the same structure
            df = pd.DataFrame(
                columns=["streamflow"],
                index=pd.date_range(start=start_date, end=end_date),
            )
            df["streamflow"] = float("nan")
            meta = {
                "grdc_file_name": "",
                "id_from_grdc": station_id,
                "river_name": "",
                "station_name": "",
                "country_code": "",
                "grdc_latitude_in_arc_degree": float("nan"),
                "grdc_longitude_in_arc_degree": float("nan"),
                "grdc_catchment_area_in_km2": float("nan"),
                "altitude_masl": float("nan"),
                "dataSetContent": "",
                "units": "m³/s",
                "time_series": "",
                "no_of_years": 0,
                "last_update": "",
                "nrMeasurements": "NA",
                "UserStartTime": start_date,
                "UserEndTime": end_date,
                "nrMissingData": 0,
            }

        # Convert the DataFrame to an xarray DataArray and append to the list
        # da = xr.DataArray(
        #     df["streamflow"].values,
        #     coords=[df.index, [station_id]],
        #     dims=["time", "station"],
        # )
        coords_dict = {"time": df.index, "station": [station_id]}
        da = xr.DataArray(
            data=df["streamflow"].values.reshape(-1, 1),
            coords=coords_dict,
            dims=["time", "station"],
            name="streamflow",
            attrs={"units": meta.get("units", "unknown")},
        )
        data_list.append(da)

        # Append metadata
        meta_list.append(meta)

    # Concatenate all DataArrays along the 'station' dimension
    ds = xr.concat(data_list, dim="station")

    # Assign attributes
    ds.attrs["description"] = "Daily river discharge"
    ds.station.attrs["description"] = "GRDC station number"

    # Write the xarray Dataset to a NetCDF file
    ds.to_netcdf(nc_file)

    print("NetCDF file created successfully!")


class GRDCDataHandler:
    def handle(self, configuration):
        aoi_param = configuration["aoi"].aoi_param
        start_time = aoi_param["start_time"]
        end_time = aoi_param["end_time"]
        station_id = aoi_param["station_id"]
        # Based on configuration, read and handle GRDC data specifically
        nc_file = configuration["path"]
        if not os.path.isfile(nc_file):
            dailygrdc2netcdf(start_time, end_time, data_dir=os.path.dirname(nc_file))
        ds = xr.open_dataset(nc_file)
        # choose data for given basin
        return ds.sel(station=int(station_id)).sel(time=slice(start_time, end_time))
