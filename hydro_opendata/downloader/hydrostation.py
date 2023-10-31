"""
Author: Wenyu Ouyang
Date: 2023-10-14 11:51:22
LastEditTime: 2023-10-31 11:41:41
LastEditors: Wenyu Ouyang
Description: Reading streamflow/water level data from hydrostation data source
FilePath: \hydro_opendata\hydro_opendata\downloader\hydrostation.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
from datetime import datetime
import numpy as np
import pandas as pd
import os
import warnings
import pytz
import requests
from dataretrieval import nwis
from pygeohydro import NWIS
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


def download_grdc_daily_data(station_id, data_dir):
    file_path = os.path.join(data_dir, f"{station_id}_Q_Day.Cmd.txt")
    if not os.path.exists(file_path):
        message = (
            "For GRDC stations, easily use table-view to see; "
            "Then, when you download the data, you can select all the station ids you need to download the data. "
            "Remember to choose the export format as 'GRDC Export Format (daily data only)'."
        )
        warnings.warn(message, UserWarning)
    return file_path


def download_nwis(
    gage_id_file="path_to_gauge_information.txt",
    start_date="2016-01-01",
    end_date="2023-06-30",
    param_code="00065",
):
    # TODO: merge this func with get_nwis_stream_data
    with open(gage_id_file, "r", encoding="utf-8") as file:
        content = file.readlines()

    gage_ids = [line.split("\t")[1].strip() for line in content[1:] if line.strip()]
    # sites = gage_ids

    for gage_id in gage_ids:
        # get instantaneous values (iv)
        df, md = nwis.get_iv(
            sites=gage_id, start=start_date, end=end_date, parameterCd=param_code
        )

        df.to_csv(
            f"{str(gage_id)}.txt",
            sep=" ",
            header=False,
            na_rep="",
            float_format="%6.2f",
        )

        # 打开原文件，并读取所有内容
        with open(f"{str(gage_id)}.txt", "r", encoding="utf-8") as file:
            content = file.read()

        # 使用replace方法去除所有的双引号
        content_no_quotes = content.replace('"', "")

        # 写入新的文件
        with open(f"{str(gage_id)}.txt", "w", encoding="utf-8") as file:
            file.write(content_no_quotes)


def get_nwis_stream_data(
    save_dir=".",
    sites=None,
    start=None,
    end=None,
    multi_index=True,
    ssl_check=True,
    param_code="All",
    standardlize=True,
):
    """
    用来获取usgs相关流域的数据

    Args:
        sites:站点id
        start:起始时间
        end:结束时间
        multi_index:多索引
        ssl_check:检查ssl
        parameterCd:用于获取部分数据，如果不指定这个参数，则获取全部数据
            00060代表流量
            00065代表水位
            出现其余数据类型，请参考USGS官网或nwis github
        standardlize:用于规范化存储txt

    Return:
        (type:DataFrame)
        datetime:时间
        site_no:站点ID
        00060:同上
        00065：同上
        00060_cd：测量情况，A代表实测，A,e代表经过处理，M代表无数据
        00065_cd: 同上
    """

    df, md = nwis.get_iv(sites=sites, start=start, end=end, parameterCd=param_code)
    df.to_csv(
        save_dir + sites + ".txt", sep=" ", header=True, na_rep="", float_format="%6.2f"
    )

    if standardlize == True:
        # 打开原文件，并读取所有内容
        with open(save_dir + sites + ".txt", "r", encoding="utf-8") as file:
            content = file.read()

        # 使用replace方法去除所有的双引号
        content_no_quotes = content.replace('"', "")

        # 写入新的文件
        with open(save_dir + sites + ".txt", "w", encoding="utf-8") as file:
            file.write(content_no_quotes)
    return [df, md]


# 等项目忙完再补充，这个是获得所有数据的
def get_nwis_full_data(
    save_dir="",
    sites=None,
    start=None,
    end=None,
    multi_index=True,
    wide_format=True,
    datetime_index=True,
    state=None,
    service="iv",
    ssl_check=True,
    **kwargs,
):
    """
    完整的获取各种nwis数据的方法

    参考：https://github.com/DOI-USGS/dataretrieval-python/blob/master/dataretrieval/nwis.py#L1289

    Args:
        save_dir = '':保存目录
        sites:站点的ID，例如:'01013500'
        start:开始时间，如start='2016-01-01'
        end:结束时间，如end='2016-01-02'
        multi_index:多标签,If False, a dataframe with a single-level index (datetime) is returned
        wide_format:If True, return data in wide format with multiple samples per row and one row per time
        service:索取数据类型
            - 'iv' : instantaneous data，即时数据，15分钟尺度
            - 'dv' : daily mean data，日尺度
            - 'qwdata' : discrete samples
            - 'site' : 站点描述
            - 'measurements' : 流量测量值
            - 'peaks': 流量峰值
            - 'gwlevels': 地表水等级
            - 'pmcodes': 获取参数代码
            - 'water_use': 使用水数据
            - 'ratings': get rating table
            - 'stat': 获取数据，A代表实测，A.e代表经过处理，M代表无
        ssl_check:检查ssl
        **kwargs:可调用的其余参数
    """
    pass


def download_nwis_daily_flow(
    usgs_site_ids: list,
    date_tuple: tuple,
    gage_dict: dict,
    save_dir: str,
    unit: str = "cfs",
) -> pd.DataFrame:
    """
    Download USGS flow data by HyRivers' pygeohydro tool.
    The tool's tutorial: https://github.com/cheginit/HyRiver-examples/blob/main/notebooks/nwis.ipynb

    Parameters
    ----------
    usgs_site_ids
        ids of USGS sites
    date_tuple
        start and end date
    gage_dict
        a dict containing gage's ids and the correspond HUC02 ids
    save_dir
        where we save streamflow data in files like CAMELS
    unit
        unit of streamflow, cms or cfs

    Returns
    -------
    pd.DataFrame
        streamflow data -- index is date; column is gage id
    """

    nwis = NWIS()
    qobs = nwis.get_streamflow(usgs_site_ids, date_tuple, mmd=False)
    # the unit of qobs is cms, but unit in CAMELS and GAGES-II is cfs, so here we transform it
    # use round(2) because in both CAMELS and GAGES-II, data with cfs only have two float digits
    if unit == "cfs":
        qobs = (qobs * 35.314666212661).round(2)
    dates = qobs.index
    camels_format_index = ["GAGE_ID", "Year", "Mnth", "Day", f"streamflow({unit})"]
    year_month_day = pd.DataFrame(
        [[dt.year, dt.month, dt.day] for dt in dates], columns=camels_format_index[1:4]
    )
    if "STAID" in gage_dict:
        gage_id_key = "STAID"
    elif "gauge_id" in gage_dict:
        gage_id_key = "gauge_id"
    elif "gage_id" in gage_dict:
        gage_id_key = "gage_id"
    else:
        raise NotImplementedError("No such gage id name")
    if "HUC02" in gage_dict:
        huc02_key = "HUC02"
    elif "huc_02" in gage_dict:
        huc02_key = "huc_02"
    else:
        raise NotImplementedError("No such huc02 id")
    read_sites = [col[5:] for col in qobs.columns.values]
    for site_id in usgs_site_ids:
        if site_id not in read_sites:
            df_flow = pd.DataFrame(
                np.full(qobs.shape[0], np.nan), columns=camels_format_index[4:5]
            )
        else:
            qobs_i = qobs["USGS-" + site_id]
            df_flow = pd.DataFrame(qobs_i.values, columns=camels_format_index[4:5])
        df_id = pd.DataFrame(
            np.full(qobs.shape[0], site_id), columns=camels_format_index[:1]
        )
        new_data_df = pd.concat([df_id, year_month_day, df_flow], axis=1)
        # output the result
        i_basin = gage_dict[gage_id_key].values.tolist().index(site_id)
        huc_id = gage_dict[huc02_key][i_basin]
        output_huc_dir = os.path.join(save_dir, huc_id)
        if not os.path.isdir(output_huc_dir):
            os.makedirs(output_huc_dir)
        output_file = os.path.join(output_huc_dir, site_id + "_streamflow_qc.txt")
        if os.path.isfile(output_file):
            os.remove(output_file)
        new_data_df.to_csv(
            output_file, header=True, index=False, sep=",", float_format="%.2f"
        )
    return qobs


def _usgsflow_df_label(usgs_text: str) -> str:
    usgs_text = usgs_text.replace(",", "")
    if usgs_text == "Discharge":
        return "cfs"
    elif usgs_text == "Gage":
        return "height"
    else:
        return usgs_text


def _process_usgs_response_text(file_name: str):
    extractive_params = {}
    with open(file_name, "r") as f:
        lines = f.readlines()
        i = 0
        params = False
        while "#" in lines[i]:
            # TODO figure out getting height and discharge code efficently
            the_split_line = lines[i].split()[1:]
            if params:
                print(the_split_line)
                if len(the_split_line) < 2:
                    params = False
                else:
                    extractive_params[
                        the_split_line[0] + "_" + the_split_line[1]
                    ] = _usgsflow_df_label(the_split_line[2])
            if len(the_split_line) > 2 and the_split_line[0] == "TS":
                params = True
            i += 1
        with open(file_name.split(".")[0] + "data.tsv", "w") as t:
            t.write("".join(lines[i:]))
        return file_name.split(".")[0] + "data.tsv", extractive_params


def _create_csv_for_usgsflow(file_path: str, params_names: dict, site_number: str):
    """
    Function that creates the final version of the CSV files

    Parameters
    ----------
    file_path : str
        [description]
    params_names : dict
        [description]
    site_number : str
        [description]
    """
    df = pd.read_csv(file_path, sep="\t")
    for key, value in params_names.items():
        df[value] = df[key]
    df.to_csv(f"{site_number}_flow_data.csv")


def download_usgs_data(
    start_date: datetime, end_date: datetime, site_number: str
) -> pd.DataFrame:
    """This method could also be used to download usgs streamflow data"""
    # TODO: to be merged with get_nwis_stream_data; may be useful when handling with hourly data
    base_url = (
        "https://nwis.waterdata.usgs.gov/usa/nwis/uv/?cb_00060=on&cb_00065&format=rdb&"
    )
    full_url = (
        base_url
        + "site_no="
        + site_number
        + "&period=&begin_date="
        + start_date.strftime("%Y-%m-%d")
        + "&end_date="
        + end_date.strftime("%Y-%m-%d")
    )
    print("Getting request from USGS")
    print(full_url)
    r = requests.get(full_url)
    with open(f"{site_number}.txt", "w") as f:
        f.write(r.text)
    print("Request finished")
    response_data = _process_usgs_response_text(f"{site_number}.txt")
    _create_csv_for_usgsflow(response_data[0], response_data[1], site_number)
    return pd.read_csv(f"{site_number}_flow_data.csv")


def _get_usgsflow_timezone_map():
    return {
        "EST": "America/New_York",
        "EDT": "America/New_York",
        "CST": "America/Chicago",
        "CDT": "America/Chicago",
        "MDT": "America/Denver",
        "MST": "America/Denver",
        "PST": "America/Los_Angeles",
        "PDT": "America/Los_Angeles",
    }


def process_intermediate_csv(df: pd.DataFrame):
    # Remove garbage first row
    # TODO: check if more rows are garbage
    df = df.iloc[1:]
    time_zone = df["tz_cd"].iloc[0]
    time_zone = _get_usgsflow_timezone_map()[time_zone]
    old_timezone = pytz.timezone(time_zone)
    new_timezone = pytz.timezone("UTC")
    # This assumes timezones are consistent throughout the USGS stream (this should be true)
    df["datetime"] = df["datetime"].map(
        lambda x: old_timezone.localize(
            datetime.strptime(x, "%Y-%m-%d %H:%M")
        ).astimezone(new_timezone)
    )
    df["cfs"] = pd.to_numeric(df["cfs"], errors="coerce")
    max_flow = df["cfs"].max()
    min_flow = df["cfs"].min()
    count_nan = len(df["cfs"]) - df["cfs"].count()
    print(f"there are {count_nan} nan values")
    return df[df.datetime.dt.minute == 0], max_flow, min_flow
