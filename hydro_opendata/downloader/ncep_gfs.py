"""
该模块用于global forecast system数据下载

如非特指，默认下载精度为0.25度。

数据说明: [https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast](https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast)

- `search` - 返回符合搜索条件的dem数据下载地址列表
- `download` - 将列表中的数据下载到本地
"""


from .downloader import download_sigletasking
import os
import subprocess


def get_gfs_from_ncep(
    date: str,
    creation_time: str,
    forecast_time: int,
    bbox=[115, 38, 136, 54],
    save_dir=".",
    cover=False,
):
    """
    从ncep官网下载近期gfs数据

    官方链接: [https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl](https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl)

    下载url示例: [https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=gfs.t00z.pgrb2.0p25.f001&all_lev=on&all_var=on&subregion=&leftlon=115&rightlon=136&toplat=54&bottomlat=38&dir=%2Fgfs.20220815%2F00%2Fatmos](https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=gfs.t00z.pgrb2.0p25.f001&all_lev=on&all_var=on&subregion=&leftlon=115&rightlon=136&toplat=54&bottomlat=38&dir=%2Fgfs.20220815%2F00%2Fatmos)

    Args:
        date (str): 下载日期，格式为：YYYYMMDD
        creation_time (str): 数据创建时间，可以是00、06、12、18中的一个
        forecast_time (int): 预测序列，范围1-384
        bbox (list): 下载数据的矩形范围
        save_dir (str): 存储文件夹
        cover (bool): 若文件已存在，是否覆盖

    """

    file_name_ = "gfsYYYYMMDD.tCCz.pgrb2.0p25.fFFF"

    url_ = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.tCCz.pgrb2.0p25.fFFF&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_entire_atmosphere=on&lev_entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29=on&lev_surface=on&var_APCP=on&var_DSWRF=on&var_PWAT=on&var_RH=on&var_SPFH=on&var_TCDC=on&var_TMP=on&var_UGRD=on&var_VGRD=on&subregion=&leftlon={str(bbox[0])}&rightlon={str(bbox[2])}&toplat={str(bbox[3])}&bottomlat={str(bbox[1])}&dir=%2Fgfs.YYYYMMDD%2FCC%2Fatmos"
    url = (
        url_.replace("YYYYMMDD", date)
        .replace("CC", creation_time.zfill(2))
        .replace("FFF", str(forecast_time).zfill(3))
    )
    file_name = (
        file_name_.replace("YYYYMMDD", date)
        .replace("CC", creation_time.zfill(2))
        .replace("FFF", str(forecast_time).zfill(3))
    )

    file_path = os.path.join(save_dir, file_name)
    if os.path.exists(file_path) and not cover:
        print(f"{file_name}已存在.")
        return

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    download_sigletasking(url, os.path.join(save_dir, file_name))


def get_gfs_from_aws(
    date: str, creation_time: str, forecast_time: int, save_dir=".", cover=False
):
    """
    从aws下载gfs数据，数据时间范围从2021年2月26日开始。

    需要安装[aws cli](https://docs.aws.amazon.com/zh_cn/cli/latest/userguide/getting-started-install.html)

    建议使用aws cli命令行下载

    aws链接: [https://registry.opendata.aws/noaa-gfs-bdp-pds/](https://registry.opendata.aws/noaa-gfs-bdp-pds/)

    下载url示例: [https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=gfs.t00z.pgrb2.0p25.f001&all_lev=on&all_var=on&subregion=&leftlon=115&rightlon=136&toplat=54&bottomlat=38&dir=%2Fgfs.20220815%2F00%2Fatmos](https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=gfs.t00z.pgrb2.0p25.f001&all_lev=on&all_var=on&subregion=&leftlon=115&rightlon=136&toplat=54&bottomlat=38&dir=%2Fgfs.20220815%2F00%2Fatmos)

    Args:
        date (str): 下载日期，格式为：YYYYMMDD
        creation_time (str): 数据创建时间，可以是00、06、12、18中的一个
        forecast_time (int): 预测序列，范围1-384
        save_dir (str): 存储文件夹
        cover (bool): 若文件已存在，是否覆盖

    """

    url_ = "s3://noaa-gfs-bdp-pds/gfs.YYYYMMDD/CC/atmos/gfs.tCCz.pgrb2.0p25.fFFF"
    if (date + creation_time.zfill(2)) < "2021032212":
        url_ = "s3://noaa-gfs-bdp-pds/gfs.YYYYMMDD/CC/gfs.tCCz.pgrb2.0p25.fFFF"
    file_name_ = "gfsYYYYMMDD.tCCz.pgrb2.0p25.fFFF"

    url = (
        url_.replace("YYYYMMDD", date)
        .replace("CC", creation_time.zfill(2))
        .replace("FFF", str(forecast_time).zfill(3))
    )
    file_name = (
        file_name_.replace("YYYYMMDD", date)
        .replace("CC", creation_time.zfill(2))
        .replace("FFF", str(forecast_time).zfill(3))
    )

    file_path = os.path.join(save_dir, file_name)
    if os.path.exists(file_path) and not cover:
        print(f"{file_name}已存在.")
        return

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    # download_sigletasking(url,os.path.join(save_dir,file_name)
    subprocess.call(["aws", "s3", "cp", url, file_path])


def get_gfs_from_gee(
    date: str,
    creation_time: str,
    forecast_time: int,
    bbox=None,
    save_dir=".",
    cover=False,
):
    """
    从google earth engine下载获取gfs数据，时间范围从2015年7月1日开始。

    需要科学上网！

    需要gee for python本地配置
    - pip install google-api-python-client
    - pip install pyCrytod
    - pip install earthengine-api
    - earthengine authenticate (需要gcloud)

    gfs from gee链接: [https://developers.google.cn/earth-engine/datasets/catalog/NOAA_GFS0P25](https://developers.google.cn/earth-engine/datasets/catalog/NOAA_GFS0P25)

    Args:
        date (str): 下载日期，格式为：YYYYMMDD
        creation_time (str): 数据创建时间，可以是00、06、12、18中的一个
        forecast_time (int): 预测序列，范围1-384
        bbox (list): 下载数据的矩形范围
        save_dir (str): 存储文件夹
        cover (bool): 若文件已存在，是否覆盖

    """
    if bbox is None:
        bbox = [115, 38, 136, 54]
    pass
