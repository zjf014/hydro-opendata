"""
Author: Jianfeng Zhu
Date: 2023-10-06 20:50:41
LastEditTime: 2023-10-14 11:50:48
LastEditors: Wenyu Ouyang
Description: 该模块用于下载，通常是url链接或执行命令：`download_from_url` - 通过url链接下载文件；`download_by_stream` - 通过stream方式下载url链接；`download_singletasking` - 单函数线程下载文件，显示进度条
FilePath: /hydro_opendata/hydro_opendata/downloader/downloader.py
Copyright (c) 2023-2024 Jianfeng Zhu. All rights reserved.
"""
import requests
from tqdm import tqdm
from zipfile import ZipFile

import wget

from ftplib import FTP
import os


def download_ftp_file(ftp_url, path=None, retries=3):
    # Parse the FTP URL
    url_parts = ftp_url.replace("ftp://", "").split("/")
    host = url_parts[0]
    file_path = "/".join(url_parts[1:])
    remote_filename = url_parts[-1]

    # If path is a directory, specify the filename to save the downloaded file
    if path and os.path.isdir(path):
        path = os.path.join(path, remote_filename)

    # Connect to the FTP server
    ftp = FTP(host)
    ftp.login()

    # Get remote file size
    remote_file_size = ftp.size(file_path)

    # Check if the file already exists at the specified path and has the same size as the remote file
    if path and os.path.exists(path) and os.path.getsize(path) == remote_file_size:
        return path

    # Download the file
    for _ in range(retries):
        with open(path, "wb") as local_file:
            ftp.retrbinary(f"RETR {file_path}", local_file.write)

        # Verify the downloaded file size
        if os.path.getsize(path) == remote_file_size:
            return path

    raise ValueError(
        "Failed to download the file after multiple attempts. The file might be corrupted or incomplete."
    )


def wget_download(url, save_path=None):
    """
    Downloads a file using wget.

    Parameters:
    - url (str): The URL of the file to be downloaded.
    - save_path (str, optional): dir or file to save (default: current working directory).

    Returns:
    - str: The path to the downloaded file.
    """
    if save_path and os.path.isdir(save_path):
        output_filename = os.path.join(save_path, os.path.basename(url))
    elif save_path:
        output_filename = save_path
    else:
        output_filename = os.path.basename(url)

    # Check if the file already exists
    if os.path.exists(output_filename):
        print(f"File {output_filename} already exists. Skipping download.")
        return output_filename

    return wget.download(url, out=output_filename)


def unzip_file(zip_path, output_folder=None):
    """
    Unzips a ZIP file.

    Parameters:
    - zip_path (str): The path to the ZIP file.
    - output_folder (str, optional): The folder where the ZIP file should be extracted to.
                                     Defaults to a folder named after the ZIP file in the ZIP file's directory.

    Returns:
    - str: The path to the extracted folder.
    """
    if not output_folder:
        output_folder = os.path.join(
            os.path.dirname(zip_path), os.path.splitext(os.path.basename(zip_path))[0]
        )

    # Check if the output folder already exists
    if not os.path.exists(output_folder):
        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_folder)
    else:
        print(f"Files already extracted to {output_folder}. Skipping extraction.")

    return output_folder


def download_from_url(url, file_name):
    """
    通过url链接下载文件的一般方法

    Args:
        url (str): url链接
        file_name (str): 文件的本地存储地址

    """

    f = requests.get(url)
    with open(file_name, "wb") as tiff:
        tiff.write(f.content)
    f.close()


def download_by_stream(url, file_name):
    """
    通过stream下载url链接文件

    Args:
        url (str): url链接
        file_name (str): 文件的本地存储地址

    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE"
    }

    # 发起 head 请求，即只会获取响应头部信息
    head = requests.head(url, headers=headers)

    if head.status_code == 404:
        print("404 not found.")
    elif head.status_code == 200:
        response = requests.get(url, headers=headers, stream=True)
        with open(file_name, mode="wb") as f:
            # 写入分块文件
            for i, chunk in enumerate(response.iter_content(chunk_size=1024), start=1):
                f.write(chunk)
                print("\r", "已下载：%.2f MB" % (i / 1024), end="", flush=True)

            print(f"{file_name}下载完成。")


def download_sigletasking(url: str, file_name: str):
    """
    单函数线程下载文件，显示进度条

    Args:
        url (str): 文件链接
        file_name (str): 文件名或文件路径

    """
    # 文件下载直链
    # 请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE"
    }

    # 发起 head 请求，即只会获取响应头部信息
    head = requests.head(url, headers=headers)

    if head.status_code == 404:
        print("404 not found.")
    elif head.status_code == 200:
        # 文件大小，以 B 为单位
        file_size = head.headers.get("Content-Length")
        if file_size is not None:
            download_single_task_with_chunks(file_size, url, headers, file_name)
        else:
            # 未获取到文件大小，采用stream方法下载
            # download_from_url(url,file_name)
            download_by_stream(url, file_name)


def download_single_task_with_chunks(file_size, url, headers, file_name):
    file_size = int(file_size)
    response = requests.get(url, headers=headers, stream=True)
    # 一块文件的大小
    # chunk_size = 1024
    chunk_size = file_size // 100 + 1
    bar = tqdm(total=file_size, desc=f"下载文件 {file_name}")
    with open(file_name, mode="wb") as f:
        # 写入分块文件
        for chunk in response.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            bar.update(chunk_size)
    # 关闭进度条
    bar.close()
