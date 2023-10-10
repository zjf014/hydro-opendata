"""
Author: Jianfeng Zhu
Date: 2023-10-06 20:50:41
LastEditTime: 2023-10-10 10:19:23
LastEditors: Wenyu Ouyang
Description: 该模块用于下载，通常是url链接或执行命令：`download_from_url` - 通过url链接下载文件；`download_by_stream` - 通过stream方式下载url链接；`download_singletasking` - 单函数线程下载文件，显示进度条
FilePath: \hydro_opendata\hydro_opendata\downloader\downloader.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""

import requests
from tqdm import tqdm


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
            file_size = int(file_size)
            response = requests.get(url, headers=headers, stream=True)
            # 一块文件的大小
            # chunk_size = 1024
            chunk_size = int(file_size / 100) + 1
            bar = tqdm(total=file_size, desc=f"下载文件 {file_name}")
            with open(file_name, mode="wb") as f:
                # 写入分块文件
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
                    bar.update(chunk_size)
            # 关闭进度条
            bar.close()
        else:
            # 未获取到文件大小，采用stream方法下载
            # download_from_url(url,file_name)
            download_by_stream(url, file_name)
