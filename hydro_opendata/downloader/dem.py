"""
该模块用于alos-dem数据下载

数据源: [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/dataset/alos-dem)

数据目录: [https://planetarycomputer.microsoft.com/api/stac/v1/](https://planetarycomputer.microsoft.com/api/stac/v1/)

数据说明: [https://www.eorc.jaxa.jp/ALOS/en/aw3d30/aw3d30v3.2_product_e_e1.2.pdf](https://www.eorc.jaxa.jp/ALOS/en/aw3d30/aw3d30v3.2_product_e_e1.2.pdf)

"""

# from pystac_client import Client
import requests
import os
import json

from .downloader import download_by_stream, download_sigletasking


class Alos_DEM:
    def __init__(self):
        self._sources = {
            "microsoft": {
                "url": "https://planetarycomputer.microsoft.com/api/stac/v1/",
                "collection_name": "alos-dem",
            },
        }
        self._hrefs = []

    def list_sources(self):
        """
        返回stac列表

        Returns:
            sources (list): stac列表
        """
        return self._sources

    def add_source(self, source_name, url, collection_name):
        """
        添加自定义stac内容

        Args:
            source_name (str): 源名称
            url (list): STAC根路径地址
            collection_name (str): 数据集名称

        Returns:
            source (dict): 符合条件的url地址列表
        """

        new = {source_name: {"url": url, "collection_name": collection_name}}
        self._sources[source_name] = {"url": url, "collection_name": collection_name}

        return new

    def search(self, source="microsoft", bbox=None, intersects=None):
        """
        搜索符合条件的dem，并返回url地址列表

        Args:
            catalog_url (str): STAC根路径地址，默认为[微软planetarycomputer](https://planetarycomputer.microsoft.com)
            bbox (list): 搜索的矩形范围
            intersects (str): 搜索范围，应符合GeoJSON的geometry属性格式

        Returns:
            hrefs (list): 符合条件的url地址列表
        """

        if bbox is None:
            bbox = [115, 38, 136, 54]
        if source not in self._sources.keys():
            return

        stac = self._sources[source]["url"]
        stac_response = requests.get(stac).json()
        catalog_links = stac_response["links"]
        search = [l["href"] for l in catalog_links if l["rel"] == "search"][0]

        params = {"collections": [self._sources[source]["collection_name"]]}
        if bbox is not None:
            params["bbox"] = bbox
        if intersects is not None:
            params["intersects"] = intersects

        query = requests.post(search, json=params).json()

        features = query["features"]

        for feat in features:
            self._hrefs.append(feat["assets"]["data"]["href"])

        return self._hrefs

    def download(self, save_dir=".", cover=False):
        """
        下载列表中的dem数据

        Args:
            save_dir (str): 本地保存目录
            cover (bool): 若文件已存在，是否覆盖

        """

        if self._hrefs is not None:
            for href in self._hrefs:
                url = requests.utils.urlparse(href)
                file_path = os.path.join(save_dir, url.path.split("/")[-1])

                if os.path.exists(file_path) and not cover:
                    continue

                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))

                download_sigletasking(href, file_path)
