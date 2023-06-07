'''
该模块用于alos-dem数据下载

数据源: [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/dataset/alos-dem)

数据目录: [https://planetarycomputer.microsoft.com/api/stac/v1/](https://planetarycomputer.microsoft.com/api/stac/v1/)

数据说明: [https://www.eorc.jaxa.jp/ALOS/en/aw3d30/aw3d30v3.2_product_e_e1.2.pdf](https://www.eorc.jaxa.jp/ALOS/en/aw3d30/aw3d30v3.2_product_e_e1.2.pdf)

- `search` - 返回符合搜索条件的dem数据下载地址列表
- `download` - 将列表中的数据下载到本地
'''

from pystac_client import Client
import requests
import os

from .downloader import download_by_stream, download_sigletasking

hrefs=[]

def search(bbox=[115,38,136,54],intersects=None):
    '''
    搜索符合条件的dem，并返回url地址列表

    Args:
        bbox (list): 搜索的矩形范围
        intersects (str): 搜索范围，应符合GeoJSON的geometry属性格式

    Returns:
        hrefs (list): 符合条件的url地址列表
    '''

    catalog=Client.open('https://planetarycomputer.microsoft.com/api/stac/v1/')
    search=catalog.search(
        collections=["alos-dem"], 
        bbox=bbox, 
        intersects=intersects,
    )

    for ic in search.item_collections():
        for item in ic:
            hrefs.append(item.get_assets()['data'].href)

    return hrefs

from .downloader import download_from_url

def download(save_dir='.',cover=False):
    '''
    下载列表中的dem数据

    Args:
        save_dir (str): 本地保存目录
        cover (bool): 若文件已存在，是否覆盖

    '''

    if hrefs is not None:

        for href in hrefs:
            url=requests.utils.urlparse(href)
            file_path=os.path.join(save_dir,url.path.split('/')[-1])

            if os.path.exists(file_path) and not cover:
                continue

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))

            download_sigletasking(href,file_path)