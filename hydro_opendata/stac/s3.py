import requests as r
import datetime
import numpy as np

from ..utils import validate


class LandsatCatalog:
    def __init__(self):
        self._root = (
            "https://landsatlook.usgs.gov/stac-server"  # Landsat STAC API Endpoint
        )

        stac_response = r.get(self._root).json()
        catalog_links = stac_response["links"]
        self._search = [l["href"] for l in catalog_links if l["rel"] == "search"][
            0
        ]  # retreive search endpoint from STAC Catalogs

    @property
    def root(self):
        return self._root

    def set_root(self, url):
        self._root = url

        stac_response = r.get(self._root).json()
        catalog_links = stac_response["links"]
        self._search = [l["href"] for l in catalog_links if l["rel"] == "search"][
            0
        ]  # retreive search endpoint from STAC Catalogs

    def search(
        self,
        start_date: str = "1972-01-01",
        end_date: str = None,
        aoi=None,
        bbox=None,
        limit=1000,
    ):
        start_date = validate(start_date, "%Y-%m-%d", "start_date格式错误，应为yyyy-mm-dd")
        if end_date is None:
            end_date = datetime.date.today().strftime("%Y-%m-%d")
        end_date = validate(end_date, "%Y-%m-%d", "end_date格式错误，应为yyyy-mm-dd")

        if start_date > end_date:
            raise ValueError("开始日期不能大于结束日期！")

        params = {}
        params["limit"] = limit

        if aoi is None and bbox is None:
            raise ValueError("需指定aoi或bbox的范围！")
        elif bbox is None:
            params["intersects"] = aoi
        else:
            params["bbox"] = bbox

        landsat_dataset = []

        # dt = f"{start_date.strftime("%Y-%m-%d")}T00:00:00Z/{end_date.strftime("%Y-%m-%d")}T23:59:59Z"
        for y in range(end_date.year, start_date.year - 1, -1):
            if y == start_date.year:
                dt = start_date.strftime("%Y-%m-%d") + "T00:00:00Z"
            else:
                dt = f"{str(y)}-01-01T00:00:00Z"

            if y == end_date.year:
                dt = dt + "/" + end_date.strftime("%Y-%m-%d") + "T23:59:59Z"
            else:
                dt = dt + f"/{str(y)}-12-31T23:59:59Z"

            params["datetime"] = dt

            query = r.post(
                self._search,
                json=params,
            ).json()
            try:
                landsat_dataset += query["features"]
            except:
                raise Exception(query)

        return landsat_dataset

    def get_hrefs(self, ids):
        hrefs = []
        params = {}
        params["ids"] = ids

        query = r.post(
            self._search,
            json=params,
        ).json()
        for feature in query["features"]:
            feature_hrefs = []
            for key in feature["assets"].keys():
                if "data" in feature["assets"][key]["roles"]:
                    feature_hrefs.append(feature["assets"][key]["href"])
            hrefs.append(feature_hrefs)

        return hrefs


from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session


class SentinelCatalog:
    def __init__(self):
        self._root = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/"

        stac_response = r.get(self._root).json()
        catalog_links = stac_response["links"]
        self._search = [l["href"] for l in catalog_links if l["rel"] == "search"][
            0
        ]  # retreive search endpoint from STAC Catalogs

        self._oauth = None

    @property
    def root(self):
        return self._root

    @property
    def token(self):
        return self._oauth

    def set_root(self, url):
        self._root = url

        stac_response = r.get(self._root).json()
        catalog_links = stac_response["links"]
        self._search = [l["href"] for l in catalog_links if l["rel"] == "search"][
            0
        ]  # retreive search endpoint from STAC Catalogs

    def get_token(self, client_id=None, client_secret=None):
        if client_id is not None:
            self.__client_id = client_id
        if client_secret is not None:
            self.__client_secret = client_secret
        # Your client credentials
        # self.__client_id = '9bff23bb-227a-4a35-9b36-22ab23b9dfaf'
        # self.__client_secret = '7Ea%6-CM%hG,k![.DQB?i-bw&Y*Q.eG6c:_{uyUW'

        if self.__client_id is None or self.__client_secret is None:
            raise ValueError("认证信息错误！")

        # Create a session
        client = BackendApplicationClient(client_id=self.__client_id)
        oauth = OAuth2Session(client=client)

        # Get token for the session
        token = oauth.fetch_token(
            token_url="https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            client_secret=client_secret,
        )

        # All requests using this session will have an access token automatically added
        # resp = oauth.get("https://services.sentinel-hub.com/oauth/tokeninfo")
        # print(resp.content)
        def sentinelhub_compliance_hook(response):
            response.raise_for_status()
            return response

        oauth.register_compliance_hook(
            "access_token_response", sentinelhub_compliance_hook
        )

        self._oauth = oauth

    def search(
        self,
        start_date: str = "2013-01-01",
        end_date: str = None,
        collections=["sentinel-1-grd", "sentinel-2-l1c", "sentinel-2-l2a"],
        aoi=None,
        bbox=None,
        limit=100,
    ):
        if self._oauth is None:
            raise ValueError("请验证token！")

        start_date = validate(start_date, "%Y-%m-%d", "start_date格式错误，应为yyyy-mm-dd")
        if end_date is None:
            end_date = datetime.date.today().strftime("%Y-%m-%d")
        end_date = validate(end_date, "%Y-%m-%d", "end_date格式错误，应为yyyy-mm-dd")

        if start_date > end_date:
            raise ValueError("开始日期不能大于结束日期！")

        params = {}
        params["limit"] = limit

        if aoi is None and bbox is None:
            raise ValueError("需指定aoi或bbox的范围！")
        elif bbox is None:
            params["intersects"] = aoi
        else:
            params["bbox"] = bbox

        sentinel_dataset = []

        d = start_date
        delta = datetime.timedelta(days=1)
        while d <= end_date:
            dt = d.strftime("%Y-%m-%d")
            params["datetime"] = f"{dt}T00:00:00Z/{dt}T23:59:59Z"

            for col in collections:
                params["collections"] = [col]

                try:
                    query = self._oauth.post(
                        self._search,
                        json=params,
                    ).json()
                    sentinel_dataset += query["features"]
                except:
                    raise Exception(query)
                    # print(query)

                d += delta

        return sentinel_dataset

    def get_hrefs(self, ids):
        hrefs = []
        params = {}
        params["ids"] = ids

        query = self._oauth.post(
            self._search,
            json=params,
        ).json()
        for feature in query["features"]:
            feature_hrefs = []
            for key in feature["assets"].keys():
                if "data" in feature["assets"][key]["roles"]:
                    feature_hrefs.append(feature["assets"][key]["href"])
            hrefs.append(feature_hrefs)

        return hrefs
