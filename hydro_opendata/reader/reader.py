"""
Author: Wenyu Ouyang
Date: 2023-10-31 09:26:31
LastEditTime: 2023-10-31 11:39:25
LastEditors: Wenyu Ouyang
Description: Interface for reader
FilePath: \hydro_opendata\hydro_opendata\reader\reader.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
from abc import ABC, abstractmethod


class AOI:
    def __init__(self, aoi_type, aoi_param):
        self._aoi_type = aoi_type  # can be "grid", "station", "basin" etc.
        self._aoi_param = aoi_param  # this can be a bounding box, coordinates, etc.

    @property
    def aoi_type(self):
        return self._aoi_type

    @property
    def aoi_param(self):
        return self._aoi_param

    def get_mask(self):
        # If it's a polygon, return its mask for data extraction
        pass

    # ... any other useful methods to describe or manipulate the AOI


class GPMDataHandler:
    def handle(self, configuration):
        # Based on configuration, read and handle GPM data specifically
        # For example:
        # data = some_reading_function(configuration)
        pass


class GFSDataHandler:
    def handle(self, configuration):
        # Based on configuration, read and handle GFS data specifically
        pass


class DataReaderStrategy(ABC):
    @abstractmethod
    def read(self, path: str, aoi: AOI):
        pass


class AbstractFileReader(DataReaderStrategy):
    def __init__(self, data_handler):
        self.data_handler = data_handler

    @abstractmethod
    def configure(self, path: str, aoi: AOI):
        pass

    def read(self, path: str, aoi: AOI):
        configuration = self.configure(path, aoi)
        return self.data_handler.handle(configuration)


class LocalFileReader(AbstractFileReader):
    def configure(self, path: str, aoi: AOI):
        return {"type": "local", "path": path, "aoi": aoi}


class MinioFileReader(AbstractFileReader):
    def __init__(self, minio_client, data_handler):
        super().__init__(data_handler)
        self.client = minio_client

    def configure(self, path: str, aoi: AOI):
        return {"type": "minio", "bucket": "your_bucket_name", "path": path, "aoi": aoi}
