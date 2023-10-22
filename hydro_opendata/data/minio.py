from ..catalog.minio import ERA5LCatalog, GPMCatalog, GFSCatalog
from ..reader.minio import ERA5LReader, GPMReader, GFSReader


class Era5L:
    def __init__(self):
        self._catalog = ERA5LCatalog()
        self._reader = ERA5LReader()

    @property
    def catalog(self):
        return self._catalog

    @property
    def reader(self):
        return self._reader


class GPM:
    def __init__(self):
        self._catalog = GPMCatalog()
        self._reader = GPMReader()

    @property
    def catalog(self):
        return self._catalog

    @property
    def reader(self):
        return self._reader


class GFS:
    def __init__(self, variable="tp"):
        self._catalog = GFSCatalog(variable)
        self._reader = GFSReader()
        self._reader.set_default_variable(self._catalog.variable)

    @property
    def catalog(self):
        return self._catalog

    @property
    def reader(self):
        return self._reader
