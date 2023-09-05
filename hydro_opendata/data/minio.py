from ..stac.minio import ERA5LCatalog, GPMCatalog, GFSCatalog
from ..s3api.minio import ERA5LReader, GPMReader, GFSReader

class Era5L:
    
    def __init__(self, dataset='wis'):
        
        self._catalog = ERA5LCatalog()
        self._reader = ERA5LReader(dataset)
        
    @property
    def catalog(self):
        return self._catalog
    
    @property
    def reader(self):
        return self._reader
    

class GPM:
    
    def __init__(self, dataset='wis'):
        
        self._catalog = GPMCatalog()
        self._reader = GPMReader(dataset)
        
    @property
    def catalog(self):
        return self._catalog
    
    @property
    def reader(self):
        return self._reader
    
class GFS:
    
    def __init__(self, dataset='wis', variable='tp'):
        
        self._catalog = GFSCatalog(variable)
        self._reader = GFSReader(dataset)
        self._reader.set_default_variable(self._catalog.variable)
        
    @property
    def catalog(self):
        return self._catalog
    
    @property
    def reader(self):
        return self._reader
    