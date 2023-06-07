from minio import *
from stac import *

class wis_stac():

    datasources = [
        'era5-land',
        'gpm-imerg-early',
        'gfs_atmos',
        'landsat-8-l1-c1',
        'sentinel-s2-l2a-cogs'
    ]
    
    def __init__(self, aoi, start_date, end_date,datasources=datasources):
        self.__aoi = aoi
        self.__start_date = start_date
        self.__end_date = end_date
        self.__collections = self.check_minio(aoi, start_date, end_date,datasources) + self.check_stac(aoi, start_date, end_date,datasources)
    
    @property
    def collections(self):
        return self.__collections
    

    def check_minio(self, aoi, start_date, end_date,datasources=datasources):

        minio_catalog = []
        if 'era5-land' in datasources:
            ints = Era5_land.check(start_date, end_date, aoi)
            if ints:
                minio_catalog.append('era5-land')
        if 'gpm-imerg-early' in datasources:
            ints = GPM_IMERG_Early.check(start_date, end_date, aoi)
            if ints:
                minio_catalog.append('gpm-imerg-early')
        if 'gfs_atmos' in datasources:
            ints = GFS_atmos.check(start_date, end_date, aoi)
            if ints:
                minio_catalog.append('gfs_atmos')

        return minio_catalog

    def check_stac(self, aoi, start_date, end_date,datasources=datasources):
        collections = []
        if 'landsat-8-l1-c1' in datasources:
            collections.append('landsat-8-l1-c1')
        if 'sentinel-s2-l2a-cogs' in datasources:
            collections.append('sentinel-s2-l2a-cogs')

        items = search(aoi, start_date, end_date, collections)
        return catalog(items)
    
    
    def items(self, collection_id):
        if collection_id == 'era5-land' and collection_id in self.__collections:
            return Era5_land.search(self.__start_date,self.__end_date,self.__aoi)
        elif collection_id == 'gpm-imerg-early' and collection_id in self.__collections:
            return GPM_IMERG_Early.search(self.__start_date,self.__end_date,self.__aoi)
        elif collection_id == 'gfs_atmos' and collection_id in self.__collections:
            return GFS_atmos.search(self.__start_date,self.__end_date,self.__aoi)
        elif collection_id == 'landsat-8-l1-c1' and collection_id in self.__collections:
            items = search(self.__aoi, self.__start_date, self.__end_date, [collection_id])
            return extent(items,collection_id)
        elif collection_id == 'sentinel-s2-l2a-cogs' and collection_id in self.__collections:
            items = search(self.__aoi, self.__start_date, self.__end_date, [collection_id])
            return extent(items,collection_id)


            
            

    