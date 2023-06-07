import numpy as np
import pandas as pd
import geopandas as gpd

class Era5_land():
    
#     def __init__(self):
#         self.id = 'era5-land'
#         self.collection_id = 'era5-land'
#         self.datasources = 'ECMWF'
#         self.description = 'https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview'
#         self.spatialresolution = '0.1 x 0.1; Native resolution is 9 km.'
#         self.temporalresolution = 'hourly'
        
#         self.starttime = np.datetime64('2015-07-01T00:00:00.00')
#         self.endtime = np.datetime64('2022-12-31T23:00:00.00')
#         self.bbox = (115, 38, 136, 54)

    collection_id = 'era5-land'
    datasources = 'ECMWF'
    description = 'https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview'
    spatialresolution = '0.1 x 0.1; Native resolution is 9 km.'
    temporalresolution = 'hourly'

    starttime = np.datetime64('2015-07-01T00:00:00.00')
    endtime = np.datetime64('2022-12-31T23:00:00.00')
    bbox = (115, 38, 136, 54)
    
    @classmethod
    def check(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < Era5_land.starttime:
                starttime = Era5_land.starttime
            if endtime > Era5_land.endtime:
                endtime = Era5_land.endtime
                
            df = pd.DataFrame(
                {
                    'id': [Era5_land.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((115 54,115 38,136 38,136 54,115 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            gs = gdf['geometry']
            ints = gs.intersects(aoi.geometry)
            
            if True in ints.to_list():
                return True
            else:
                return False
        else:
            return None
    

    @classmethod
    def search(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < Era5_land.starttime:
                starttime = Era5_land.starttime
            if endtime > Era5_land.endtime:
                endtime = Era5_land.endtime
                
            df = pd.DataFrame(
                {
                    'id': [Era5_land.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((115 54,115 38,136 38,136 54,115 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            clip = gdf.clip(aoi)
            
            return clip
            
        else:
            return None
        

class GPM_IMERG_Early():

    collection_id = 'gpm-imerg-early'
    datasources = 'NASA & JAXA'
    description = 'https://disc.gsfc.nasa.gov/datasets/GPM_3IMERGHHE_06/summary'
    spatialresolution = '0.1 x 0.1; Native resolution is 9 km. (60°S-60°N)'
    temporalresolution = 'half-hourly'

    starttime = np.datetime64('2015-07-01T00:00:00.00')
    endtime = np.datetime64('2022-12-31T23:00:00.00')
    bbox = (73, 3, 136, 54)
    
    @classmethod
    def check(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < GPM_IMERG_Early.starttime:
                starttime = GPM_IMERG_Early.starttime
            if endtime > GPM_IMERG_Early.endtime:
                endtime = GPM_IMERG_Early.endtime
                
            df = pd.DataFrame(
                {
                    'id': [GPM_IMERG_Early.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((73 54,73 3,136 3,136 54,73 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')

            gs = gdf['geometry']
            ints = gs.intersects(aoi.geometry)
            
            if True in ints.to_list():
                return True
            else:
                return False
            
        else:
            return None

    @classmethod
    def search(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < GPM_IMERG_Early.starttime:
                starttime = GPM_IMERG_Early.starttime
            if endtime > GPM_IMERG_Early.endtime:
                endtime = GPM_IMERG_Early.endtime
                
            df = pd.DataFrame(
                {
                    'id': [GPM_IMERG_Early.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((73 54,73 3,136 3,136 54,73 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            clip = gdf.clip(aoi)
            
            return clip
            
        else:
            return None




class GFS_atmos():

    collection_id = 'gfs_atmos'
    datasources = 'NOAA'
    description = 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php'
    spatialresolution = '0.25 x 0.25'
    temporalresolution = 'hourly; 1-120h'

    starttime = np.datetime64('2015-07-01T00:00:00.00')
    endtime = np.datetime64('2022-12-31T23:00:00.00')
    bbox = (115, 38, 136, 54)

    @classmethod
    def check(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < GFS_atmos.starttime:
                starttime = GFS_atmos.starttime
            if endtime > GFS_atmos.endtime:
                endtime = GFS_atmos.endtime
                
            df = pd.DataFrame(
                {
                    'id': [GFS_atmos.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((115 54,115 38,136 38,136 54,115 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            gs = gdf['geometry']
            ints = gs.intersects(aoi.geometry)
            
            if True in ints.to_list():
                return True
            else:
                return False
            
        else:
            return None

    @classmethod
    def search(cls, starttime, endtime, aoi):
        if starttime < endtime:
            if starttime < GFS_atmos.starttime:
                starttime = GFS_atmos.starttime
            if endtime > GFS_atmos.endtime:
                endtime = GFS_atmos.endtime
                
            df = pd.DataFrame(
                {
                    'id': [GFS_atmos.collection_id],
                    'start_time': [starttime],
                    'end_time': [endtime],
                    'geometry': ['POLYGON((115 54,115 38,136 38,136 54,115 54))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            clip = gdf.clip(aoi)
            
            return clip
            
        else:
            return None