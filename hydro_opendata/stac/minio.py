from ..common import minio_paras, fs
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import json

bucket_name = minio_paras['bucket_name']

class Era5_land():
    
    def __init__(self):

        self.collection_id = 'era5-land'
        self.datasources = 'ECMWF'
        self.description = 'https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview'
        self.spatialresolution = '0.1 x 0.1; Native resolution is 9 km.'
        self.temporalresolution = 'hourly'

        # with fs.open('test/geodata/era5_land.json/')
        self.starttime = np.datetime64('2015-07-01T00:00:00.00')
        self.endtime = np.datetime64('2022-12-31T23:00:00.00')
        self.bbox = (115, 38, 136, 54)

    def search(self, aoi, starttime=None, endtime=None):
        
        if starttime is None:
            starttime = self.starttime
        if endtime is None:
            endtime = self.endtime
        
        if starttime < endtime:
            if starttime < self.starttime:
                starttime = self.starttime
            if endtime > self.endtime:
                endtime = self.endtime
                
            df = pd.DataFrame(
                {
                    'id': [self.collection_id],
                    'start_time': [self.starttime],
                    'end_time': [self.endtime],
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

    def __init__(self):
        self.collection_id = 'gpm-imerg-early'
        self.datasources = 'NASA & JAXA'
        self.description = 'https://disc.gsfc.nasa.gov/datasets/GPM_3IMERGHHE_06/summary'
        self.spatialresolution = '0.1 x 0.1; Native resolution is 9 km. (60°S-60°N)'
        self.temporalresolution = 'half-hourly'

        with fs.open(os.path.join(bucket_name,'geodata/gpm/gpm.json')) as f:
            gpm = json.load(f)
            self.starttime = np.datetime64(gpm['start'])
            self.endtime = np.datetime64(gpm['end'])
            self.bbox = gpm['bbox']

    def search(self, aoi, starttime=None, endtime=None):
        
        if starttime is None:
            starttime = self.starttime
        if endtime is None:
            endtime = self.endtime
            
        if starttime < endtime:
            if starttime < self.starttime:
                starttime = self.starttime
            if endtime > self.endtime:
                endtime = self.endtime
                
            df = pd.DataFrame(
                {
                    'id': [self.collection_id],
                    'start_time': [self.starttime],
                    'end_time': [self.endtime],
                    'geometry': [f'POLYGON(({self.bbox[0]} {self.bbox[3]},{self.bbox[0]} {self.bbox[1]},{self.bbox[2]} {self.bbox[1]},{self.bbox[2]} {self.bbox[3]},{self.bbox[0]} {self.bbox[3]}))']
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            clip = gdf.clip(aoi)
            
            return clip
            
        else:
            return None
        
class GFS_atmos():

    def __init__(self, variable='tp'):
        self.variable = variable
        self.collection_id = f'gfs_atmos.{variable}'
        self.datasources = 'NOAA'
        self.description = 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php'
        self.spatialresolution = '0.25 x 0.25'
        self.temporalresolution = 'hourly; 1-120h'

        with fs.open(os.path.join(bucket_name,'geodata/gfs/gfs.json')) as f:
            gfs = json.load(f)
            self.starttime = np.datetime64(gfs[variable][0]['start'])
            self.endtime = np.datetime64(gfs[variable][-1]['end'])
            # self.bbox = gpm['bbox']

    def search(self, aoi, starttime=None, endtime=None):
        
        if starttime is None:
            starttime = self.starttime
        if endtime is None:
            endtime = self.endtime
        
        if starttime < endtime:
            
            stime = []
            etime = []
            bbox = []
            ids = []
            
            with fs.open(os.path.join(bucket_name,'geodata/gfs/gfs.json')) as f:
                cont = json.load(f)
                clist = cont[self.variable]
            
            for c in clist:

                if starttime < np.datetime64(c['start']):
                    stime.append(np.datetime64(c['start']))
                else:
                    stime.append(starttime)

                if endtime > np.datetime64(c['end']):
                    etime.append(np.datetime64(c['end']))
                else:
                    etime.append(endtime)

                bb = c['bbox']
                bbox.append(f'POLYGON(({bb[0]} {bb[3]},{bb[0]} {bb[1]},{bb[2]} {bb[1]},{bb[2]} {bb[3]},{bb[0]} {bb[3]}))')
                ids.append(self.collection_id)

            df = pd.DataFrame(
                {
                    'id': ids,
                    'start_time': stime,
                    'end_time': etime,
                    'geometry': bbox
                }
            )
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')
            
            clip = gdf.clip(aoi)
            
            return clip
            
        else:
            return None