from .wis_s3api import paras
import os
import s3fs
import numpy as np
import xarray as xr
from datetime import date

endpoint_url = paras['endpoint_url']
access_key = paras['access_key']
secret_key = paras['secret_key']
bucket_path = paras['bucket_path']

home_path = os.environ['HOME']

if os.path.exists(os.path.join(home_path,'.wiss3api')):
    for line in open(os.path.join(home_path,'.wiss3api')):
        key = line.split('=')[0].strip()
        value = line.split('=')[1].strip()
        # print(key,value)
        if key == 'endpoint_url':
            endpoint_url = value
        elif key == 'access_key':
            access_key = value
        elif key == 'secret_key':
            secret_key = value
        elif key == 'bucket_path':
            bucket_path = value

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": endpoint_url}, 
    key=access_key, 
    secret=secret_key
)

create_date = date(2022,1,1)
box = (115,38,136,54)
variables = [
    'downward_shortwave_radiation_flux',
    'precipitable_water_entire_atmosphere',
    'relative_humidity_2m_above_ground',
    'specific_humidity_2m_above_ground',
    'temperature_2m_above_ground',
    'total_cloud_cover_entire_atmosphere',
    'total_precipitation_surface',
    'u_component_of_wind_10m_above_ground',
    'v_component_of_wind_10m_above_ground',
]

def _bbox(bbox, resolution, offset):
    
    lx = bbox[0]
    rx = bbox[2]   
    LLON = round(int(lx) + resolution * int((lx - int(lx)) / resolution + 0.5) + offset * (int(lx * 10) / 10 + offset - lx) / abs(int(lx * 10) // 10 + offset - lx + 0.0000001), 3)
    RLON = round(int(rx) + resolution * int((rx - int(rx)) / resolution + 0.5) - offset * (int(rx * 10) / 10 + offset - rx) / abs(int(rx * 10) // 10 + offset - rx + 0.0000001), 3)
    
    by = bbox[1]
    ty = bbox[3]
    BLAT = round(int(by) + resolution * int((by - int(by)) / resolution + 0.5) + offset * (int(by * 10) / 10 + offset - by) / abs(int(by * 10) // 10 + offset - by + 0.0000001), 3)
    TLAT = round(int(ty) + resolution * int((ty - int(ty)) / resolution + 0.5) - offset * (int(ty * 10) / 10 + offset - ty) / abs(int(ty * 10) // 10 + offset - ty + 0.0000001), 3)
    
    # print(LLON,BLAT,RLON,TLAT)
    
    return LLON,BLAT,RLON,TLAT


def open_dataset(data_variables=variables, creation_date=create_date, creation_time='00', valid_time=range(1,121), bbox=box, time_chunks=24):
    
    year = str(creation_date.year)
    month = str(creation_date.month).zfill(2)
    day = str(creation_date.day).zfill(2)

    chunks = {"time": time_chunks}
    ds = xr.open_dataset(
        "reference://", 
        engine="zarr", 
        chunks=chunks,
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fs.open('s3://' + bucket_path + f'gfs/gfs_history/{year}/{month}/{day}/gfs{year+month+day}.t{creation_time}z.0p25.json'), 
                "remote_protocol": "s3",
                "remote_options": {
                    'client_kwargs': {'endpoint_url': endpoint_url}, 
                    'key': access_key, 
                    'secret': secret_key}
            }
        }      
    )
    
    # ds = ds.filter_by_attrs(long_name=lambda v: v in data_variables)
    ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    ds = ds.transpose('time','valid_time','lon','lat')
    
    bbox = _bbox(bbox, 0.25, 0)
    
    if bbox[0] < box[0]:
        left = box[0]
    else:
        left = bbox[0]
        
    if bbox[1] < box[1]:
        bottom = box[1]
    else:
        bottom = bbox[1]
    
    if bbox[2] > box[2]:
        right = box[2]
    else:
        right = bbox[2]
    
    if bbox[3] > box[3]:
        top = box[3]
    else:
        top = bbox[3]
    
    longitudes = slice(left - 0.00001, right + 0.00001)
    latitudes = slice(bottom - 0.00001, top + 0.00001)
    
    ds = ds.sortby('lat', ascending=True)
    ds = ds.sel(lon=longitudes, lat=latitudes)
    ds = ds.isel(valid_time=valid_time)
    
    if len(data_variables) < 9 and len(data_variables) > 0:
        dss = []
        for v in data_variables:
            dss.append(ds[v])
        return xr.merge(dss)
    else:
        return ds


import geopandas as gpd

def from_shp(data_variables=variables, creation_date=create_date, creation_time='00', valid_time=range(1,121), shp=None, time_chunks=24):

    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = _bbox((b.loc[0]['minx'],b.loc[0]['miny'],b.loc[0]['maxx'],b.loc[0]['maxy']), 0.1, 0)

    ds = open_dataset(data_variables, creation_date, creation_time, valid_time, bbox, time_chunks)
    
    return ds