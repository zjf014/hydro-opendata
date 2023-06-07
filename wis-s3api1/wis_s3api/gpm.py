from .wis_s3api import paras
import os
import numpy as np
import s3fs
import xarray as xr

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

start = np.datetime64("2016-01-01T00:00:00.000000000")
end = np.datetime64("2022-12-31T23:30:00.000000000")
box = (73.05, 3.05, 135.95, 53.95)
variables=[
    'HQobservationTime',
    'HQprecipSource',
    'HQprecipitation',
    'IRkalmanFilterWeight',
    'IRprecipitation',
    'precipitationCal',
    'precipitationUncal',
    'probabilityLiquidPrecipitation',
    'randomError'
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


def get_dataset_year(start_time=start, end_time=end, bbox=box, time_chunks=24):
    
    year = str(start_time)[:4]
    
    chunks = {"time": time_chunks}
    ds = xr.open_dataset(
        "reference://", 
        engine="zarr", 
        chunks=chunks,
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fs.open('s3://' + bucket_path + f'gpm/{year}/gpm{year}_inc.json'), 
                "remote_protocol": "s3",
                "remote_options": {
                    'client_kwargs': {'endpoint_url': endpoint_url}, 
                    'key': access_key, 
                    'secret': secret_key}
            }
        }      
    )
    
    ds = ds['precipitationCal']
    # ds.to_dataframe().filter(['precipitationCal','precipitationUncal']).to_xarray()
    
    # ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    ds = ds.transpose('time','lon','lat')
    
    if start_time < start:
        start_time = start
    
    if end_time > end:
        end_time = end
    
    times = slice(start_time, end_time)
    ds = ds.sel(time=times)
    
    bbox = _bbox(bbox, 0.1, 0.05)
    
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
    
    return ds

def open_dataset(start_time=start, end_time=end, bbox=box, time_chunks=24):
    
    year_start = int(str(start_time)[:4])
    year_end = int(str(end_time)[:4])
    
    if year_start == year_end:

        ds = get_dataset_year(start_time=start_time, end_time=end_time, bbox=bbox, time_chunks=time_chunks)
        return ds
    
    elif year_start < year_end:
        
        dss = []
        years = range(year_start, year_end + 1)
        for year in years:
            
            if year == year_start:
                
                dss.append(get_dataset_year(start_time=start_time, end_time=np.datetime64(f"{year}-12-31T23:30:00.000000000"), bbox=bbox, time_chunks=time_chunks))
                
            elif year == year_end:
                
                dss.append(get_dataset_year(start_time=np.datetime64(f"{year}-01-01T00:00:00.000000000"), end_time=end_time, bbox=bbox, time_chunks=time_chunks))

            else:
                dss.append(get_dataset_year(
                    start_time=np.datetime64(f"{year}-01-01T00:00:00.000000000"), 
                    end_time=np.datetime64(f"{year}-12-31T23:30:00.000000000"), 
                    bbox=bbox, 
                    time_chunks=time_chunks
                ))
        return xr.merge(dss)


import geopandas as gpd

def from_shp(start_time=start, end_time=end, shp=None, time_chunks=24):

    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = _bbox((b.loc[0]['minx'],b.loc[0]['miny'],b.loc[0]['maxx'],b.loc[0]['maxy']), 0.1, 0.05)

    ds = open_dataset(start_time, end_time, bbox, time_chunks)
    
    return ds