from .wis_s3api import paras
import os
import s3fs
import numpy as np
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

start = np.datetime64('2015-01-01T00:00:00.000000000')
end = np.datetime64('2021-12-31T23:00:00.000000000')
box = (115,38,136,54)
variables = [
    '10 metre U wind component',
    '10 metre V wind component',
    '2 metre dewpoint temperature',
    '2 metre temperature',
    'Evaporation',
    'Evaporation from bare soil',
    'Evaporation from open water surfaces excluding oceans',
    'Evaporation from the top of canopy',
    'Evaporation from vegetation transpiration',
    'Forecast albedo',
    'Lake bottom temperature',
    'Lake ice total depth',
    'Lake ice surface temperature',
    'Lake mix-layer depth',
    'Lake mix-layer temperature',
    'Lake shape factor',
    'Lake total layer temperature',
    'Leaf area index, high vegetation',
    'Leaf area index, low vegetation',
    'Potential evaporation',
    'Runoff',
    'Skin reservoir content',
    'Skin temperature',
    'Snow albedo',
    'Snow cover',
    'Snow density',
    'Snow depth',
    'Snow depth water equivalent',
    'Snow evaporation',
    'Snowfall',
    'Snowmelt',
    'Soil temperature level 1',
    'Soil temperature level 2',
    'Soil temperature level 3',
    'Soil temperature level 4',
    'Sub-surface runoff',
    'Surface latent heat flux',
    'Surface net solar radiation',
    'Surface net thermal radiation',
    'Surface pressure',
    'Surface runoff',
    'Surface sensible heat flux',
    'Surface solar radiation downwards',
    'Surface thermal radiation downwards',
    'Temperature of snow layer',
    'Total precipitation',
    'Volumetric soil water layer 1',
    'Volumetric soil water layer 2',
    'Volumetric soil water layer 3',
    'Volumetric soil water layer 4'
]
accumulated = [
    # '10 metre U wind component',
    # '10 metre V wind component',
    # '2 metre dewpoint temperature',
    # '2 metre temperature',
    'Evaporation',
    'Evaporation from bare soil', 
    'Evaporation from open water surfaces excluding oceans',
    'Evaporation from the top of canopy',
    'Evaporation from vegetation transpiration',
    # 'Forecast albedo',
    # 'Lake bottom temperature',
    # 'Lake ice total depth',
    # 'Lake ice surface temperature',
    # 'Lake mix-layer depth',
    # 'Lake mix-layer temperature',
    # 'Lake shape factor',
    # 'Lake total layer temperature',
    # 'Leaf area index, high vegetation',
    # 'Leaf area index, low vegetation',
    'Potential evaporation',
    'Runoff',
    # 'Skin reservoir content',
    # 'Skin temperature',
    # 'Snow albedo',
    # 'Snow cover',
    # 'Snow density',
    # 'Snow depth',
    # 'Snow depth water equivalent',
    'Snow evaporation',
    'Snowfall',
    'Snowmelt',
    # 'Soil temperature level 1',
    # 'Soil temperature level 2',
    # 'Soil temperature level 3',
    # 'Soil temperature level 4',
    'Sub-surface runoff',
    'Surface latent heat flux',
    'Surface net solar radiation',
    'Surface net thermal radiation',
    # 'Surface pressure',
    'Surface runoff',
    'Surface sensible heat flux',
    'Surface solar radiation downwards',
    'Surface thermal radiation downwards',
    # 'Temperature of snow layer',
    'Total precipitation'
    # 'Volumetric soil water layer 1',
    # 'Volumetric soil water layer 2',
    # 'Volumetric soil water layer 3',
    # 'Volumetric soil water layer 4'
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


def open_dataset(data_variables=variables, start_time=start, end_time=end, bbox=box, time_chunks=24):

    chunks = {"time": time_chunks}
    ds = xr.open_dataset(
        "reference://", 
        engine="zarr", 
        chunks=chunks,
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fs.open('s3://' + bucket_path + 'era5_land/era5_land_.json'), 
                "remote_protocol": "s3",
                "remote_options": {
                    'client_kwargs': {'endpoint_url': endpoint_url}, 
                    'key': access_key, 
                    'secret': secret_key}
            }
        }      
    )
    
    ds = ds.filter_by_attrs(long_name=lambda v: v in data_variables)
    ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    ds = ds.transpose('time','lon','lat')
    
    if start_time < start:
        start_time = start
    
    if end_time > end:
        end_time = end
    
    times = slice(start_time, end_time)
    ds = ds.sel(time=times)
    
    bbox = _bbox(bbox, 0.1, 0)
    
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


import geopandas as gpd

def from_shp(data_variables=variables, start_time=start, end_time=end, shp=None, time_chunks=24):

    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = _bbox((b.loc[0]['minx'],b.loc[0]['miny'],b.loc[0]['maxx'],b.loc[0]['maxy']), 0.1, 0)

    ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)
    
    return ds


from netCDF4 import Dataset,date2num,num2date
import time
from datetime import datetime, timedelta

def _creatspinc(value, data_vars, lats, lons, starttime, filename, resolution):

    gridspi = Dataset(filename, 'w', format='NETCDF4')

    # dimensions
    gridspi.createDimension('time', value[0].shape[0])
    gridspi.createDimension('lat', value[0].shape[2])   #len(lat)
    gridspi.createDimension('lon', value[0].shape[1])

    # Create coordinate variables for dimensions
    times = gridspi.createVariable('time', np.float64, ('time',))
    latitudes = gridspi.createVariable('lat', np.float32, ('lat',))
    longitudes = gridspi.createVariable('lon', np.float32, ('lon',))

    # Create the actual variable
    for var,attr in data_vars.items():
        gridspi.createVariable(var, np.float32, ('time', 'lon', 'lat',))

    # Global Attributes
    gridspi.description = 'var'
    gridspi.history = 'Created ' + time.ctime(time.time())
    gridspi.source = 'netCDF4 python module tutorial'

    # Variable Attributes
    latitudes.units = 'degree_north'
    longitudes.units = 'degree_east'
    times.units = 'days since 1970-01-01 00:00:00'
    times.calendar = 'gregorian'

    # data
    latitudes[:] = lats
    longitudes[:] = lons
    
    # Fill in times
    dates = []
    if resolution == 'daily':
        for n in range(value[0].shape[0]):
            dates.append(starttime + n)
        times[:] = dates[:]
    
    elif resolution == '6-hourly':
        # for n in range(value[0].shape[0]):
        #     dates.append(starttime + (n+1) * np.timedelta64(6, 'h'))
        
        for n in range(value[0].shape[0]):
            dates.append(starttime + (n+1) * timedelta(hours=6))

        times[:] = date2num(dates, units = times.units,calendar = times.calendar)
        # print 'time values (in units %s): ' % times.units +'\n', times[:]
        dates = num2date(times[:], units=times.units, calendar=times.calendar)

    

    # Fill in values  
    i = 0
    for var,attr in data_vars.items():
        gridspi.variables[var].long_name = attr['long_name']
        gridspi.variables[var].units = attr['units']
        gridspi.variables[var][:]= value[i][:]
        i = i + 1

    gridspi.close()
    
    

def to_netcdf(data_variables=variables, start_time=start, end_time=end, shp=None, resolution='hourly', save_file='era5.nc', time_chunks=24):
    
    gdf = gpd.GeoDataFrame.from_file(shp)
    b = gdf.bounds
    bbox = _bbox((b.loc[0]['minx'],b.loc[0]['miny'],b.loc[0]['maxx'],b.loc[0]['maxy']), 0.1, 0)
    
    if resolution == 'hourly':
        
        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)
    
        if ds.to_netcdf(save_file) == None:
            print(save_file, '已生成')
            ds = xr.open_dataset(save_file)
            return ds

    if resolution == 'daily':
        
        start_time = np.datetime64(f'{str(start_time)[:10]}T01:00:00.000000000')        
        end_time = np.datetime64(str(end_time)[:10])+1
        end_time = np.datetime64(f'{str(end_time)}T00:00:00.000000000')
    
        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)
        
        days = ds['time'].size // 24
        
        data_vars = {}
        for k,v in ds.data_vars.items():
            data_vars[k] = v.attrs
        
        daily_arr = []
        
        for var,attr in data_vars.items():
            
            a = ds[var].to_numpy()
            
            if attr['long_name'] in accumulated:
                xlist = [x for x in range(a.shape[0]) if x%24!=23]
                _a = np.delete(a,xlist,axis=0)
                
                daily_arr.append(_a)
                
            else:
                r = np.split(a,days,axis=0)
                _r = [np.expand_dims(np.mean(r[i],axis=0),axis=0) for i in range(len(r))]
                __r = np.concatenate(_r)
                
                daily_arr.append(__r)
                
        lats = ds['lat'].to_numpy()
        lons = ds['lon'].to_numpy()
        
        start_time = np.datetime64(str(start_time)[:10])
        
        _creatspinc(daily_arr, data_vars, lats, lons, start_time, save_file, 'daily')
        
        new = xr.open_dataset(save_file)
        print(save_file, '已生成')
        return new
    
    if resolution == '6-hourly':
        
        start_time = np.datetime64(f'{str(start_time)[:10]}T01:00:00.000000000')        
        end_time = np.datetime64(str(end_time)[:10])+1
        end_time = np.datetime64(f'{str(end_time)}T00:00:00.000000000')
    
        ds = open_dataset(data_variables, start_time, end_time, bbox, time_chunks)
        
        days = ds['time'].size // 6
        
        data_vars = {}
        for k,v in ds.data_vars.items():
            data_vars[k] = v.attrs
        
        daily_arr = []
        
        for var,attr in data_vars.items():
            
            a = ds[var].to_numpy()
            
            if attr['long_name'] in accumulated:
                xlist = [x for x in range(a.shape[0]) if x%6!=5]
                _a = np.delete(a,xlist,axis=0)
                
                daily_arr.append(_a)
                
            else:
                r = np.split(a,days,axis=0)
                _r = [np.expand_dims(np.mean(r[i],axis=0),axis=0) for i in range(len(r))]
                __r = np.concatenate(_r)
                
                daily_arr.append(__r)
                
        lats = ds['lat'].to_numpy()
        lons = ds['lon'].to_numpy()
        
        # start_time = np.datetime64(f'{str(start_time)[:10]}') 
        year = int(f'{str(start_time)[0:4]}')
        month = int(f'{str(start_time)[5:7]}')
        day = int(f'{str(start_time)[8:10]}')
        dt = datetime(year,month,day,0,0,0)
        
        _creatspinc(daily_arr, data_vars, lats, lons, dt, save_file, '6-hourly')
        
        new = xr.open_dataset(save_file)
        print(save_file, '已生成')
        return new