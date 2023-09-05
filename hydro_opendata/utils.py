import numpy as np
from netCDF4 import Dataset,date2num,num2date
import time
from datetime import datetime, timedelta

def creatspinc(value, data_vars, lats, lons, starttime, filename, resolution):

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
    

def regen_box(bbox, resolution, offset):
    
    lx = bbox[0]
    rx = bbox[2]
    LLON = round(int(lx) + resolution * int((lx - int(lx)) / resolution + 0.5) + offset * (int(lx * 10) / 10 + offset - lx) / abs(int(lx * 10) // 10 + offset - lx + 0.0000001), 3)
    RLON = round(int(rx) + resolution * int((rx - int(rx)) / resolution + 0.5) - offset * (int(rx * 10) / 10 + offset - rx) / abs(int(rx * 10) // 10 + offset - rx + 0.0000001), 3)
    
    by = bbox[1]
    ty = bbox[3]
    BLAT = round(int(by) + resolution * int((by - int(by)) / resolution + 0.5) + offset * (int(by * 10) / 10 + offset - by) / abs(int(by * 10) // 10 + offset - by + 0.0000001), 3)
    TLAT = round(int(ty) + resolution * int((ty - int(ty)) / resolution + 0.5) - offset * (int(ty * 10) / 10 + offset - ty) / abs(int(ty * 10) // 10 + offset - ty + 0.0000001), 3)
    
    # print(LLON,BLAT,RLON,TLAT)
    
    return [LLON,BLAT,RLON,TLAT]