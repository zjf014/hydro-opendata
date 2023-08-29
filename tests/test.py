import sys
sys.path.append('..')

print('----------------------------------------------------------------------------')
from hydro_opendata.stac.minio import Era5_land, GPM_IMERG_Early, GFS_atmos
import geopandas as gpd

aoi = gpd.read_file('test.geojson')
print(aoi)
print()

era5 = Era5_land()
e = era5.search(aoi=aoi)
print(e)
print()

gpm = GPM_IMERG_Early()
g = gpm.search(aoi=aoi)
print(g)
print()

gfs = GFS_atmos('tp')
f = gfs.search(aoi=aoi)
print(f)
print()

print('----------------------------------------------------------------------------')
from hydro_opendata.s3api import era5, gfs, gpm
import numpy as np

ds1 = era5.open_dataset()
print(ds1)
print()

ds2 = gfs.open_dataset(creation_date=np.datetime64("2022-08-01"))
print(ds2)
print()

start_time=np.datetime64("2023-08-23T00:00:00.000000000")
end_time=np.datetime64("2023-08-24T23:30:00.000000000")
ds3 = gpm.from_aoi(start_time=start_time, end_time=end_time, aoi=aoi)
print(ds3)

print('----------------------------------------------------------------------------')
from hydro_opendata.downloader.dem import Alos_DEM
dem = Alos_DEM()
# sources = dem.list_sources()
# print(sources)

# new = dem.add_source('aws','https:\\','dem')
# print(new)
# sources = dem.list_sources()
# print(sources)
hrefs = dem.search()
print(len(hrefs))