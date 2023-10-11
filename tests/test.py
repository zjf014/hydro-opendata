import sys
sys.path.append('/home/zhujianfeng/wis/hydro-opendata/')

# print(sys.path)

# print('----------------------------------------------------------------------------')
# from hydro_opendata.stac.minio import ERA5LCatalog, GPMCatalog, GFSCatalog
import geopandas as gpd

aoi = gpd.read_file('test.geojson')
# print(aoi)
# print()

# era5 = ERA5LCatalog()
# print(era5.datasets)
# e = era5.search(aoi=aoi)
# print(e)
# print()

# gpm = GPMCatalog()
# print(gpm.datasets)
# g = gpm.search(aoi=aoi)
# print(g)
# print()

# gfs = GFSCatalog()
# print(gfs.datasets)
# f = gfs.search(aoi=aoi)
# print(f)
# print()

# print('----------------------------------------------------------------------------')
from hydro_opendata.s3api.minio import ERA5LReader, GPMReader, GFSReader
import numpy as np


# 指定开始及结束时间
# start_time=np.datetime64("2021-06-01T00:00:00.000000000")
# end_time=np.datetime64("2021-06-30T23:00:00.000000000")

# 通过指定四至范围读取
# bbox=(121,39,123,40)

# era5 = ERA5LReader()
# ds1 = era5.open_dataset(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, dataset='wis', bbox=bbox)
# print(ds1)
# print()

# gfs = GFSReader()
# ds2 = gfs.open_dataset(creation_date=np.datetime64("2022-08-01"), dataset='wis')
# print(ds2)
# print()

start_time=np.datetime64("2023-06-01T00:00:00.000000000")
end_time=np.datetime64("2023-06-30T23:30:00.000000000")
bbox=(-123,39,-121,40)
gpm = GPMReader()
ds3 = gpm.open_dataset(start_time=start_time, end_time=end_time, dataset='camels', bbox=bbox, time_resolution='1d')
print(ds3)

# print('----------------------------------------------------------------------------')
# from hydro_opendata.downloader.dem import Alos_DEM
# dem = Alos_DEM()
# # sources = dem.list_sources()
# # print(sources)

# # new = dem.add_source('aws','https:\\','dem')
# # print(new)
# # sources = dem.list_sources()
# # print(sources)
# hrefs = dem.search()
# print(len(hrefs))



# ds = gr.open_dataset(start_time=start_time, end_time=end_time, bbox=(134,52, 137,56))
# print(ds)

# print('----------------------------------------------------------------------------')

from hydro_opendata.data.minio import Era5L, GPM, GFS

# era5 = Era5L()
# print(era5.catalog.spatial_resolution)
# print(era5.catalog.datasets)

# start_time=np.datetime64("2021-06-30T00:00:00")
# end_time=np.datetime64("2021-07-31T23:30:00")
# e = era5.catalog.search(aoi=aoi, start_time=start_time, end_time=end_time)
# print(e)


# eds = era5.reader.open_dataset(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, dataset='wis', bbox=(121,39,122,40))
# print(eds)
# print()
# eds = era5.reader.from_shp(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, dataset='wis', shp='test.geojson')
# print(eds)
# print()
# eds = era5.reader.from_aoi(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, dataset='wis', aoi=aoi)
# print(eds)
# print()

# eds = era5.reader.to_netcdf(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, dataset='wis', shp='test.geojson', resolution='6-hourly')
# print(eds)
# print()

# print('----------------------------------------------------------------------------')

# gpm = GPM()
# print(gpm.catalog.spatial_resolution)
# print(gpm.catalog.datasets)

# start_time=np.datetime64("2023-06-30T00:00:00")
# end_time=np.datetime64("2023-07-31T23:30:00")
# g = gpm.catalog.search(aoi=aoi, start_time=start_time, end_time=end_time)
# print(g)

# gds = gpm.reader.open_dataset(start_time=start_time, end_time=end_time, dataset='wis', bbox=(121,39,122,40))
# print(gds)
# print()
# gds = gpm.reader.from_shp(start_time=start_time, end_time=end_time, dataset='wis', shp='test.geojson')
# print(gds)
# print()
# gds = gpm.reader.from_aoi(start_time=start_time, end_time=end_time, dataset='wis', aoi=aoi)
# print(gds)
# print()

# print('----------------------------------------------------------------------------')

# gfs = GFS('tp')
# print(gfs.catalog.spatial_resolution)
# print(gfs.catalog.datasets)

# g = gfs.catalog.search(aoi=aoi)
# print(g)

# creation_date = np.datetime64("2022-06-30")
# creation_time = '00'
# gds = gfs.reader.open_dataset(creation_date=creation_date, creation_time=creation_time, dataset='wis', bbox=(121,39,122,40))
# print(gds)
# print()
# gds = gfs.reader.from_shp(creation_date=creation_date, creation_time=creation_time, dataset='wis', shp='test.geojson')
# print(gds)
# print()
# gds = gfs.reader.from_aoi(creation_date=creation_date, creation_time=creation_time, dataset='wis', aoi=aoi)
# print(gds)
# print()

# from hydro_opendata.stac.s3 import LandsatCatalog

# lsc = LandsatCatalog()
# print(lsc.root)
# lss = lsc.search(start_date='2023-09-01',bbox=[121,39,122,40])
# print(lss[0]['assets'])

# from hydro_opendata.stac.s3 import SentinelCatalog

# sc = SentinelCatalog()
# print(sc.token)
# sc.get_token(client_id='sh-410b53e7-a3b6-4abc-b830-f194959fa6a5',client_secret='BNH1RSB7kiC95lWRf0UVgIeOa5aEeBOg')
# print(sc.token)
# scs = sc.search(start_date='2022-09-01',bbox=[121,39,122,40])
# # print(scs[0]['assets'])
# print(len(scs))