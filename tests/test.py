import sys
sys.path.append('/home/zhujianfeng/wis/hydro-opendata/')

# print(sys.path)

print('----------------------------------------------------------------------------')
# from hydro_opendata.stac.minio import Era5_land, GPM_IMERG_Early, GFS_atmos
import geopandas as gpd

aoi = gpd.read_file('test.geojson')
print(aoi)
print()

# era5 = Era5_land()
# e = era5.search(aoi=aoi)
# print(e)
# print()

# gpm = GPM_IMERG_Early()
# g = gpm.search(aoi=aoi)
# print(g)
# print()

# gfs = GFS_atmos('tp')
# f = gfs.search(aoi=aoi)
# print(f)
# print()

# print('----------------------------------------------------------------------------')
from hydro_opendata.s3api.minio import ERA5LReader
import numpy as np

era5 = ERA5LReader()

# 指定开始及结束时间
start_time=np.datetime64("2021-06-01T00:00:00.000000000")
end_time=np.datetime64("2021-06-30T23:00:00.000000000")

# 通过指定四至范围读取
bbox=(121,39,123,40)
ds1 = era5.open_dataset(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, bbox=bbox)
print(ds1)
# print()

# gr = GPMReader('wis')
# ds2 = gfs.open_dataset(creation_date=np.datetime64("2022-08-01"))
# print(ds2)
# print()

# start_time=np.datetime64("2023-08-23T00:00:00.000000000")
# end_time=np.datetime64("2023-08-24T23:30:00.000000000")
# ds3 = gpm.from_aoi(start_time=start_time, end_time=end_time, aoi=aoi)
# print(ds3)

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

# from hydro_opendata.data.minio import Era5L, GPM, GFS

# era5 = Era5L('wis')
# print(era5.catalog.spatial_resolution)
# print(era5.catalog.start_time)
# print(era5.catalog.end_time)

# start_time=np.datetime64("2021-06-30T00:00:00")
# end_time=np.datetime64("2021-07-31T23:30:00")
# e = era5.catalog.search(aoi=aoi, start_time=start_time, end_time=end_time)
# print(e)


# eds = era5.reader.open_dataset(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, bbox=(121,39,122,40))
# print(eds)
# print()
# eds = era5.reader.from_shp(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, shp='test.geojson')
# print(eds)
# print()
# eds = era5.reader.from_aoi(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, aoi=aoi)
# print(eds)
# print()

# eds = era5.reader.to_netcdf(data_variables=['Total precipitation'], start_time=start_time, end_time=end_time, shp='test.geojson', resolution='6-hourly')
# print(eds)
# print()

# print('----------------------------------------------------------------------------')

# gpm = GPM('wis')
# print(gpm.catalog.spatial_resolution)
# print(gpm.catalog.start_time)
# print(gpm.catalog.end_time)

# start_time=np.datetime64("2023-06-30T00:00:00")
# end_time=np.datetime64("2023-07-31T23:30:00")
# g = gpm.catalog.search(aoi=aoi, start_time=start_time, end_time=end_time)
# print(g)

# gds = gpm.reader.open_dataset(start_time=start_time, end_time=end_time, bbox=(121,39,122,40))
# print(gds)
# print()
# gds = gpm.reader.from_shp(start_time=start_time, end_time=end_time, shp='test.geojson')
# print(gds)
# print()
# gds = gpm.reader.from_aoi(start_time=start_time, end_time=end_time, aoi=aoi)
# print(gds)
# print()

# print('----------------------------------------------------------------------------')

# gfs = GFS('wis', 'tp')
# print(gfs.catalog.spatial_resolution)
# print(gfs.catalog.start_time)
# print(gfs.catalog.end_time)

# g = gfs.catalog.search(aoi=aoi)
# print(g)

# creation_date = np.datetime64("2022-06-30")
# creation_time = '00'
# gds = gfs.reader.open_dataset(creation_date=creation_date, creation_time=creation_time, bbox=(121,39,122,40))
# print(gds)
# print()
# gds = gfs.reader.from_shp(creation_date=creation_date, creation_time=creation_time, shp='test.geojson')
# print(gds)
# print()
# gds = gfs.reader.from_aoi(creation_date=creation_date, creation_time=creation_time, aoi=aoi)
# print(gds)
# print()