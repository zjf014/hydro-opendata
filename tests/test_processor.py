"""
Author: Wenyu Ouyang
Date: 2023-10-28 08:28:37
LastEditTime: 2023-10-31 09:45:31
LastEditors: Wenyu Ouyang
Description: Test funcs for processor
FilePath: \hydro_opendata\tests\test_processor.py
Copyright (c) 2023-2024 Wenyu Ouyang. All rights reserved.
"""
import os
import geopandas as gpd
from hydro_opendata.processor.minio import GeoProcessor, geojson_to_shp


def test_geojson_to_shp(tmp_path):
    # create a temporary GeoJSON file
    input_geojson = os.path.join(tmp_path, "test.geojson")
    points = gpd.points_from_xy([0, 1], [0, 1])
    gdf = gpd.GeoDataFrame({"col1": [1, 2], "geometry": points})
    gdf.to_file(input_geojson, driver="GeoJSON")

    # call the function with the temporary GeoJSON file
    output_folder = os.path.join(tmp_path, "shp_output")
    geojson_to_shp(input_geojson, output_folder=output_folder)

    # check that the Shapefile was created
    assert os.path.exists(os.path.join(output_folder, "test.shp"))

    # check that the ZIP file was created
    assert os.path.exists(os.path.join(tmp_path, "test.zip"))


def test_upload_and_read_shp(tmp_path, minio_paras):
    # create a temporary GeoJSON file
    input_geojson = os.path.join(tmp_path, "test.geojson")
    points = gpd.points_from_xy([0, 1], [0, 1])
    gdf = gpd.GeoDataFrame({"col1": [1, 2], "geometry": points})
    gdf.to_file(input_geojson, driver="GeoJSON")
    geo_processor = GeoProcessor(minio_paras)
    geo_processor.upload_geojson(gj_local_path=input_geojson, gj_mo_name="test.geojson")
    gdf_rd = geo_processor.read_shp("test.zip")
    assert gdf_rd.equals(gdf)
