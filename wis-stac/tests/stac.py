import planetary_computer as pc
import pystac_client
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

# stac = "https://planetarycomputer.microsoft.com/api/stac/v1"
stac = "https://earth-search.aws.element84.com/v0"
landsat_collection = "landsat-8-l1-c1"  # Name of the STAC collection
sentinel_collection = "sentinel-s2-l2a-cogs"  # Name of the STAC collection

# Open the Catalogue
CATALOG = pystac_client.Client.open(stac)

def search(aoi, start_date, end_date,collections=[landsat_collection, sentinel_collection]):
    
    geom = aoi.geometry[0]
    x,y = geom.exterior.xy
    # 构造数据范围的dictionary，方便后面在stac中search
    bbox_latlon = {
        "type":f"{geom.geom_type}",
        "coordinates":[[[i,j] for i,j in zip(x,y) ]]
    }

    # Do a search
    SEARCH = CATALOG.search(
        intersects=bbox_latlon,
        datetime=f"{start_date}/{end_date}",
        collections=collections,
        # **kwargs,
    )

    # Get all items and sign if using Planetary Computer
    items = SEARCH.get_all_items()
    if stac == "https://planetarycomputer.microsoft.com/api/stac/v1":
        items = pc.sign(items)
        
    return items

def catalog(items):
        
    catalog = []
    for item in items:
        catalog.append(item.collection_id)
    catalog = list(set(catalog))
    return catalog


def extent(items, collection_id=['landsat-8-l1-c1']):
    ids = []
    properties = []
    geometries = []
    for item in items:
        if item.collection_id in collection_id:
            ids.append(item.id)
            properties.append(item.properties)
            geometries.append(shape(item.geometry).wkt)
            
            
    dfs = pd.DataFrame(
        {
            'id': ids,
            'properties': properties,
            'geometry': geometries
        }
    )
    
    dfs['geometry'] = gpd.GeoSeries.from_wkt(dfs['geometry'])
    gdfs = gpd.GeoDataFrame(dfs, geometry='geometry',crs='EPSG:4326')
    
    return gdfs