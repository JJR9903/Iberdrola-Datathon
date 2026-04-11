import polars as pl
import geopandas as gpd
from shapely import wkb
import os
import time

def main(
    charging_points_path="data/processed/charging_points.parquet",
    road_network_path="data/processed/integrated_road_network.parquet",
    output_path="data/processed/charging_stations.parquet",
    max_distance=1000
):
    """
    Filters high-power EV charging sites, groups them by site_id, and calculates
    the distance to the nearest road backbone.
    
    Args:
        charging_points_path: Path to the input charging points parquet.
        road_network_path: Path to the input integrated road network parquet.
        output_path: Path to save the final results.
        max_distance: Maximum distance (in meters) to allow for a site to be kept.
    """
    start_time = time.time()
    
    print(f"Loading charging points from {charging_points_path}...")
    # 1. Load and filter charging points
    df_full = pl.read_parquet(charging_points_path)
    
    # Filter for max_power >= 100000
    df_full = df_full.filter(pl.col('max_power') >= 100000)
    
    # 2. Group by site_id with requested aggregations
    df_sites = df_full.group_by("site_id").agg([
        pl.col("site_name").first(),
        pl.col("latitude").first(),
        pl.col("longitude").first(),
        pl.col("connector_type").unique(),
        pl.col("connector_type").count().alias('n_chargers'),
        pl.col("max_power").unique(),
        pl.col("charging_mode").unique()
    ]).filter(
        (pl.col("latitude").is_not_null()) & (pl.col("longitude").is_not_null())
    )
    
    print(f"Filtered to {df_sites.height} high-power sites. Converting to GeoPandas...")
    
    # 3. Convert to GeoPandas
    df_sites_pd = df_sites.to_pandas()
    gdf_sites = gpd.GeoDataFrame(
        df_sites_pd,
        geometry=gpd.points_from_xy(df_sites_pd.longitude, df_sites_pd.latitude),
        crs="EPSG:4326"
    )
    
    # Project to metric CRS for distance calculation
    gdf_sites = gdf_sites.to_crs(epsg=3042)
    
    print(f"Loading road network from {road_network_path}...")
    # 4. Load road network
    df_roads = pl.read_parquet(road_network_path)
    
    # Convert binary WKB to shapely geometries
    df_roads_pd = df_roads.to_pandas()
    df_roads_pd['geometry'] = df_roads_pd['geometry'].apply(wkb.loads)
    
    gdf_roads = gpd.GeoDataFrame(df_roads_pd, geometry='geometry', crs="EPSG:3042")
    
    print("Calculating distance to nearest road...")
    # 5. Calculate closest distance
    gdf_sites = gpd.sjoin_nearest(
        gdf_sites,
        gdf_roads[['backbone_id', 'geometry']],
        how="left",
        distance_col="distance_to_road_m"
    )
    
    # sjoin_nearest might create duplicates if multiple roads are at the same minimum distance
    gdf_sites = gdf_sites.drop_duplicates(subset=['site_id'])
    
    # Remove the index_right column added by sjoin
    if 'index_right' in gdf_sites.columns:
        gdf_sites = gdf_sites.drop(columns=['index_right'])
    
    # 6. Filter by max_distance
    print(f"Filtering sites further than {max_distance}m from road network...")
    initial_count = len(gdf_sites)
    gdf_sites = gdf_sites[gdf_sites['distance_to_road_m'] <= max_distance].copy()
    final_count = len(gdf_sites)
    
    print(f"Kept {final_count} out of {initial_count} sites based on distance filter.")
    
    # 7. Save results
    print(f"Saving results to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_sites.to_parquet(output_path)
    
    end_time = time.time()
    print(f"Success! Final dataset has {len(gdf_sites)} sites. Total time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    main()
