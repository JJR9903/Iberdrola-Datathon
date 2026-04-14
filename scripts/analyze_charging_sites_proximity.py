import polars as pl
import geopandas as gpd
import os
import time

def main(
    charging_points_path="data/processed/charging_points.parquet",
    road_network_path="data/processed/integrated_road_network.parquet",
    backbone_roads_path="data/processed/backbone_roads.parquet",
    output_path="data/processed/charging_stations.parquet",
    max_distance=1000
):
    """
    Filters high-power EV charging sites, groups them by site_id, and calculates
    proximity to road segments and backbone roads.
    
    Args:
        charging_points_path: Path to the input charging points parquet.
        road_network_path: Path to the input integrated road network parquet (segments).
        backbone_roads_path: Path to the input backbone roads parquet.
        output_path: Path to save the final results.
        max_distance: Maximum distance (in meters) to a segment to allow for a site to be kept.
    """
    start_time = time.time()
    
    # 1. Load and aggregate charging points
    print(f"Loading charging points from {charging_points_path}...")
    df_full = pl.read_parquet(charging_points_path)
    
    # Filter for max_power >= 100000
    df_full = df_full.filter(pl.col('max_power') >= 100000)
    
    # Group by site_id with requested aggregations
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
    
    # Convert to GeoPandas
    df_sites_pd = df_sites.to_pandas()
    gdf_sites = gpd.GeoDataFrame(
        df_sites_pd,
        geometry=gpd.points_from_xy(df_sites_pd.longitude, df_sites_pd.latitude),
        crs="EPSG:4326"
    )
    
    # Project to metric CRS (standard for the project is 3042)
    gdf_sites = gdf_sites.to_crs(epsg=3042)
    
    # 2. Distance to Segments & Initial Filter
    print(f"Loading road segments from {road_network_path}...")
    # Using gpd.read_parquet as requested by the user
    gdf_roads = gpd.read_parquet(road_network_path)
    if gdf_roads.crs is None:
        gdf_roads.set_crs(3042, inplace=True)
    
    print("Calculating distance to nearest road segment...")
    # sjoin_nearest to get nearest segment and distance
    gdf_sites = gpd.sjoin_nearest(
        gdf_sites,
        gdf_roads[['segment_id', 'geometry']],
        how="left",
        distance_col="distance_to_segment_m"
    )
    
    # Drop duplicates (if a point is equidistant to multiple segments) and remove index_right
    gdf_sites = gdf_sites.drop_duplicates(subset=['site_id'])
    if 'index_right' in gdf_sites.columns:
        gdf_sites = gdf_sites.drop(columns=['index_right'])
    
    # Filter by max_distance to segments
    print(f"Filtering sites further than {max_distance}m from road segments...")
    initial_count = len(gdf_sites)
    gdf_sites = gdf_sites[gdf_sites['distance_to_segment_m'] <= max_distance].copy()
    final_count = len(gdf_sites)
    print(f"Kept {final_count} out of {initial_count} sites based on segment distance.")
    
    # 3. Link to Backbone Roads
    print(f"Loading backbone roads from {backbone_roads_path}...")
    gdf_backbones = gpd.read_parquet(backbone_roads_path)
    if gdf_backbones.crs is None:
        gdf_backbones.set_crs(3042, inplace=True)
        
    print("Linking stations to nearest backbone road...")
    # sjoin_nearest to get nearest backbone and distance
    gdf_sites = gpd.sjoin_nearest(
        gdf_sites,
        gdf_backbones[['backbone_id', 'geometry']],
        how="left",
        distance_col="distance_to_backbone_m"
    )
    
    # Drop duplicates and index_right
    gdf_sites = gdf_sites.drop_duplicates(subset=['site_id'])
    if 'index_right' in gdf_sites.columns:
        gdf_sites = gdf_sites.drop(columns=['index_right'])
        
    # 4. Save results
    print(f"Saving results to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_sites.to_parquet(output_path)
    
    end_time = time.time()
    print(f"Success! Final dataset has {len(gdf_sites)} sites. Total time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    main()
