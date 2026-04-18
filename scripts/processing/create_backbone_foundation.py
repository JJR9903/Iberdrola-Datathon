import geopandas as gpd
import pandas as pd
import fiona
import numpy as np
import os
import time

# Enable KML driver
try:
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
except Exception:
    pass

def discretize_backbone_roads(kmz_path, sampling_interval_m=200):
    """
    Converts LineStrings from a KMZ file into a series of Points along their path.
    Each point stores the distance from the line's start (m_ref).
    """
    print(f" - Discretizing backbones into points (Interval={sampling_interval_m}m)...")
    gdf_backbone = gpd.read_file(kmz_path, driver='KML')
    
    # Extract attributes from HTML description (preserving backbone metadata)
    gdf_backbone["route_name"] = gdf_backbone["description"].str.extract(
        r"<td>Carretera</td>\s*<td>([^<]+)</td>", expand=False
    )
    gdf_backbone["tipo_via"] = gdf_backbone["description"].str.extract(
        r"<td>Tipo_de_via</td>\s*<td>([^<]+)</td>", expand=False
    )
    
    # Preserve original ID as backbone_id
    gdf_backbone = gdf_backbone.rename(columns={'id': 'backbone_id'})
    
    # Ensure metric CRS or project to local UTM (assuming EPSG:3042 for Spain or as per project)
    # Using 3042 as default for this project unless specified
    if gdf_backbone.crs != "EPSG:3042":
        gdf_backbone = gdf_backbone.to_crs(epsg=3042)
    
    points_data = []
    for _, row in gdf_backbone.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        
        length = geom.length
        if length <= 0:
            distances = [0.0]
        else:
            distances = np.arange(0, length, sampling_interval_m)
            if len(distances) == 0 or distances[-1] < length:
                distances = np.append(distances, length)
            
        for d in distances:
            pt = geom.interpolate(d)
            entry = row.to_dict()
            entry['geometry'] = pt
            entry['m_ref'] = round(d, 2)
            points_data.append(entry)
            
    gdf_pts = gpd.GeoDataFrame(points_data, crs=gdf_backbone.crs)
    
    # Create unique point IDs
    gdf_pts['point_idx'] = gdf_pts.groupby('backbone_id').cumcount()
    gdf_pts['point_id'] = (
        gdf_pts['backbone_id'].astype(str) + 
        "_" + 
        gdf_pts['point_idx'].astype(str)
    )
    
    return gdf_pts

def map_traffic_to_points(gdf_points, shp_path, traffic_parquet_path, traffic_columns=["total_max"], buffer_radius_m=50):
    """
    Maps traffic intensity metrics (e.g. total and short) from road segments to backbone points.
    """
    print(f" - Mapping traffic columns {traffic_columns} (Buffer={buffer_radius_m}m)...")
    
    # Load segment geometries and traffic info
    gdf_segments = gpd.read_file(shp_path)
    if gdf_segments.crs is None:
        gdf_segments.set_crs(3042, inplace=True)
    elif gdf_segments.crs != gdf_points.crs:
        gdf_segments = gdf_segments.to_crs(gdf_points.crs)
        
    df_traffic = pd.read_parquet(traffic_parquet_path)
    
    # Validate requested columns
    available_cols = [c for c in traffic_columns if c in df_traffic.columns]
    if not available_cols:
        print(f"   Warning: None of the requested columns {traffic_columns} were found. Skipping traffic mapping.")
        return gdf_points
        
    print(f"   Mapping columns: {available_cols}")

    # Pre-clean IDs for join
    gdf_segments['id_tramo'] = gdf_segments['id_tramo'].astype(str)
    df_traffic['traffic_segment_id'] = df_traffic['traffic_segment_id'].astype(str)
    
    # Join traffic info to segment geometries
    # Drop geometry from traffic to avoid duplicates during merge
    gdf_merged = gdf_segments.merge(df_traffic.drop(columns=['geometry'], errors='ignore'), left_on='id_tramo', right_on='traffic_segment_id')
    
    # 1. Spatial Join with Buffer
    gdf_pts_buffered = gdf_points.copy()
    gdf_pts_buffered['geometry'] = gdf_pts_buffered.geometry.buffer(buffer_radius_m)
    
    joined = gpd.sjoin(
        gdf_pts_buffered[['point_id', 'backbone_id', 'point_idx', 'geometry']], 
        gdf_merged[['id_tramo', 'geometry'] + available_cols], 
        how='inner', 
        predicate='intersects'
    )
    
    if joined.empty:
        print("   Warning: No segments matched the backbone points.")
        for col in available_cols:
            gdf_points[col] = 0.0
        return gdf_points

    # 2. Neighbor Validation Filter
    joined['has_neighbor'] = joined.groupby(['id_tramo', 'backbone_id'])['point_idx'].transform(
        lambda x: x.isin(x + 1) | x.isin(x - 1)
    )
    joined_filtered = joined[joined['has_neighbor']].copy()
    
    if joined_filtered.empty:
        print("   Warning: No segments passed the neighbor-validation filter.")
        for col in available_cols:
            gdf_points[col] = 0.0
        return gdf_points

    # 3. Sum Traffic per Point
    traffic_summary = joined_filtered.groupby('point_id')[available_cols].sum().reset_index()
    
    # Merge back to original points
    gdf_final = gdf_points.merge(traffic_summary, on='point_id', how='left')
    gdf_final[available_cols] = gdf_final[available_cols].fillna(0)
    
    # 4. Gap Filling (Interpolation for single-point gaps)
    gdf_final = gdf_final.sort_values(['backbone_id', 'point_idx'])
    for col in available_cols:
        prev_val = gdf_final.groupby('backbone_id')[col].shift(1)
        next_val = gdf_final.groupby('backbone_id')[col].shift(-1)
        
        mask = (gdf_final[col] == 0) & (prev_val > 0) & (next_val > 0)
        gdf_final.loc[mask, col] = (prev_val + next_val) / 2
    
    return gdf_final

def assign_nearest_charging_stations(gdf_points, chargers_parquet_path, max_distance=None):
    """
    Assigns the nearest ultra-fast charging station site_id and distance.
    """
    print(f" - Assigning nearest charging stations (MaxDist={max_distance})...")
    gdf_chargers = gpd.read_parquet(chargers_parquet_path)
    
    if gdf_chargers.crs != gdf_points.crs:
        gdf_chargers = gdf_chargers.to_crs(gdf_points.crs)
        
    # We now have site_id!
    cols_to_keep = ['site_id', 'geometry']
    gdf_chargers_subset = gdf_chargers[cols_to_keep].rename(columns={
        'site_id': 'nearest_charger_id'
    })
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_chargers_subset,
        how="left",
        max_distance=max_distance,
        distance_col="dist_charger_m"
    )
    
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])
        
    return gdf_result

def assign_nearest_gas_stations(gdf_points, gas_stations_parquet_path, max_distance=None):
    """
    Assigns the nearest gas station station_id and distance.
    """
    print(f" - Assigning nearest gas stations (MaxDist={max_distance})...")
    gdf_gas = gpd.read_parquet(gas_stations_parquet_path)
    
    if gdf_gas.crs != gdf_points.crs:
        gdf_gas = gdf_gas.to_crs(gdf_points.crs)
        
    gdf_gas_subset = gdf_gas[['station_id', 'geometry']].rename(columns={
        'station_id': 'nearest_gas_station_id'
    })
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_gas_subset,
        how="left",
        max_distance=max_distance,
        distance_col="dist_gas_station_m"
    )
    
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])
        
    return gdf_result

def main(
    kmz_path, 
    traffic_shp_path, 
    traffic_parquet_path, 
    chargers_path, 
    gas_stations_path, 
    output_path,
    sub_steps=["all"],
    traffic_columns=["total_max"],
    sampling_interval_m=200,
    buffer_radius_m=50,
    max_distance_proximity=None
):
    """
    Orchestrates the creation of the backbone foundation points.
    """
    start_time = time.time()
    print("🚀 Starting Backbone Foundation Creation...")
    
    run_all = "all" in sub_steps
    
    if run_all or "discretize" in sub_steps:
        gdf_points = discretize_backbone_roads(kmz_path, sampling_interval_m)
    else:
        print(f" - Skipping discretization. Loading existing points from {output_path}...")
        gdf_points = gpd.read_parquet(output_path)

    if run_all or "traffic" in sub_steps:
        gdf_points = map_traffic_to_points(gdf_points, traffic_shp_path, traffic_parquet_path, traffic_columns, buffer_radius_m)

    if run_all or "chargers" in sub_steps:
        gdf_points = assign_nearest_charging_stations(gdf_points, chargers_path, max_distance_proximity)

    if run_all or "gas_stations" in sub_steps:
        gdf_points = assign_nearest_gas_stations(gdf_points, gas_stations_path, max_distance_proximity)

    print(f" - Saving final foundation dataset to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_points.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Created foundation with {len(gdf_points)} points.")
    print(f"   Time elapsed: {time.time() - start_time:.1f}s")
    return gdf_points

if __name__ == "__main__":
    main(
        kmz_path="data/raw/roads/roads.kmz",
        traffic_shp_path="data/raw/traffic/geometria/Geometria_tramos.shp",
        traffic_parquet_path="data/standardized/traffic.parquet",
        chargers_path="data/standardized/chargers.parquet",
        gas_stations_path="data/standardized/gas_stations.parquet",
        output_path="data/processed/backbone_foundation.parquet"
    )
