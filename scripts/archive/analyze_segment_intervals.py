import geopandas as gpd
import pandas as pd
import numpy as np
import os
import time
from shapely import wkb

def main(
    backbone_path="data/processed/backbone_roads.parquet",
    charging_stations_path="data/processed/charging_stations.parquet",
    integrated_road_path="data/processed/integrated_road_network.parquet",
    output_path="data/processed/road_segment_charging_metrics.parquet"
):
    start_time = time.time()
    print("🚀 Starting Enhanced Road Segment Interval Analysis (with Detours)...")
    
    # 1. Load Datasets
    print("Loading datasets...")
    gdf_backbones = gpd.read_parquet(backbone_path)
    gdf_segments = pl_read_geoparquet(integrated_road_path)
    gdf_stations = gpd.read_parquet(charging_stations_path)
    
    # Ensure CRS consistency (EPSG:3042)
    target_crs = "EPSG:3042"
    for gdf in [gdf_backbones, gdf_segments, gdf_stations]:
        if gdf.crs is None:
            gdf.set_crs(target_crs, inplace=True)
        elif gdf.crs != target_crs:
            gdf.to_crs(target_crs, inplace=True)
            
    print(f"Loaded {len(gdf_backbones)} backbones, {len(gdf_segments)} segments, and {len(gdf_stations)} stations.")
    
    results = []
    
    # 2. Processing Loop by Backbone
    backbone_groups = gdf_segments.groupby('backbone_id')
    total_backbones = len(backbone_groups)
    
    # Pre-calculate nearest stations for sparse backbones to avoid repeated sjoin_nearest in loop
    print("Identifying sparse backbones for global fallback...")
    backbone_counts = gdf_stations.groupby('backbone_id').size()
    sparse_backbone_ids = [bid for bid in gdf_segments['backbone_id'].unique() if backbone_counts.get(bid, 0) < 2]
    
    sparse_fallbacks = {}
    if sparse_backbone_ids:
        print(f"Finding fallbacks for {len(sparse_backbone_ids)} sparse corridors...")
        # For each sparse backbone, find the 2 absolute nearest stations
        gdf_sparse = gdf_backbones[gdf_backbones['backbone_id'].isin(sparse_backbone_ids)]
        # sjoin_nearest handles finding the absolute closest
        fallback_matches = gpd.sjoin_nearest(
            gdf_sparse,
            gdf_stations[['site_id', 'geometry', 'n_chargers']],
            how="left",
            max_distance=100000, # 100km max fallback search
            distance_col="dist_to_backbone"
        )
        # Note: sjoin_nearest doesn't support 'k' easily in a single call for unique sets.
        # We'll just do it manually for each sparse backbone if needed or use a more robust way.
        # For now, let's just use the loop logic for simplicity unless it's too slow.
        pass

    print("Analyzing charging gaps...")
    for i, (b_id, segments) in enumerate(backbone_groups):
        if i % 100 == 0:
            print(f"   Processing corridor {i}/{total_backbones}...")
            
        backbone_row = gdf_backbones[gdf_backbones['backbone_id'] == b_id]
        if backbone_row.empty: continue
        backbone_geom = backbone_row.geometry.iloc[0]
        
        # Get stations serving this backbone
        local_stations = gdf_stations[gdf_stations['backbone_id'] == b_id].copy()
        
        # Sparse Fallback Logic
        if len(local_stations) < 2:
            # Find the closest stations manually
            dists = gdf_stations.distance(backbone_geom)
            # Take the top 2 (ensuring they aren't duplicates by site_id)
            fallback_indices = dists.nsmallest(5).index # Take top 5 to handle duplicates
            fallbacks = gdf_stations.loc[fallback_indices].drop_duplicates(subset=['site_id']).head(2).copy()
            # Calculate distance to road for these fallbacks
            fallbacks['distance_to_backbone_m'] = fallbacks.distance(backbone_geom)
            # Use them
            local_stations = fallbacks
            
        # Linear Referencing for stations
        local_stations['mile_marker'] = local_stations.geometry.apply(lambda p: backbone_geom.project(p))
        local_stations = local_stations.sort_values('mile_marker')
        
        # Segment analysis
        for _, seg_row in segments.iterrows():
            s_min = seg_row['master_start_m']
            s_max = seg_row['master_end_m']
            
            # Identify {Inside, Behind, Ahead}
            inside = local_stations[(local_stations['mile_marker'] >= s_min) & (local_stations['mile_marker'] <= s_max)]
            behind = local_stations[local_stations['mile_marker'] < s_min].tail(1)
            ahead = local_stations[local_stations['mile_marker'] > s_max].head(1)
            
            local_set = pd.concat([inside, behind, ahead]).drop_duplicates(subset=['site_id']).sort_values('mile_marker')
            
            results.append(calculate_segment_metrics(seg_row, b_id, local_set, backbone_geom, inside))

    # 3. Consolidate Results
    print("Saving results...")
    df_results = pd.DataFrame(results)
    df_results.to_parquet(output_path)
    
    print(f"✨ Analysis complete! Output: {len(df_results)} rows. Time: {time.time()-start_time:.1f}s")

def pl_read_geoparquet(path):
    df = pd.read_parquet(path)
    if 'geometry' in df.columns and not isinstance(df['geometry'].iloc[0], str):
        df['geometry'] = df['geometry'].apply(wkb.loads)
    return gpd.GeoDataFrame(df, geometry='geometry')

def calculate_segment_metrics(seg_row, b_id, local_stations, backbone_line, inside_stations):
    entry = {
        'segment_id': seg_row['segment_id'],
        'backbone_id': b_id,
        'num_stations': len(local_stations),
        'num_chargers': local_stations['n_chargers'].sum() if not local_stations.empty else 0,
        'has_charger_inside': not inside_stations.empty,
        'avg_gap_m': np.nan,
        'max_gap_m': np.nan
    }
    
    if len(local_stations) >= 2:
        # Detour Formula: Gap = |Pos_B - Pos_A| + Detour_A + Detour_B
        positions = local_stations['mile_marker'].values
        detours = local_stations['distance_to_backbone_m'].values
        
        gaps = []
        for j in range(len(local_stations) - 1):
            road_dist = abs(positions[j+1] - positions[j])
            total_gap = road_dist + detours[j] + detours[j+1]
            if total_gap > 1.0: # Filter out near-zero gaps
                gaps.append(total_gap)
        
        if gaps:
            entry['avg_gap_m'] = np.mean(gaps)
            entry['max_gap_m'] = np.max(gaps)
            
    return entry

if __name__ == "__main__":
    main()
