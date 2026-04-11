import geopandas as gpd
import pandas as pd
import fiona
import networkx as nx
from shapely.geometry import MultiLineString, LineString
from shapely.ops import unary_union, linemerge
import os
import time

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'

def main(
    shp_path="./data/raw/road_routes/geometria/Geometria_tramos.shp",
    traffic_path="./data/processed/road_routes_traffic.parquet",
    kmz_path="./data/raw/roads/query.kmz",
    output_path="./data/processed/integrated_road_network.parquet",
    small_segment_length_m=2000,
    bridge_gap_threshold_m=1000,
    parallel_threshold_m=100
):
    start_time = time.time()
    print("Loading data...")
    # Load Shapefile
    gdf_segments = gpd.read_file(shp_path)
    if gdf_segments.crs is None:
        gdf_segments.set_crs(3042, inplace=True)
    
    print(f"Loaded {len(gdf_segments)} segments. Loading traffic data...")
    # Load Consolidated Traffic Data
    df_info = pd.read_parquet(traffic_path)
    
    # Identify all traffic columns to keep
    traffic_cols = [c for c in df_info.columns if c.startswith('total_')]
    if not traffic_cols:
        print("Warning: No 'total_YYYYMMDD' columns found.")
    
    gdf_segments['id_tramo'] = gdf_segments['id_tramo'].astype(str)
    df_info['tramo'] = df_info['tramo'].astype(str)
    
    print("Loading KMZ backbones...")
    # Load KMZ as backbone reference
    gdf_backbone = gpd.read_file(kmz_path, driver='KML')
    gdf_backbone = gdf_backbone.to_crs(gdf_segments.crs)
    gdf_backbone['backbone_id'] = gdf_backbone.index
    gdf_backbone = gdf_backbone[['backbone_id','geometry']]
    
    # Filter out small backbone reference roads
    gdf_backbone['backbone_length_m'] = gdf_backbone.geometry.length
    gdf_backbone = gdf_backbone[gdf_backbone['backbone_length_m'] >= small_segment_length_m].copy()
    print(f"Filtered KMZ: {len(gdf_backbone)} backbones remain (min length {small_segment_length_m}m).")
    
    print("Merging shapefile and traffic data...")
    # Using 'total' as a helper for some logic if needed, but we keep the multiples
    gdf_merged = gdf_segments.merge(df_info, left_on='id_tramo', right_on='tramo')
    # Create a helper 'total' column (average) just for the sindex query/sorting if needed, 
    # but the aggregation will use the raw columns.
    if traffic_cols:
        gdf_merged['total'] = gdf_merged[traffic_cols].mean(axis=1)
    else:
        gdf_merged['total'] = 0
    
    gdf_merged['length_m'] = gdf_merged.geometry.length
    
    print("Assigning segments to nearest backbone road using centroids (Optimized)...")
    # PERFORMANCE OPTIMIZATION: Use centroids for assignment
    gdf_centroids = gdf_merged.copy()
    gdf_centroids['geometry'] = gdf_centroids.geometry.centroid
    
    # We use a max_distance to keep it fast
    gdf_assigned_centroids = gpd.sjoin_nearest(
        gdf_centroids, 
        gdf_backbone[['backbone_id', 'geometry']], 
        max_distance=500, # 2km max distance to road backbone
        distance_col="dist_to_backbone"
    )
    
    # Map back to original line geometries using explicit ID merge
    gdf_merged = gdf_merged.merge(
        gdf_assigned_centroids[['id_tramo', 'backbone_id', 'dist_to_backbone']], 
        on='id_tramo', 
        how='left'
    )
    # Filter segments that couldn't be matched within 2km (minor roads)
    gdf_merged = gdf_merged.dropna(subset=['backbone_id'])
    
    print(f"Beginning simplification for {len(gdf_merged)} matched segments...")
    
    processed_groups = []
    backbone_groups = list(gdf_merged.groupby('backbone_id'))
    total_groups = len(backbone_groups)
    
    for i, (bib_id, group) in enumerate(backbone_groups):
        if i % 200 == 0:
            print(f"Processing backbone {i}/{total_groups}... (Elapsed: {time.time()-start_time:.1f}s)")
            
        group = group.copy().reset_index(drop=True)
        
        # We find clusters of segments that should be merged
        # Faster cluster approach: 1km buffer for nearby joins
        # For parallel joins, we use a 100m rule.
        
        # Create adjacency matrix based on thresholds
        G = nx.Graph()
        G.add_nodes_from(group.index)
        
        # Spatial Index query
        sindex = group.sindex
        
        # We only need to check segments against their neighbors
        for idx in group.index:
            geom = group.at[idx, 'geometry']
            length = group.at[idx, 'length_m']
            
            # Max possible search radius
            search_rad = max(bridge_gap_threshold_m, parallel_threshold_m)
            possible_indices = sindex.query(geom.buffer(search_rad))
            
            for other_idx in possible_indices:
                if idx >= other_idx:
                    continue
                
                other_geom = group.at[other_idx, 'geometry']
                other_length = group.at[other_idx, 'length_m']
                dist = geom.distance(other_geom)
                
                if dist < parallel_threshold_m:
                    G.add_edge(idx, other_idx)
                elif (length < small_segment_length_m or other_length < small_segment_length_m) and dist < bridge_gap_threshold_m:
                    G.add_edge(idx, other_idx)
        
        for component in nx.connected_components(G):
            subset = group.iloc[list(component)]
            merged_geom = unary_union(subset.geometry)
            
            # linemerge joins LineStrings together if they are end-to-end
            if not merged_geom.is_empty:
                if isinstance(merged_geom, (MultiLineString, list)):
                    try:
                        merged_geom = linemerge(merged_geom)
                    except Exception:
                        pass  # Keep as MultiLineString if merge fails
            
            # Sum all individual total_ columns
            traffic_sums = subset[traffic_cols].sum().to_dict()
            
            processed_entry = {
                'backbone_id': bib_id,
                'geometry': merged_geom,
                'original_segment_count': len(subset)
            }
            # Merge the traffic sums into the entry
            processed_entry.update(traffic_sums)
            processed_groups.append(processed_entry)

    print("Consolidating results...")
    gdf_final = gpd.GeoDataFrame(processed_groups, crs=gdf_segments.crs)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_final.to_parquet(output_path)
    print(f"Process Complete. Output: {len(gdf_final)} super-segments. Total Time: {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    main()
