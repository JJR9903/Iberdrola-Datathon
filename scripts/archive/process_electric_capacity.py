import pandas as pd
import polars as pl
import geopandas as gpd
from shapely.geometry import Point
import os

def clean_coordinate(val):
    if isinstance(val, str):
        try:
            return float(val.replace(',', '.'))
        except ValueError:
            return 0.0
    return float(val) if val is not None else 0.0

def load_and_clean_data(file_path, company_name):
    print(f"Reading {company_name} from {file_path}...")
    
    # Define common columns
    common_cols = [
        'Gestor de red', 
        'Provincia', 
        'Municipio', 
        'Coordenada UTM X', 
        'Coordenada UTM Y', 
        'Subestación'
    ]
    
    # Handle Iberdrola's different capacity column name
    capacity_col = 'Capacidad disponible (MW)'
    if company_name == 'Iberdrola':
        capacity_col = 'Capacidad firme disponible (MW)'
    
    # Select columns to read
    usecols = common_cols + [capacity_col]
    
    try:
        # We use pandas to read excel as polars excel reader requires additional dependencies
        print(f"  Loading columns: {usecols}")
        df_pd = pd.read_excel(file_path, usecols=usecols)
        # Convert to Polars for cleaning
        df = pl.from_pandas(df_pd)
        print(f"  Successfully loaded {len(df)} rows.")
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
        # Fallback to loading everything
        print("  Attempting to load without usecols fallback...")
        df_pd = pd.read_excel(file_path)
        df = pl.from_pandas(df_pd)
        df = df.select([pl.col(c) for c in df.columns if c in usecols])
        print(f"  Fallback loaded {len(df)} rows.")
    
    # Rename Iberdrola's capacity column to be consistent
    if company_name == 'Iberdrola' and capacity_col in df.columns:
        df = df.rename({capacity_col: 'Capacidad disponible (MW)'})
    
    # Clean coordinates and capacity
    print("  Cleaning coordinates and data types...")
    
    # Helper for capacity cleaning in Polars
    def clean_capacity_val(val):
        if val is None: return 0.0
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            cleaned = "".join(c for c in val if c.isdigit() or c in ',.')
            if not cleaned: return 0.0
            return float(cleaned.replace(',', '.'))
        return 0.0

    df = df.with_columns([
        pl.col('Coordenada UTM X').map_elements(clean_coordinate, return_dtype=pl.Float64),
        pl.col('Coordenada UTM Y').map_elements(clean_coordinate, return_dtype=pl.Float64),
        pl.col('Capacidad disponible (MW)').map_elements(clean_capacity_val, return_dtype=pl.Float64),
        pl.lit(company_name).alias('company')
    ])
    
    # Ensure all required strings are strings
    str_cols = ['Gestor de red', 'Provincia', 'Municipio', 'Subestación']
    df = df.with_columns([
        pl.col(c).cast(pl.Utf8) for c in str_cols if c in df.columns
    ])
    
    return df

def main(
    raw_dir='data/raw/electric_capacity',
    output_path='data/processed/electric_capacity_merged.parquet',
    files=None
):
    if files is None:
        files = {
            'Endesa': 'Endesa_2026_04_01.xlsx',
            'Iberdrola': 'Iberdrola_2026_04_01.xlsx',
            'Viesgo': 'Viesgo_2026_04_01.xlsx'
        }
    
    dfs = []
    for company, filename in files.items():
        file_path = os.path.join(raw_dir, filename)
        if os.path.exists(file_path):
            dfs.append(load_and_clean_data(file_path, company))
        else:
            print(f"Warning: {file_path} not found.")
    
    if not dfs:
        print("No data found.")
        return
    
    # Concatenate all dataframes in Polars
    print("Concatenating dataframes...")
    merged_df = pl.concat(dfs)
    print(f"Merged dataframe has {len(merged_df)} rows.")
    
    # Spatial Processing: Bridge to GeoPandas for coordinate transformation
    print("Converting to GeoPandas for spatial transformation (EPSG:25830 -> EPSG:4326)...")
    pdf = merged_df.to_pandas()
    gdf = gpd.GeoDataFrame(
        pdf, 
        geometry=gpd.points_from_xy(pdf['Coordenada UTM X'], pdf['Coordenada UTM Y']),
        crs="EPSG:25830"
    )
    
    # Transform to WGS84
    gdf_wgs84 = gdf.to_crs("EPSG:4326")
    pdf['longitude'] = gdf_wgs84.geometry.x
    pdf['latitude'] = gdf_wgs84.geometry.y
    
    # Convert back to Polars for final formatting and saving
    final_df = pl.from_pandas(pdf)
    
    print(f"Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.write_parquet(output_path)
    print(f"Successfully saved merged data to {output_path}")
    print(f"Total rows: {len(final_df)}")

if __name__ == "__main__":
    main()
