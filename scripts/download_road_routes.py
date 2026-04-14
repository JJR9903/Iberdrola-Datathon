import os
import requests
import gzip
import shutil

"""
DOWNLOAD SCRIPT FOR ROAD ROUTES DATASETS (2024)
...
"""

def download_file(url, output_path, label, decompress=False):
    """Downloads a file from a URL to a local path with User-Agent headers."""
    print(f"[{label}] Downloading from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # If decompressing, download to a temporary .gz file first
    download_path = output_path + ".gz" if decompress else output_path
    
    try:
        with requests.get(url, stream=True, timeout=300, headers=headers) as r:
            r.raise_for_status()
            with open(download_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        if decompress:
            print(f"[{label}] Decompressing {download_path}...")
            with gzip.open(download_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(download_path)
            
        print(f"[{label}] Successfully saved to {output_path}")
    except Exception as e:
        print(f"[{label}] Error downloading or decompressing file: {e}")
        # Cleanup if download_path exists but failed
        if decompress and os.path.exists(download_path):
            os.remove(download_path)
        raise

def main(base_dir="data/raw/road_routes", geom_url_base=None, info_url_base=None, info_files=None):
    if geom_url_base is None:
        geom_url_base = "https://movilidad-opendata.mitma.es/estudios_rutas/geometria/Geometria_tramos_2023_2024/Geometria_tramos"
    
    if info_url_base is None:
        info_url_base = "https://movilidad-opendata.mitma.es/estudios_rutas/informacion_tramo/"
        
    if info_files is None:
        info_files = [
            "20240331_Tramos_info_odmatrix.csv.gz",
            "20240824_Tramos_info_odmatrix.csv.gz",
            "20240827_Tramos_info_odmatrix.csv.gz",
            "20241016_Tramos_info_odmatrix.csv.gz",
            "20241019_Tramos_info_odmatrix.csv.gz"
        ]

    # 1. Geometry Files
    geom_extensions = [".cpg", ".dbf", ".prj", ".shp", ".shx"]
    geom_dir = os.path.join(base_dir, "geometria")
    
    for ext in geom_extensions:
        url = f"{geom_url_base}{ext}"
        filename = f"Geometria_tramos{ext}"
        download_file(url, os.path.join(geom_dir, filename), f"Geometry {ext}")
    
    # 2. Information Files (2024)
    info_dir = os.path.join(base_dir, "informacion_tramo")
    
    for filename in info_files:
        url = f"{info_url_base}{filename}"
        # Convert e.g. 20240331_Tramos_info_odmatrix.csv.gz -> 20240331_info_tramo.csv
        date_prefix = filename.split('_')[0]
        final_filename = f"{date_prefix}_info_tramo.csv"
        download_file(url, os.path.join(info_dir, final_filename), "Traffic Info", decompress=True)

if __name__ == "__main__":
    main()
