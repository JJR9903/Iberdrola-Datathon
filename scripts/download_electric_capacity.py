import os
import requests

"""
DOWNLOAD SCRIPT FOR ELECTRIC CAPACITY DATASETS (APRIL 2026)

This script downloads the capacity datasets for Iberdrola (i-DE), Endesa (e-distribución), 
and Viesgo as used in the Iberdrola Datathon project. 

Manual Access Documentation:
---------------------------
1. Iberdrola (i-DE):
   - Website: https://www.i-de.es/conexion-red-electrica/suministro-electrico/mapa-capacidad-consumo
   - Steps: Scroll past the interactive map to the section "También puedes descargar y consultar 
     los datos del mapa en los siguientes formatos". Click the "XLSX" link.
   - Version: April 2026 (2026_04_01).

2. Endesa (e-distribución):
   - Website: https://www.edistribucion.com/es/red-electrica/nodos-capacidad-red/capacidad-generacion.html
   - Steps: Scroll down to "Capacidad de generación en e-distribución abril 2026 xlsx". 
     Click the download button.
   - Version: April 2026 (2026_04_01).

3. Viesgo:
   - Website: https://www.viesgodistribucion.com/mapa-interactive-de-la-red
   - Steps: Click "Capacidad de generación en la red", scroll down below the map to 
     "Información de capacidad de Acceso de Generación archivo XLSX".
   - Version: April 2026 (2026_04_01).

Note: These URLs are pinned to the April 2026 versions to ensure reproducibility.
"""

def download_file(url, output_path, label):
    """Downloads a file from a URL to a local path with User-Agent headers."""
    print(f"[{label}] Downloading from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        with requests.get(url, stream=True, timeout=120, headers=headers) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"[{label}] Successfully saved to {output_path}")
    except Exception as e:
        print(f"[{label}] Error downloading file: {e}")
        # We don't raise here to allow other downloads to continue, 
        # but in a production pipeline you might want to.

def main(datasets=None):
    base_dir = "data/raw/electric_capacity"
    
    if datasets is None:
        datasets = [
            {
                "label": "Iberdrola",
                "url": "https://www.i-de.es/documents/d/guest/2026_04_01_r1-001_demanda-1-",
                "filename": "Iberdrola_2026_04_01.xlsx"
            },
            {
                "label": "Endesa",
                "url": "https://www.edistribucion.com/content/dam/edistribucion/conexion-a-la-red/descargables/nodos/generacion/202604/2026_04_01_R1299_generación.xlsx",
                "filename": "Endesa_2026_04_01.xlsx"
            },
            {
                "label": "Viesgo",
                "url": "https://storage.googleapis.com/apdes-prd-interactivemap-resources/network-capacity-map/v2/viesgo/doc_generation/2026_04_01_R1005_generacion.xlsx",
                "filename": "Viesgo_2026_04_01.xlsx"
            }
        ]
    
    for ds in datasets:
        output_path = os.path.join(base_dir, ds["filename"])
        download_file(ds["url"], output_path, ds["label"])

if __name__ == "__main__":
    main()
