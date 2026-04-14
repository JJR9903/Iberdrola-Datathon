import os
import requests

def download_file(url, output_path):
    """Downloads a file from a URL to a local path."""
    print(f"Downloading from: {url}")
    print(f"To: {output_path}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        with requests.get(url, stream=True, timeout=120, headers=headers) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Successfully saved to {output_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

def main(
    url="https://mapas.fomento.gob.es/arcgis2/rest/services/Hermes/0_CARRETERAS/MapServer/19/query?where=GEOM%20is%20not%20null&outFields=*&f=kmz", 
    output_path="data/raw/roads/query.kmz"
):
    download_file(url, output_path)

if __name__ == "__main__":
    main()
