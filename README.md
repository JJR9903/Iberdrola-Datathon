# Iberdrola Datathon 2026: Route to Electrification of Mobility

## Project Overview
This project aims to optimize the deployment of electric vehicle (EV) charging infrastructure across Spain's **interurban road network**. The objective is to identify the most strategic locations for high-power charging stations by balancing mobility demand, 2027 projection scenarios, and the physical constraints of the electrical distribution grid.

---

## 🛠 Project Components & Automation

The project is divided into three functional layers: **Data Acquisition**, **Processing Pipeline**, and **Strategic Analysis**.

### 1. Download & Acquisition Scripts (`scripts/`)
These scripts handle the ingestion of raw data from public sources and ministry portals.
- `download_roads_kmz.py`: Fetches the regional KMZ backbone network.
- `download_road_routes.py`: Automates the download of road geometry and traffic CSV datasets from MITMA.
- `download_electric_capacity.py`: Retrieves electrical hosting capacity XLSX maps from major operators (i-DE, Endesa, Viesgo).
- `download_ev_charging_points.py`: Replaces previous extractors; fetches and cleans the DGT public charging point XML.
- `download_ev_registrations.py`: Replaces previous extractors; manages the bulk download and normalization of DGT vehicle ZIP files.

### 2. Processing & Engineering Pipeline
The "Engine" of the project, responsible for cleaning, merging, and spatial transformation.
- `download_ev_charging_points.py`: Extracts and cleans charging point data from raw formats.
- `download_ev_registrations.py`: Processes historical vehicle registration volumes.
- `merge_traffic_data.py`: Consolidates multi-day traffic CSVs into a single longitudinal dataset.
- `process_electric_capacity.py`: Unifies capacity maps from **i-DE**, **Endesa**, and **Viesgo** into a standard geospatial format.
- `process_road_segments.py`: **The Core Orchestrator**. It performs spatial normalization, joins traffic data to the road backbone, and simplifies the network using "Sequential Greedy Fusion" to create a manageable digital twin.

### 3. Analysis & Strategy Scripts
Advanced logic to identify gaps and optimize deployment.
- `analyze_charging_sites_proximity.py`: Filters for high-power sites (>100kW) and links them spatially to the nearest interurban segments.
- `analyze_segment_intervals.py`: Calculates the **Charging Gap**. It computes the distance between chargers along specific corridors, incorporating "detour factors" (actual distance to the road).

---

## 📊 Data Catalog: Processed Outputs

All processed files are stored in `data/processed/`. These represent the "Gold Layer" of our data lake.

| File Name | Description | Source Script | Intention / Goal |
| :--- | :--- | :--- | :--- |
| `backbone_roads.parquet` | Simplified major road axes. | `process_road_segments.py` | Spatial reference for all interurban analysis. |
| `integrated_road_network.parquet` | Road network fragmented by segments with integrated traffic data. | `process_road_segments.py` | Basis for traffic and demand modeling. |
| `charging_stations.parquet` | High-power (>100kW) station locations. | `analyze_charging_sites_proximity.py` | Identify existing fast-charging nodes. |
| `road_segment_charging_metrics.parquet` | Gap analysis (Avg/Max distance). | `analyze_segment_intervals.py` | Locate segments suffering from "Range Anxiety." |
| `electric_capacity_merged.parquet` | Unified grid capacity layer. | `process_electric_capacity.py` | Consolidated hosting capacity data from Endesa, Iberdrola, and Viesgo. |
| `ev_registrations_forecast.parquet` | 2027 EV adoption projections. | `EV_forecast.ipynb` | Set the target demand for the simulation. |
| `road_routes_traffic.parquet` | Consolidated traffic timeline. | `merge_traffic_data.py` | Weighted traffic demand per road segment. |

---

## 📓 Research & Visualization Notebooks

The `notebooks/` directory contains the analytical "Deep Dives" and visualization layers.

- `EV_forecast.ipynb`: Time-series modeling (ARIMA/Polars) to project vehicle growth to 2027.
- `EV_charging_points.ipynb`: Geospatial exploration of current infrastructure coverage.
- `road_segment_visualization.ipynb`: Large-scale interactive mapping of traffic flows and road connectivity.
- `electric_capacity_visualization.ipynb`: Analysis of grid hosting capacity "bottlenecks."
- `analyze_segments_charging_points.ipynb`: Integrated validation of connectivity and charger proximity.

---

## 🛠 Reproducibility & Environment

This project uses `uv` for high-performance dependency management.

### 1. Environment Setup
```bash
# Sync dependencies and create .venv
uv sync
```

### 2. Running the Code  
You can run the entire processing pipeline via the centralized orchestrator, **`main.py`**. The pipeline follows a specific chronological order to ensure all dependencies are met:

1.  **`download`**: The "Ingestion" step. Fetches all raw data (KMZ, XLSX, CSV, XML, ZIP) from MITMA, DGT, and Grid Operators.
2.  **`charging`**: Processes the downloaded XML data from public charging sites.
3.  **`registrations`**: Processes the historical DGT vehicle registration datasets.
4.  **`capacity`**: Merges and normalizes grid hosting capacity from Endesa, Iberdrola, and Viesgo.
5.  **`traffic`**: Consolidates multi-day traffic CSVs into a single time-series dataset.
6.  **`segments`**: The major synthesis step—links traffic data to the road backbone and simplifies geometry via fusion.
7.  **`proximity`**: Performs final spatial analysis linking charging stations to the nearest road segments.

**Execute the full pipeline:**
```bash
./.venv/bin/python main.py
```

### 3. Pipeline Configuration (`config.toml`)
The orchestrator's behavior is controlled by the `config.toml` file located in the root directory.

#### The `[execution]` Section
- **`steps`**: A list of strings determining which parts of the pipeline to run. 
    - Use `["all"]` to run the full sequence (Starting with `download`).
    - Use a subset like `["download", "registrations"]` for targeted runs.

#### The `[steps.download]` Section
This section centralizes all external dependencies. It contains the URLs and local paths for every required dataset, allowing for easy updates to the target years or data sources without modifying code.
- **`force`**: A boolean flag. 
    - If `false` (default), the orchestrator will skip any step whose output file already exists.
    - If `true`, it will rerun all selected steps regardless of existing outputs.

> [!TIP]
> Each step creates a standardized Parquet file in `data/processed/`. The orchestrator's "smart skipping" saves time when testing specific changes in the second half of the pipeline.

---

## 📅 Final Deliverables (March 2026 Strategy)
- **KPI Scorecard**: Global impact metrics of the proposed station network.
- **Optimal Deployment Map**: Coordinates and charger counts for 2027 readiness.
- **Grid Strategy**: Roadmap for electrical infrastructure reinforcements.
