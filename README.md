# Iberdrola Datathon 2026: Route to Electrification of Mobility

## Project Overview
This project aims to optimize the deployment of electric vehicle (EV) charging infrastructure across Spain's **interurban road network**. The objective is to identify the most strategic locations for high-power charging stations by balancing mobility demand, 2027 projection scenarios, and the physical constraints of the electrical distribution grid.

---

---

## 📂 Project Structure

The repository is organized into a sequential pipeline. All core logic and orchestrators are located in the `scripts/` directory, while interactive analysis is done in `notebooks/`.

```text
/ (Root)
├── config.toml               # Single source of truth for all parameters
├── notebooks/                # Analytical and visualization tools
│   ├── 01_forecast.ipynb     # EV growth projections
│   └── 02_backbone_analysis.ipynb
└── scripts/
    ├── 01_acquisition.py      # RAW layer ingestion
    ├── 02_standardization.py  # SILVER layer transformation
    ├── 03_processing.py       # GOLD layer foundation
    ├── sync_cloud.py          # Data mirror utility
    └── archive/               # Superseded legacy scripts
```

## 🚀 Quick Start: Data Access

To ensure immediate reproducibility, all standardized datasets are mirrored in our **GCP Data Bucket**. You can synchronize the official "Silver Layer" directly using:

```bash
# Sync standardized datasets from cloud
python3 scripts/sync_cloud.py
```

---

## 🧪 Pipeline Orchestration

The project follows a modular "Medallion Architecture" driven by a sequential pipeline:

### 1. Ingestion (`scripts/01_acquisition.py`)
Manages data fetching from ministry portals (MITMA, DGT, CNMC).
- **Outputs**: `data/raw/`

### 2. Standardization (`scripts/02_standardization.py`)
Transforms raw files into clean, metric-projected (**EPSG:25830**) tabular datasets.
- **Outputs**: `data/standardized/` (Parquet)

### 3. Processing (`scripts/03_processing.py`)
Consolidates all layers into the final analytical backbone foundation.
- **Outputs**: `data/processed/`

---

## 📓 Research & Analysis

Interactive analysis is performed via Jupyter Notebooks. Both notebooks include **automated synchronization**, allowing you to **fast-track the analysis** by bypassing the manual execution of the acquisition and standardization scripts.

- **[01_forecast.ipynb](file:///Users/juanjose/Library/CloudStorage/GoogleDrive-jj.rincon@student.ie.edu/My%20Drive/Iberdrola%20Datathon/notebooks/01_forecast.ipynb)**: Time-series analysis and forecasting of EV registrations to 2027.
- **[02_backbone_analysis.ipynb](file:///Users/juanjose/Library/CloudStorage/GoogleDrive-jj.rincon@student.ie.edu/My%20Drive/Iberdrola%20Datathon/notebooks/02_backbone_analysis.ipynb)**: Spatial analysis, discretization, and infrastructure gap evaluation.

---

## 🛠 Reproducibility & Environment

This project uses `uv` for high-performance dependency management.

### 1. Environment Setup
```bash
# Sync dependencies and create 3.13+ environment
uv sync
```

### 2. Configuration (`config.toml`)
All orchestrators are controlled via the `config.toml` file. You can toggle steps, modify buffer radii, or change sampling intervals without touching code.

---

## 📅 Final Deliverables (March 2026 Strategy)
- **KPI Scorecard**: Global impact metrics of the proposed station network.
- **Optimal Deployment Map**: Coordinates and charger counts for 2027 readiness.
- **Grid Strategy**: Roadmap for electrical infrastructure reinforcements.
