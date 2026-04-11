# Iberdrola Datathon 2026: Route to Electrification of Mobility

## Project Overview
This project aims to optimize the deployment of electric vehicle (EV) charging infrastructure across Spain's **interurban road network**. The objective is to identify the most strategic locations for high-power charging stations by balancing mobility demand, 2027 projection scenarios, and the physical constraints of the electrical distribution grid.

The goal is to solve "range anxiety" for long-distance travel while minimizing capital expenditure and ensuring grid stability.

---

## 🛠 What We Have Done So Far

We have built a robust data engineering and analytics foundation focused on three pillars: **Demand**, **Infrastructure**, and **Capacity**.

### 1. Data Processing Architecture
- **Centralized Orchestrator**: Developed `main.py` to automate the entire data pipeline.
- **DGT Registration Pipeline**: Automated the extraction and cleaning of historical vehicle registration data from the DGT portal (`scripts/extract_ev_registrations.py`).
- **Charging Point Consolidation**: Unified disparate datasets of current EV charging stations across Spain (`scripts/extract_ev_charging_points.py`).
- **Electrical Grid Capacity**: Merged and processed distribution capacity data from major grid operators: **i-DE (Iberdrola)**, **Endesa**, and **Viesgo** (`scripts/process_electric_capacity.py`).
- **Road Route Processing**: Integrated and simplified road network segments from Shapefiles and KMZ sources, optimizing for interurban connectivity analysis (`scripts/process_road_segments.py`).

### 2. Analytical Models
- **Predictive Forecasting**: Implemented a high-performance forecasting model using **Polars** and **Statsmodels** to project EV adoption trends toward the 2027 target horizon (`notebooks/EV_forecast.ipynb`).
- **Geospatial Infrastructure Mapping**: Created interactive visualizations to analyze the current distribution of charging points and electrical substations (`notebooks/*.html`).

---

## 🛠 Reproducibility & Environment

This project uses `uv` for high-performance dependency management and reproducible virtual environments.

### 1. Project Configuration
The project dependencies and Python version constraints are defined in `pyproject.toml`.

### 2. Environment Setup
To create and synchronize the virtual environment, run:
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies and create .venv
uv sync
```

### 3. Running the Code
You can run the processing pipeline in two ways:

**Option A: Direct Execution (without activation)**
You can run scripts directly using the virtual environment's Python executable without manually activating the shell:
```bash
./.venv/bin/python main.py
```

**Option B: Activated Environment**
Activate the environment and run as usual:
```bash
source .venv/bin/activate
python main.py
```

---

## 🚀 Next Steps: Processing & Integration

The next phase moves from data collection to **strategic optimization**:

1.  **Spatial Network Analysis**: 
    - Perform a spatial join between the Spain Interurban Road Network (MTR data) and existing charging points.
    - Identify "dark spots" or segments with high traffic but low charger density.
2.  **Demand-Capacity Collision**:
    - Build a "Friction Model" that overlays projected 2027 charging demand with current grid hosting capacity.
    - Model potential congestion at specific electrical nodes.
3.  **Optimization Engine**:
    - Develop an algorithm to suggest the *minimum* number of new stations required to achieve full network connectivity (removing range anxiety).

---

## 📅 Final Deliverables (March 2026)

To secure the final submission as per the **IE-Iberdrola Datathon** requirements, we are moving toward the following deliverables:

### 1. The Scorecard (Global Network KPIs)
A unified analysis of our proposed infrastructure's impact, including:
- Total chargers proposed.
- Average distance between stations on main corridors.
- Estimated utilization rates for 2027.

### 2. Optimal Station Dataset
A structured dataset containing:
- Precise geographic coordinates for proposed stations.
- Number and power of chargers per location.
- **Grid Viability Status**: Identifying which locations are "Plug-and-Play" vs. those requiring "Grid Reinforcement."

### 3. Grid Bottleneck Analysis
A geospatial report identifying where high mobility demand collides with low hosting capacity, providing a roadmap for electrical grid upgrades.

### 4. Strategic Narrative
A business-driven proposal justifying the rollout strategy, including phasing (2025–2027) and workarounds for grid-constrained areas.

---

## 📂 Project Structure
- `scripts/`: Data extraction and transformation modules.
- `notebooks/`: Exploratory analysis and model development.
- `data/`: Consolidated datasets (Parquet/CSV).
- `main.py`: Main entry point for the data pipeline.
