import os
import sys
import tomllib

# Add the current directory (scripts/) to allow module-based imports
current_dir = os.path.dirname(os.path.realpath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from processing import merge_traffic_data
    from processing import create_backbone_foundation
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def load_config(config_path="config.toml"):
    """Loads and returns the TOML configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found at {os.getcwd()}")
        sys.exit(1)
    
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def run_step(step_name, config, force=False):
    """
    Executes a single processing step with configuration injection.
    Includes smart skipping and dependency checking.
    """
    step_config = config['steps'].get(step_name)
    if not step_config:
        print(f"Error: Configuration for step '{step_name}' not found under [steps].")
        return False

    # 1. Dependency Check
    dependencies = step_config.get('depends_on', [])
    for dep in dependencies:
        if not os.path.exists(dep):
            print(f"ERROR: Prerequisite missing for '{step_name}'. Please ensure raw data is downloaded and prior steps are run. Missing: {dep}")
            return False

    # 2. Smart Skipping Logic
    output_path = step_config.get('output_path')
    if output_path and os.path.exists(output_path) and not force:
        print(f"Skipping '{step_name}': Output already exists at {output_path}")
        return True

    print(f"\n>>> Executing Step: {step_name}")
    
    try:
        if step_name == "traffic":
            merge_traffic_data.main(
                input_dir=step_config['raw_dir'],
                output_path=step_config['output_path']
            )
        elif step_name == "backbone_foundation":
            create_backbone_foundation.main(
                kmz_path=step_config['kmz_path'],
                traffic_shp_path=step_config['traffic_shp_path'],
                traffic_parquet_path=step_config['traffic_parquet_path'],
                chargers_path=step_config['chargers_path'],
                gas_stations_path=step_config['gas_stations_path'],
                output_path=step_config['output_path'],
                sub_steps=step_config.get('sub_steps', ["all"]),
                traffic_columns=step_config.get('traffic_columns', ["total_max"]),
                sampling_interval_m=step_config.get('sampling_interval_m', 200),
                buffer_radius_m=step_config.get('buffer_radius_m', 50),
                max_distance_proximity=step_config.get('max_distance_proximity', None)
            )
        else:
            print(f"Error: Manual glue-code for step '{step_name}' is missing in 03_processing.py.")
            return False
            
        return True
    except Exception as e:
        print(f"CRITICAL FAILURE in '{step_name}': {e}")
        return False

def main():
    """Main orchestrator entry point for processing."""
    print("=== Iberdrola Datathon: Data Processing Orchestrator ===\n")
    
    # Allow running from root if scripts/03_processing.py is called from root
    config_path = "config.toml"
    if not os.path.exists(config_path):
        config_path = os.path.join("..", "config.toml")
        
    config = load_config(config_path)
    
    # Read execution settings
    execution = config.get('process_execution', config.get('execution', {}))
    steps_requested = execution.get('steps', ["all"])
    force_run = execution.get('force', False)

    # Definitive order of processing steps (Simplified)
    canonical_order = [
        "traffic", 
        "backbone_foundation"
    ]
    
    if "all" in steps_requested:
        steps_to_run = canonical_order
    else:
        steps_to_run = [s for s in canonical_order if s in steps_requested]
        
        # Check for user typos
        invalid = [s for s in steps_requested if s not in canonical_order and s != "all"]
        if invalid:
            print(f"Warning: Disregarding unknown step names found in config: {invalid}")

    if not steps_to_run:
        print("No valid processing steps selected for execution.")
        return

    print(f"Sequence: {', '.join(steps_to_run)}")
    print(f"Force Flag: {force_run}")

    for step in steps_to_run:
        if not run_step(step, config, force=force_run):
            print(f"\nPipeline ABORTED at step: {step}")
            sys.exit(1)

    print("\n=== All processing tasks finished successfully ===")

if __name__ == "__main__":
    main()
