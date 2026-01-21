from dagster import asset, Definitions, AssetIn
import sys
import subprocess
import os

def run_script(script_name):
    # Get the path to the current Python executable (the one in your venv)
    python_executable = sys.executable 
    
    script_path = os.path.join("scripts", script_name)
    
    print(f"📡 Executing {script_name} using {python_executable}...")
    
    result = subprocess.run(
        [python_executable, script_path], 
        capture_output=True, 
        text=True
    )
    
    if result.returncode != 0:
        # This will now show the detailed error in the Dagster UI
        raise Exception(f"❌ Stage {script_name} failed:\n{result.stderr}")
    
    print(f"✅ {script_name} completed successfully.")

# --- ASSET 1: BRONZE LAYER ---
@asset
def bronze_data():
    run_script("2-3_batch_ingestion.py")
    return "recomart-data-lake/bronze"

# --- ASSET 2: SILVER LAYER (DEPENDS ON BRONZE) ---
@asset(ins={"upstream": AssetIn("bronze_data")})
def silver_data(upstream):
    run_script("4_data_validation.py")
    return "recomart-data-lake/silver"

# --- ASSET 3: GOLD LAYER (DEPENDS ON SILVER) ---
@asset(ins={"upstream": AssetIn("silver_data")})
def gold_data(upstream):
    run_script("5_data_preparation.py")
    return "recomart-data-lake/gold"

# --- ASSET 4: FEATURE STORE (DEPENDS ON GOLD) ---
@asset(ins={"upstream": AssetIn("gold_data")})
def feature_store(upstream):
    run_script("6_data_transformation.py")
    run_script("7_feature_store.py")
    return "metadata_registry.json"

# --- ASSET 5: TRAINED MODEL (DEPENDS ON FEATURE STORE) ---
@asset(ins={"upstream": AssetIn("feature_store")})
def trained_model(upstream):
    run_script("9_model_training.py")
    return "mlflow_run"

# --- REPOSITORY DEFINITION ---
defs = Definitions(
    assets=[bronze_data, silver_data, gold_data, feature_store, trained_model]
)