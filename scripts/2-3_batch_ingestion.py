import pandas as pd
import requests
import logging
import os
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# 1. GLOBAL CONFIGURATION & AUDIT SETUP
# ==========================================
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BRONZE_PATH = "./recomart-data-lake/bronze"
LOG_FILE = f"./logs/ingestion_{TIMESTAMP}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Created directory: {path}")

# ==========================================
# 2. ERROR HANDLING & RETRY MECHANISMS
# ==========================================
def get_api_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504] 
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

# ==========================================
# 3. INGESTION FUNCTIONS (BRONZE LAYER)
# ==========================================

def ingest_user_interactions(source_path):
    """Source type 1: Local CSV -> Bronze CSV"""
    try:
        logging.info(f"--- Starting Ingestion: User Interactions ---")
        df = pd.read_csv(source_path)
        
        target_dir = os.path.join(BRONZE_PATH, "user_interactions", f"dt_{TIMESTAMP}")
        create_directory(target_dir)
        
        # Output as CSV for easy inspection
        output_file = os.path.join(target_dir, "interactions.csv")
        df.to_csv(output_file, index=False)
        
        logging.info(f"SUCCESS: Ingested {len(df)} rows to {output_file}")
        
    except FileNotFoundError:
        logging.error(f"CRITICAL: Source file not found at {source_path}")
    except Exception as e:
        logging.error(f"FAILED: Unexpected error during CSV ingestion: {str(e)}")

def ingest_product_data(api_url):
    """Source type 2: REST API -> Bronze CSV"""
    try:
        logging.info(f"--- Starting Ingestion: Product Catalog API ---")
        session = get_api_session()
        response = session.get(api_url, timeout=10)
        response.raise_for_status() 
        
        # Convert API JSON response directly to a DataFrame
        data = response.json()
        df_products = pd.DataFrame(data)
        
        target_dir = os.path.join(BRONZE_PATH, "product_catalog", f"dt_{TIMESTAMP}")
        create_directory(target_dir)
        
        # Saving as CSV instead of JSON
        output_file = os.path.join(target_dir, "products.csv")
        df_products.to_csv(output_file, index=False)
            
        logging.info(f"SUCCESS: API data converted and saved to {output_file}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"FAILED: API Request error: {e}")
    except Exception as e:
        logging.error(f"FAILED: Unexpected error during API ingestion: {str(e)}")

# ==========================================
# 4. EXECUTION
# ==========================================
if __name__ == "__main__":

    ingest_user_interactions("./source_data/interactions.csv")
    ingest_product_data("https://fakestoreapi.com/products")
    
    logging.info("Ingestion pipeline run complete.")