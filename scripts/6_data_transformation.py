import pandas as pd
import numpy as np
import os

# --- 1. CONFIGURATION ---
# Paths should match your established Data Lake structure
GOLD_FILE = os.path.join(".", "recomart-data-lake", "gold", "recomart_gold_prepared.csv")
FEATURE_STORE_DIR = os.path.join(".", "recomart-data-lake", "feature_store")

# Ensure the feature store directory exists
os.makedirs(FEATURE_STORE_DIR, exist_ok=True)

def run_feature_transformation():
    print("Starting Section 6: Feature Engineering & Transformation...")
    
    # Check if the Gold file exists from Section 5
    if not os.path.exists(GOLD_FILE):
        print(f"Error: Gold file not found at {GOLD_FILE}")
        print("Please run Section 5 script first to generate the Gold dataset.")
        return

    # Load the Gold dataset
    df = pd.read_csv(GOLD_FILE)
    
    # Ensure timestamp is datetime for aggregation
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --- 2. USER FEATURE ENGINEERING (Activity & Engagement) ---
    # Using Named Aggregation to prevent MultiIndex issues (fixes KeyError)
    print("Engineering User Features...")
    user_features = df.groupby('user_id').agg(
        user_activity_count=('interaction_weight', 'count'),
        user_avg_affinity=('interaction_weight', 'mean'),
        user_total_score=('interaction_weight', 'sum'),
        last_active_ts=('timestamp', 'max')
    ).reset_index()

    # --- 3. ITEM FEATURE ENGINEERING (Popularity & Quality) ---
    print("Engineering Item Features...")
    item_features = df.groupby('product_id').agg(
        item_interaction_count=('interaction_weight', 'count'),
        item_avg_affinity=('interaction_weight', 'mean'),
        norm_price=('norm_price', 'first'),
        norm_rating=('norm_rating', 'first'),
        category=('category', 'first')
    ).reset_index()
    
    # Calculate Global Popularity Score (Log-normalized to reduce blockbuster bias)
    item_features['global_popularity_score'] = np.log1p(item_features['item_interaction_count'])

    # --- 4. INTERACTION MATRIX (Similarity Input) ---
    print("Creating User-Item Affinity Matrix...")
    interaction_matrix = df.groupby(['user_id', 'product_id']).agg(
        affinity_score=('interaction_weight', 'sum')
    ).reset_index()

    # --- 5. STORAGE (Feature Warehouse) ---
    print(f"Saving Transformed Data to {FEATURE_STORE_DIR}...")
    
    user_features.to_csv(os.path.join(FEATURE_STORE_DIR, "user_feature_store.csv"), index=False)
    item_features.to_csv(os.path.join(FEATURE_STORE_DIR, "item_feature_store.csv"), index=False)
    interaction_matrix.to_csv(os.path.join(FEATURE_STORE_DIR, "user_item_affinity_matrix.csv"), index=False)

    print("-" * 45)
    print("TASK COMPLETE")
    print(f"Total Users Processed: {len(user_features)}")
    print(f"Total Items Processed: {len(item_features)}")
    print(f"Total Unique Interactions: {len(interaction_matrix)}")
    print("-" * 45)

if __name__ == "__main__":
    run_feature_transformation()