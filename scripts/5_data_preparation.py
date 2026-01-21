import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
import os
import glob
import ast

# --- 1. CONFIGURATION & STYLE ---
sns.set_theme(style="whitegrid")
SILVER_BASE = os.path.join("recomart-data-lake", "silver")
GOLD_BASE = os.path.join("recomart-data-lake", "gold")
EDA_DIR = "eda_plots"

# Ensure target directories exist
os.makedirs(GOLD_BASE, exist_ok=True)
os.makedirs(EDA_DIR, exist_ok=True)

# --- 2. DYNAMIC SILVER DISCOVERY ---

def get_latest_silver_path(dataset_name):
    """Fetches the latest silver folder based on timestamp."""
    search_path = os.path.join(SILVER_BASE, dataset_name, "dt_*")
    folders = glob.glob(search_path)
    if not folders:
        raise FileNotFoundError(f"No Silver data found for {dataset_name}. Please run Section 4 script first.")
    return max(folders)

# --- 3. CORE PREPROCESSING FUNCTIONS ---

def engineer_interaction_features(df):
    """
    Encodes event types into a Composite Preference Score.
    This creates a numerical 'interaction_weight' for the model.
    """
    # Business logic weights for implicit feedback
    event_weights = {
        'view': 1,
        'click': 2,
        'add_to_cart': 5,
        'purchase': 10
    }
    df['interaction_weight'] = df['event_type'].map(event_weights).fillna(1)
    
    # Standardize time format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def engineer_product_features(df_prod):
    """
    Parses nested dictionary strings and normalizes numerical attributes.
    """
    # Extract numerical rate from stringified dict: "{'rate': 3.9, 'count': 120}"
    def parse_rate(val):
        try:
            return ast.literal_eval(val).get('rate', 0)
        except:
            return 0

    df_prod['rating_val'] = df_prod['rating'].apply(parse_rate)
    
    # Min-Max Normalization (Scales values between 0 and 1)
    scaler = MinMaxScaler()
    df_prod[['norm_price', 'norm_rating']] = scaler.fit_transform(
        df_prod[['price', 'rating_val']]
    )
    return df_prod

# --- 4. EDA & SPARSITY ANALYSIS ---

def run_eda(df_inter, df_prod):
    """
    Generates summary plots for interaction distributions and item popularity.
    """
    print("Generating Exploratory Analysis Plots...")
    
    # Plot 1: Item Popularity (The Long Tail)
    plt.figure(figsize=(10, 5))
    item_counts = df_inter['product_id'].value_counts()
    sns.histplot(item_counts, bins=50, kde=True, color='teal')
    plt.title('Product Popularity (Long Tail Distribution Analysis)', fontsize=14)
    plt.xlabel('Number of Interactions per Product')
    plt.ylabel('Count of Products')
    plt.savefig(f"{EDA_DIR}/item_popularity.png")
    plt.close()

    # Plot 2: User-Item Interaction Heatmap (Sparsity Visualization)
    plt.figure(figsize=(12, 8))
    # Sample top 50 users and items for visibility
    top_u = df_inter['user_id'].value_counts().head(50).index
    top_i = df_inter['product_id'].value_counts().head(50).index
    subset = df_inter[df_inter['user_id'].isin(top_u) & df_inter['product_id'].isin(top_i)]
    
    pivot = subset.pivot_table(index='user_id', columns='product_id', values='interaction_weight', aggfunc='sum').fillna(0)
    sns.heatmap(pivot > 0, cmap="YlGnBu", cbar=False)
    plt.title('User-Item Sparsity Pattern (Top 50 Sample)', fontsize=14)
    plt.savefig(f"{EDA_DIR}/sparsity_heatmap.png")
    plt.close()

def calculate_global_sparsity(df):
    """Calculates matrix sparsity: (1 - (Actual Interactions / Total Possible Cells))"""
    n_users = df['user_id'].nunique()
    n_items = df['product_id'].nunique()
    actual_interactions = len(df.drop_duplicates(['user_id', 'product_id']))
    
    total_matrix_cells = n_users * n_items
    sparsity = (1 - (actual_interactions / total_matrix_cells)) * 100
    
    print("-" * 40)
    print("GLOBAL DATASET METRICS")
    print("-" * 40)
    print(f"Total Unique Users:    {n_users}")
    print(f"Total Unique Products: {n_items}")
    print(f"Calculated Sparsity:   {sparsity:.2f}%")
    print("-" * 40)
    return sparsity

# --- 5. MAIN EXECUTION PIPELINE ---

def run_data_preparation():
    print("Initializing Section 5: Data Preparation...")

    try:
        # Step 1: Load Latest Silver Data
        inter_dir = get_latest_silver_path("user_interactions")
        prod_dir = get_latest_silver_path("product_catalog")
        
        df_inter = pd.read_csv(os.path.join(inter_dir, "user_interactions_silver.csv"))
        df_prod = pd.read_csv(os.path.join(prod_dir, "product_catalog_silver.csv"))
        
        # Step 2: Feature Engineering
        df_inter = engineer_interaction_features(df_inter)
        df_prod = engineer_product_features(df_prod)
        
        # Step 3: EDA & Sparsity Reporting
        calculate_global_sparsity(df_inter)
        run_eda(df_inter, df_prod)
        
        # Step 4: Merge metadata to create the 'Gold' Prepared Dataset
        gold_df = pd.merge(
            df_inter, 
            df_prod[['id', 'category', 'norm_price', 'norm_rating']], 
            left_on='product_id', 
            right_on='id', 
            how='left'
        ).drop(columns=['id']) # Remove duplicate ID after join
        
        # Step 5: Final Export
        gold_output_path = os.path.join(GOLD_BASE, "recomart_gold_prepared.csv")
        gold_df.to_csv(gold_output_path, index=False)
        
        print(f"\n Pipeline Successful!")
        print(f" Gold Dataset Saved: {gold_output_path}")
        print(f" EDA Assets Created in: ./{EDA_DIR}/")

    except Exception as e:
        print(f" Execution Error: {e}")

if __name__ == "__main__":
    run_data_preparation()