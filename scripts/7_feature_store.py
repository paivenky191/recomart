import pandas as pd
import json
import os
import datetime

# ==========================================
# 1. FEATURE STORE ENGINE
# ==========================================
class RecomartFeatureStore:
    def __init__(self, base_path=os.path.join(".", "recomart-data-lake", "feature_store")):
        self.base_path = base_path
        self.registry_path = os.path.join(self.base_path, "metadata_registry.json")
        
        # Ensure the feature store directory exists
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path, exist_ok=True)
            
        self.registry = self._load_registry()

    def _load_registry(self):
        """Loads the metadata registry or initializes a new one."""
        if os.path.exists(self.registry_path):
            with open(self.registry_path, 'r') as f:
                return json.load(f)
        return {"feature_views": {}, "metadata": {"project": "Recomart", "last_updated": ""}}

    def register_feature_view(self, name, source_filename, entity_key, features, version="v1.0"):
        """
        Documents and versions a group of features (Feature View).
        """
        full_source_path = os.path.abspath(os.path.join(self.base_path, source_filename))
        
        if not os.path.exists(full_source_path):
            print(f"❌ Error: File not found: {full_source_path}")
            return

        self.registry["feature_views"][name] = {
            "source": full_source_path,
            "entity_key": entity_key,
            "feature_list": features,
            "version": version,
            "created_at": str(datetime.datetime.now())
        }
        self.registry["metadata"]["last_updated"] = str(datetime.datetime.now())
        
        with open(self.registry_path, 'w') as f:
            json.dump(self.registry, f, indent=4)
        print(f"📝 Feature View '{name}' ({version}) successfully registered.")

    def get_historical_features(self, view_name):
        """Retrieves the full dataset for batch model training."""
        if view_name not in self.registry["feature_views"]:
            raise ValueError(f"Feature view '{view_name}' is not registered.")
        
        view = self.registry["feature_views"][view_name]
        df = pd.read_csv(view["source"])
        
        # Filter for only the registered features + the entity key
        cols_to_keep = [view["entity_key"]] + view["feature_list"]
        return df[cols_to_keep]

    def get_online_feature(self, view_name, entity_id):
        """Retrieves specific features for a single entity for real-time inference."""
        df = self.get_historical_features(view_name)
        entity_key = self.registry["feature_views"][view_name]["entity_key"]
        
        # Cast both to string for robust matching
        result = df[df[entity_key].astype(str) == str(entity_id)]
        if result.empty:
            return None
        return result

# ==========================================
# 2. REGISTRATION & RETRIEVAL DEMO
# ==========================================
def run_feature_store_pipeline():
    print("🚀 Initializing Recomart Feature Store Pipeline...")
    fs = RecomartFeatureStore()

    # --- 1. Register User Features (Activity/Frequency) ---
    fs.register_feature_view(
        name="user_signals",
        source_filename="user_feature_store.csv",
        entity_key="user_id",
        features=["user_activity_count", "user_avg_affinity", "user_total_score"],
        version="v1.1"
    )

    # --- 2. Register Item Features (Popularity/Quality) ---
    fs.register_feature_view(
        name="item_signals",
        source_filename="item_feature_store.csv",
        entity_key="product_id",
        features=["item_interaction_count", "global_popularity_score", "norm_rating"],
        version="v1.1"
    )

    # --- 3. Register Affinity Matrix (The Model Target) ---
    fs.register_feature_view(
        name="affinity_matrix",
        source_filename="user_item_affinity_matrix.csv",
        entity_key="user_id",
        features=["product_id", "affinity_score"],
        version="v1.0"
    )

    print("\n" + "="*50)
    print("📊 SAMPLE FEATURE RETRIEVAL DEMONSTRATION")
    print("="*50)
    
    try:
        # A. Batch Training Retrieval (Offline)
        # ------------------------------------
        # The model asks for the affinity matrix view to begin training
        train_df = fs.get_historical_features("affinity_matrix")
        print(f"✅ Training Set (Affinity Matrix) retrieved: {len(train_df)} rows.")

        # B. Real-time Inference Retrieval (Online Simulation)
        # ------------------------------------
        # When a user visits, we fetch their activity signals for ranking
        sample_user_id = train_df['user_id'].iloc[0] # Pick the first user for testing
        online_profile = fs.get_online_feature("user_signals", sample_user_id)
        
        if online_profile is not None:
            print(f"✅ Online Feature Retrieval for User '{sample_user_id}':")
            # Displaying the specific columns model needs for inference
            print(online_profile.to_string(index=False))
        else:
            print(f"❓ User '{sample_user_id}' profile not found.")

    except Exception as e:
        print(f"⚠️ Error during retrieval: {e}")
        print("💡 Ensure Section 6 was run successfully and column names match.")
    
    print("="*50)

if __name__ == "__main__":
    run_feature_store_pipeline()