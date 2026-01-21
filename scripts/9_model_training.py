import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import mlflow
import os
import datetime

# --- 1. DATA RETRIEVAL ---
# We use the Item Feature Store created in Section 6
ITEM_FEAT_PATH = os.path.join(".", "recomart-data-lake", "feature_store", "item_feature_store.csv")

def run_content_based_recommender():
    mlflow.set_experiment("Recomart_Content_Based_RecSys")
    
    with mlflow.start_run(run_name=f"TFIDF_Similarity_{datetime.datetime.now().strftime('%Y%m%d')}"):
        print("Loading Item Features...")
        df_items = pd.read_csv(ITEM_FEAT_PATH)

        # --- 2. TEXT VECTORIZATION ---
        # We turn the 'category' into a numerical vector using TF-IDF
        # This gives higher weight to rare, specific categories
        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform(df_items['category'].fillna('None'))
        
        print(f"Item Feature Matrix Shape: {tfidf_matrix.shape}")

        # --- 3. SIMILARITY CALCULATION ---
        # Linear kernel is faster than cosine_similarity for TF-IDF matrices
        cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
        
        # Log metadata to MLflow
        mlflow.log_param("vectorizer", "TF-IDF")
        mlflow.log_param("similarity_metric", "Cosine (Linear Kernel)")
        mlflow.log_param("total_items", len(df_items))

        # --- 4. RECOMMENDATION FUNCTION ---
        def get_recommendations(product_id, top_n=5):
            # Get index of the product
            idx = df_items.index[df_items['product_id'] == product_id].tolist()[0]
            
            # Get pairwise similarity scores for all products
            sim_scores = list(enumerate(cosine_sim[idx]))
            
            # Sort by similarity score
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            
            # Get top_n most similar (skipping the first one, which is itself)
            sim_scores = sim_scores[1:top_n+1]
            
            item_indices = [i[0] for i in sim_scores]
            return df_items.iloc[item_indices][['product_id', 'category', 'norm_rating']]

        # --- 5. EVALUATION (Sample) ---
        sample_id = df_items['product_id'].iloc[0]
        recs = get_recommendations(sample_id)
        
        print(f"\nContent-Based Recommendations for Product ID {sample_id}:")
        print(recs)
        
        mlflow.set_tag("model_type", "Content-Based")
        print(f"\nExperiment tracked in MLflow. Run ID: {mlflow.active_run().info.run_id}")

if __name__ == "__main__":
    run_content_based_recommender()