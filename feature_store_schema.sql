-- Schema for Recomart Feature Store (PostgreSQL/BigQuery compatible)

-- Table 1: User Feature Store (Activity & Global Preferences)
CREATE TABLE user_feature_store (
    user_id VARCHAR(50) PRIMARY KEY,
    total_interactions INT,
    avg_interaction_weight FLOAT, -- Mean intensity of engagement
    most_frequent_category VARCHAR(100),
    last_active_timestamp TIMESTAMP,
    user_sparsity_ratio FLOAT -- Interactions relative to total catalog
);

-- Table 2: Item Feature Store (Popularity & Quality)
CREATE TABLE item_feature_store (
    product_id INT PRIMARY KEY,
    interaction_count INT,
    avg_user_rating FLOAT, -- Parsed from the 'rating' field
    global_popularity_score FLOAT, -- Normalized interaction count
    category_encoded INT,
    normalized_price FLOAT
);

-- Table 3: Interaction Matrix (Similarity & Co-occurrence Input)
CREATE TABLE interaction_matrix (
    user_id VARCHAR(50),
    product_id INT,
    composite_affinity_score FLOAT, -- Aggregated interaction weights
    PRIMARY KEY (user_id, product_id)
);