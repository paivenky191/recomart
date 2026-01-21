# 🚀 Recomart Recommendation Engine

Welcome to the **Recomart** project. This repository implements a full **Medallion Architecture** (Bronze → Silver → Gold) to transform raw e-commerce logs into a production-ready recommendation system.



## 📁 Repository Structure
```text
recomart-project/
├── .dvc/                       # DVC internal configuration
├── recomart-data-lake/         # The Managed Data Lake (Tracked by DVC)
│   ├── bronze/                 # Raw ingested data
│   ├── silver/                 # Validated, cleaned data
│   └── gold/                   # Feature-engineered data
├── scripts/                    # Core Pipeline Scripts
│   ├── 2-3_batch_ingestion.py  # Section 1-3: Batch Ingestion & Landing
│   ├── 4_data_validation.py    # Section 4: Data Quality (GX Audit)
│   ├── 5_data_preparation.py   # Section 5: Data Prep & EDA
│   ├── 6_data_transformation.py# Section 6: Feature Engineering
│   ├── 7_feature_store.py      # Section 7: Metadata Registry
│   └── 9_model_training.py     # Section 9: RecSys Training (MLflow)
├── dvc.yaml                    # Section 8: Data Lineage Map
├── metadata_registry.json      # Feature Store Metadata
└── mlruns/                     # MLflow experiment tracking

