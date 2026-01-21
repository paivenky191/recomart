# Recomart Recommendation Engine

Welcome to the **Recomart** project. This repository implements a full **Medallion Architecture** (Bronze → Silver → Gold) to transform raw e-commerce logs into a production-ready recommendation system.



## Repository Structure
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
```
## 🛠️ Setup & Installation
### 1. Clone & Environment
Clone the repository and set up a Python virtual environment to manage dependencies

```code
# Clone the repository
git clone <your-repo-url>
cd recomart-project

# Setup Virtual Environment
python -m venv venv
.\venv\Scripts\activate
```

### 2. Install Dependencies
Install the core libraries for data processing, validation, and machine learning.

```code
pip install pandas numpy matplotlib seaborn great_expectations fpdf dvc mlflow scikit-learn
```

### 3. DVC Initialization
Initialize Data Version Control (DVC) to track your datasets.

```code
# Initialize DVC
dvc init

# Link to a local storage 'Vault' (outside the project folder)
mkdir D:\DMML\Server\Recomart_Data_Vault
dvc remote add -d myremote D:\DMML\Server\Recomart_Data_Vault
```

### 🏎️ Execution Guide (The 9-Section Pipeline)
Follow these steps in order to process data from raw logs to a trained model.

#### Phase 1: Ingestion & Validation

##### 1. Batch Ingestion: Extracts data from API and log sources into the Bronze layer.

```code
python scripts/2-3_batch_ingestion.py
```

##### 2. Data Validation: Executes Great Expectations suites and generates a DQ_Audit_Report.pdf.

```code
python scripts/4_data_validation.py
```

#### Phase 2: Preparation & Transformation

##### 3. Data Preparation: Performs cleaning and EDA to create the Silver layer.

```code
python scripts/5_data_preparation.py
```

##### 4. Feature Engineering: Generates user/item affinity signals for the Gold layer.

```code
python scripts/6_data_transformation.py
```

##### 5. Feature Store: Registers metadata for feature versioning and retrieval.

```code
python scripts/7_feature_store.py
```

#### Phase 3: Lineage & Training

##### 6. Versioning & Lineage: Uses DVC to reproduce the pipeline and track data hashes.

```code
dvc repro
dvc push
```

##### 7. Model Training: Trains a Content-Based Recommender and tracks metrics via MLflow.

```code
python scripts/9_model_training.py
```