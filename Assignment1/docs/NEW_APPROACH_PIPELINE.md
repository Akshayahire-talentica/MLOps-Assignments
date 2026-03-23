# MLOps Pipeline - New Approach Documentation

## Overview

This document describes the updated MLOps pipeline architecture that follows the modern approach with:
- **S3 + DVC** for data storage and versioning
- **Apache Spark** for distributed ETL
- **Great Expectations** for data validation (ONLY)
- **Feature Engineering** on validated data
- **MLflow** for experiment tracking and model registry
- **Evidently AI** for drift detection and auto-retraining

---

## Pipeline Architecture

### 1. Data Ingestion (S3 + DVC + PySpark)

**Location**: `src/data_ingestion/`

**Approach**:
- Raw data is stored in S3 bucket: `s3://mlops-movielens-poc/raw/`
- DVC tracks data versions (`.dvc` files in `data/raw/`)
- Apache Spark performs distributed ETL processing
- Processed data written back to S3: `s3://mlops-movielens-poc/processed/`

**Key Files**:
- `src/data_ingestion/run_ingestion.py` - Main Spark ingestion runner
- `src/data_ingestion/ingest_movies.py` - MovieLens data ingestor with PySpark
- `src/data_ingestion/s3_storage.py` - S3 client wrapper

**Tests**:
- `tests/test_s3_dvc_ingestion.py` - Tests S3 data loading, DVC integration, PySpark ETL

**Command**:
```bash
python src/data_ingestion/run_ingestion.py
```

**CI/CD Stage**: `data-pipeline`

---

### 2. Data Validation (Great Expectations ONLY)

**Location**: `src/data_validation/`

**Approach**:
- **ONLY Great Expectations** is used for validation (legacy validators removed)
- Validates schema, data quality, and constraints
- Generates HTML and JSON validation reports
- Reports uploaded to S3: `s3://mlops-movielens-poc/reports/great_expectations/`

**Key Files**:
- `src/data_validation/run_ge_validation.py` - GE validation runner
- `src/data_validation/ge_utils.py` - GE utility functions
- `config/ge_validation.yaml` - GE configuration

**Tests**:
- `tests/test_ge_validation.py` - Tests Great Expectations validation only

**Command**:
```bash
python src/data_validation/run_ge_validation.py --stage processed
```

**CI/CD Stage**: `data-pipeline` (after ingestion)

---

### 3. Feature Engineering

**Location**: `src/features/`

**Approach**:
- Feature engineering performed on **validated data only**
- Creates user features, movie features, and interaction features
- Features stored in `data/features/` and synced to S3
- Feature metadata tracked with timestamps

**Key Files**:
- `src/features/feature_engineering.py` - Feature engineering pipeline
- `src/features/feature_validator.py` - Feature quality checks

**Tests**:
- `tests/test_phase2.py` (TestFeatureEngineering) - Tests feature engineering on validated data

**Command**:
```bash
python src/features/feature_engineering.py
```

**CI/CD Stage**: `feature-engineering`

---

### 4. Model Training (MLflow + scikit-learn)

**Location**: `src/training/`

**Approach**:
- **scikit-learn** models trained on feature-engineered data
- **MLflow** tracks all experiments:
  - Parameters (n_components, max_iter, etc.)
  - Metrics (RMSE, MAE, Precision@K)
  - Model artifacts
- Models registered in MLflow Model Registry
- Artifacts stored in S3: `s3://mlops-movielens-poc/mlflow-artifacts/`

**Key Files**:
- `src/training/model_trainer.py` - Model trainer with MLflow integration
- `src/training/model_selection.py` - Model evaluation and selection

**Tests**:
- `tests/test_mlflow_training.py` - Tests MLflow tracking, experiment logging, S3 artifact storage

**Command**:
```bash
export MLFLOW_TRACKING_URI=http://localhost:5000
python src/training/model_trainer.py
python src/training/model_selection.py
```

**CI/CD Stage**: `model-training`

**MLflow UI**:
```bash
mlflow server --backend-store-uri file:///path/to/mlruns \
  --default-artifact-root s3://mlops-movielens-poc/mlflow-artifacts \
  --host 0.0.0.0 --port 5000
```

---

### 5. Drift Detection (Evidently AI)

**Location**: `src/monitoring/`

**Approach**:
- **Evidently AI** calculates model and data drift
- Compares current features against baseline
- Drift thresholds:
  - **Warning**: 0.15 (15% drift)
  - **Critical**: 0.25 (25% drift)
- **Auto-retrain** triggered on critical drift
- Drift reports stored in `reports/drift/` and S3

**Key Files**:
- `src/monitoring/drift_detector.py` - Evidently AI drift detector
- `config/drift_config.yaml` - Drift detection configuration

**Tests**:
- `tests/test_evidently_drift.py` - Tests Evidently AI drift detection and auto-retrain

**Command**:
```bash
python src/monitoring/drift_detector.py
```

**CI/CD Stage**: `drift-detection` → `retrain-on-drift` (if critical drift)

---

## CI/CD Pipeline (GitHub Actions)

**File**: `.github/workflows/mlops-cicd-pipeline.yml`

### Pipeline Stages

1. **code-quality** - Linting with flake8, black, isort
2. **unit-tests** - New approach tests:
   - S3 + DVC + PySpark Ingestion
   - Great Expectations Validation
   - Feature Engineering
   - MLflow Model Training
   - Evidently AI Drift Detection
3. **data-pipeline** - S3 + DVC + PySpark + GE validation
4. **feature-engineering** - Feature creation on validated data
5. **model-training** - MLflow experiment tracking
6. **drift-detection** - Evidently AI drift check
7. **retrain-on-drift** - Auto-retrain if critical drift detected (⚠️ NEW)
8. **build-docker-images** - Build API, Model v2, Router
9. **integration-tests** - Docker Compose full stack
10. **push-to-ecr** - Push to AWS ECR
11. **deploy-to-eks** - Deploy to EKS cluster
12. **eks-smoke-tests** - Verify deployment
13. **deploy-local** - Deploy to Docker Desktop
14. **pipeline-summary** - Final status report

### Auto-Retrain on Drift

When critical drift is detected:
1. Drift detector outputs `critical_drift=true`
2. `retrain-on-drift` job triggers
3. Full pipeline re-runs automatically
4. Incident report created in `reports/incidents/`

---

## Data Flow

```
Raw Data (S3) 
  ↓ [DVC tracking]
PySpark ETL
  ↓
Processed Data (S3)
  ↓
Great Expectations Validation
  ↓
Feature Engineering
  ↓
Features (S3)
  ↓
scikit-learn Training
  ↓
MLflow Registry (S3 artifacts)
  ↓
Model Serving (Docker + K8s)
  ↓
Evidently AI Drift Detection
  ↓ [if critical drift]
Auto-Retrain (full pipeline)
```

---

## Configuration Files

| File | Purpose |
|------|---------|
| `config/data_ingestion_config.yaml` | S3, DVC, Spark, training config |
| `config/ge_validation.yaml` | Great Expectations validation |
| `config/drift_config.yaml` | Evidently AI drift thresholds |
| `.dvc/config` | DVC remote storage (S3) |
| `dvc.yaml` | DVC pipeline stages |

---

## Running the Complete Pipeline Locally

```bash
# 1. Pull raw data from S3 via DVC
dvc pull

# 2. Run PySpark ingestion
python src/data_ingestion/run_ingestion.py

# 3. Validate with Great Expectations
python src/data_validation/run_ge_validation.py --stage processed

# 4. Feature engineering
python src/features/feature_engineering.py

# 5. Train model with MLflow
export MLFLOW_TRACKING_URI=http://localhost:5000
python src/training/model_trainer.py

# 6. Detect drift
python src/monitoring/drift_detector.py

# 7. Build and deploy
docker-compose up -d
```

---

## Testing Strategy

### Unit Tests

Run all new approach tests:
```bash
pytest tests/test_s3_dvc_ingestion.py -v
pytest tests/test_ge_validation.py -v
pytest tests/test_mlflow_training.py -v
pytest tests/test_evidently_drift.py -v
pytest tests/test_phase2.py -v
```

### Integration Tests

```bash
pytest tests/ -m integration -v
```

### CI Tests

GitHub Actions runs all tests automatically on push to `main`, `tohid`, `tohid-devops`, `develop`

---

## Key Improvements

### ✅ From Old Approach → New Approach

| Component | Old | New |
|-----------|-----|-----|
| **Data Storage** | Local files | S3 + DVC versioning |
| **ETL** | Pandas (single-node) | PySpark (distributed) |
| **Validation** | Custom validators | Great Expectations ONLY |
| **Tracking** | Local logs | MLflow experiment tracking |
| **Artifacts** | Local storage | S3 bucket (`mlops-movielens-poc`) |
| **Drift** | Manual checks | Evidently AI auto-detection |
| **Retrain** | Manual | Automatic on critical drift |
| **Workflows** | 3 separate files | 1 consolidated pipeline |

---

## Troubleshooting

### Issue: Flake8 E999 SyntaxError

**Solution**: Fixed in `src/data_validation/run_validation.py` - removed legacy Spark code, kept GE-only implementation

### Issue: Multiple workflows running

**Solution**: Consolidated to single `mlops-cicd-pipeline.yml`, removed `tests.yml` and `docker-build.yml`

### Issue: Tests using old validators

**Solution**: Created new test files for new approach:
- `test_s3_dvc_ingestion.py`
- `test_ge_validation.py`
- `test_mlflow_training.py`
- `test_evidently_drift.py`

---

## S3 Bucket Structure

```
s3://mlops-movielens-poc/
├── raw/                          # Raw data (DVC tracked)
├── processed/                    # PySpark processed data
│   ├── movies/
│   ├── ratings/
│   └── users/
├── features/                     # Engineered features
├── mlflow-artifacts/             # MLflow model artifacts
├── reports/
│   ├── great_expectations/       # GE validation reports
│   └── drift/                    # Evidently AI drift reports
├── dvc-cache/                    # DVC remote cache
└── baseline/                     # Baseline data for drift detection
```

---

## Airflow Integration

**Location**: `airflow/dags/`

Airflow can be used to orchestrate the pipeline:
- Schedule PySpark ingestion jobs
- Trigger validation after ingestion
- Monitor pipeline health

**To deploy Airflow**:
```bash
kubectl apply -f airflow/manifests/
```

---

## Monitoring & Observability

- **MLflow UI**: http://localhost:5000 - Experiment tracking
- **Prometheus**: http://localhost:9090 - Metrics collection
- **Grafana**: http://localhost:3000 - Visualization
- **Drift Reports**: `reports/drift/` - Evidently AI HTML reports

---

## Production Deployment

### EKS Deployment

```bash
# Build and push to ECR
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin $ECR_REGISTRY

docker build --platform linux/amd64 -t $ECR_REGISTRY/mlops-api:latest .
docker push $ECR_REGISTRY/mlops-api:latest

# Deploy to EKS
kubectl apply -f k8s-minimal/ -n mlops
```

### Local Docker Desktop

```bash
docker-compose up -d
```

---

## Contributors

- Data Ingestion: Akshay
- Validation: Mohit
- Training: Vikas
- Drift Detection: Vikas
- CI/CD: Tohid

---

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Great Expectations](https://greatexpectations.io/)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Evidently AI](https://docs.evidentlyai.com/)
- [DVC Documentation](https://dvc.org/doc)

---

**Last Updated**: February 12, 2026
