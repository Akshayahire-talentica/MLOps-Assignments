# MLOps Complete Pipeline - Production Ready

## 🎯 Overview

This is a **production-ready MLOps POC** implementing the complete machine learning lifecycle with industry-standard tools and practices.

## 🏗️ Architecture

### Complete ML Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RAW DATA (S3 + DVC)                          │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              DATA INGESTION + PYSPARK ETL (Airflow)                 │
│  • Load from S3                                                      │
│  • Distributed processing with PySpark                               │
│  • Data quality checks                                               │
│  • Write to S3 (Parquet)                                            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│           DATA VALIDATION (Great Expectations)                       │
│  • Schema validation                                                 │
│  • Data quality checks                                               │
│  • Statistical profiling                                             │
│  • Reports to S3                                                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   FEATURE ENGINEERING                                │
│  • User features                                                     │
│  • Movie features                                                    │
│  • Interaction features                                              │
│  • Upload to S3                                                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│         MODEL TRAINING (Scikit-learn + MLflow)                       │
│  • NMF recommendation model                                          │
│  • Experiment tracking (MLflow)                                      │
│  • Model registry                                                    │
│  • Artifacts to S3                                                   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              MODEL EVALUATION & SELECTION                            │
│  • Performance metrics                                               │
│  • Model comparison                                                  │
│  • Best model promotion                                              │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│            DRIFT DETECTION (Evidently AI)                            │
│  • Data drift monitoring                                             │
│  • Model drift detection                                             │
│  • Automatic retraining trigger                                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼ (if critical drift)
┌─────────────────────────────────────────────────────────────────────┐
│                    AUTO-RETRAINING                                   │
│  • Re-run full pipeline                                              │
│  • Update baseline                                                   │
│  • Deploy new model                                                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  DEPLOYMENT (EKS + ECR)                              │
│  • Canary deployment (30% v2, 70% v1)                               │
│  • Health checks                                                     │
│  • Monitoring (Prometheus + Grafana)                                 │
└─────────────────────────────────────────────────────────────────────┘
```

## 🛠️ Technology Stack

### Data & ETL
- **Storage**: AWS S3 (mlops-movielens-poc)
- **Version Control**: DVC
- **ETL Processing**: Apache Spark (PySpark)
- **Orchestration**: Apache Airflow
- **Validation**: Great Expectations

### ML & Training
- **Framework**: Scikit-learn (NMF)
- **Tracking**: MLflow
- **Registry**: MLflow Model Registry
- **Drift Detection**: Evidently AI

### Infrastructure
- **Compute**: AWS EKS
- **Containers**: Docker + ECR
- **Monitoring**: Prometheus + Grafana
- **CI/CD**: GitHub Actions

## 🚀 Quick Start

### Prerequisites

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install AWS CLI
aws configure

# Install kubectl
# https://kubernetes.io/docs/tasks/tools/

# Install DVC
pip install dvc[s3]
```

### Local Development (Docker Compose)

```bash
# Start all services
docker-compose up -d

# Access services
# - MLflow: http://localhost:5000
# - Airflow: http://localhost:8080 (admin/admin123)
# - API: http://localhost:8000
# - Prometheus: http://localhost:9090
# - Grafana: http://localhost:3000 (admin/admin123)
```

### Run Complete Pipeline Locally

```bash
# Run full ML lifecycle
python run_complete_pipeline.py

# Skip data ingestion (use existing data)
python run_complete_pipeline.py --skip-ingestion

# Force retraining
python run_complete_pipeline.py --force-retrain
```

### Deploy to EKS

```bash
# Build and push images to ECR
bash scripts/aws/build_and_push_to_ecr.sh --region ap-south-1 --version 1.0.0 --push

# Deploy to EKS
bash scripts/aws/deploy_complete_mlops.sh

# Verify deployment
kubectl get pods -n mlops
kubectl get svc -n mlops
```

## 📊 Pipeline Stages

### 1. Data Ingestion (S3 + DVC + PySpark)

```python
# Run ingestion
python src/data_ingestion/run_ingestion.py

# Upload raw data to S3
python scripts/upload_raw_data_to_s3.py --bucket mlops-movielens-poc
```

**Features:**
- ✅ Load raw data from S3
- ✅ Distributed processing with PySpark
- ✅ Data quality transformations
- ✅ Write to S3 in Parquet format
- ✅ DVC version control

### 2. Data Validation (Great Expectations)

```python
# Run validation
python src/data_validation/run_ge_validation.py --stage processed
```

**Features:**
- ✅ Schema validation
- ✅ Data quality checks
- ✅ Statistical profiling
- ✅ HTML reports generation
- ✅ Upload reports to S3

### 3. Feature Engineering

```python
# Run feature engineering
python src/features/feature_engineering.py
```

**Features:**
- ✅ User features (demographics, behavior)
- ✅ Movie features (genres, popularity)
- ✅ Interaction features (temporal, ratings)
- ✅ Save as Parquet and CSV
- ✅ Upload to S3

### 4. Model Training (MLflow + Scikit-learn)

```python
# Train models
python src/training/model_trainer.py

# Model selection
python src/training/model_selection.py
```

**Features:**
- ✅ NMF collaborative filtering
- ✅ MLflow experiment tracking
- ✅ Model registry
- ✅ Artifacts to S3
- ✅ Performance metrics

### 5. Drift Detection (Evidently AI)

```python
# Run drift detection
python src/monitoring/drift_detector.py
```

**Features:**
- ✅ Data drift detection (PSI, KS test)
- ✅ Evidently AI integration
- ✅ Prometheus metrics
- ✅ Auto-retraining on critical drift
- ✅ Drift reports to S3

### 6. Deployment (EKS + Canary)

```bash
# Deploy to EKS
bash scripts/aws/deploy_complete_mlops.sh
```

**Features:**
- ✅ Canary deployment (30% v2, 70% v1)
- ✅ Health checks
- ✅ Prometheus monitoring
- ✅ Grafana dashboards
- ✅ Auto-scaling

## 🔄 Automated Workflows (CI/CD)

### Complete MLOps Pipeline (GitHub Actions)

Workflow: `.github/workflows/complete-mlops-pipeline.yml`

**Triggered on:**
- Push to main/tohid/develop
- Pull requests
- Scheduled (daily 2 AM UTC)

**Stages:**
1. ✅ Code quality & linting
2. ✅ Unit tests & coverage
3. ✅ Data pipeline (S3 + DVC)
4. ✅ Great Expectations validation
5. ✅ Feature engineering
6. ✅ Model training & MLflow registry
7. ✅ Drift detection (Evidently AI)
8. ✅ Auto-retraining (if drift)
9. ✅ Build & push to ECR
10. ✅ Deploy to EKS
11. ✅ Integration tests

## 📈 Monitoring & Observability

### Prometheus Metrics

- `mlops_drift_score` - Current drift score
- `mlops_drift_detected` - Drift detected (0/1)
- `mlops_drift_severity` - Severity level (0=none, 1=warning, 2=critical)
- `mlops_model_predictions_total` - Total predictions
- `mlops_model_latency_seconds` - Prediction latency

### Grafana Dashboards

Access at: `http://localhost:3000` (local) or via EKS LoadBalancer

- **ML Pipeline Dashboard**: Pipeline execution metrics
- **Model Performance**: Prediction latency, throughput
- **Drift Monitoring**: Data/model drift trends
- **Infrastructure**: Resource utilization

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test suite
pytest tests/test_ingestion.py -v
pytest tests/test_validation.py -v
pytest tests/test_phase2.py -v
```

## 📁 Project Structure

```
.
├── airflow/
│   └── dags/
│       ├── mlops_full_pipeline_dag.py    # Complete ML pipeline DAG
│       └── example_dag.py
├── config/
│   ├── data_ingestion_config.yaml        # S3, PySpark config
│   ├── drift_config.yaml                  # Evidently AI config
│   └── ge_validation.yaml                 # Great Expectations config
├── data/
│   ├── raw/                               # Raw data (DVC tracked)
│   ├── processed/                         # Validated data
│   ├── features/                          # Engineered features
│   ├── models/                            # Trained models
│   └── baseline/                          # Baseline for drift detection
├── k8s-minimal/                           # EKS manifests
├── monitoring/
│   ├── prometheus/
│   └── grafana/
├── scripts/aws/
│   ├── build_and_push_to_ecr.sh
│   ├── deploy_complete_mlops.sh
│   └── ...
├── src/
│   ├── data_ingestion/                    # PySpark ETL
│   ├── data_validation/                   # Great Expectations
│   ├── features/                          # Feature engineering
│   ├── training/                          # Scikit-learn + MLflow
│   ├── monitoring/                        # Evidently AI drift
│   └── serving/                           # Model serving API
├── tests/                                 # Unit & integration tests
├── .github/workflows/
│   └── complete-mlops-pipeline.yml        # Full CI/CD
├── docker-compose.yml                     # Local stack
├── run_complete_pipeline.py               # Master orchestrator
└── requirements.txt
```

## 🔐 Configuration

### S3 Bucket (config/data_ingestion_config.yaml)

```yaml
s3:
  enabled: true
  bucket_name: "mlops-movielens-poc"
  region: "ap-south-1"
  raw_data_prefix: "raw/"
  processed_data_prefix: "processed/"
  metadata_prefix: "metadata/"
```

### Drift Detection (config/drift_config.yaml)

```yaml
thresholds:
  warning: 0.15    # Drift score for warning
  critical: 0.25   # Drift score for critical (triggers retraining)

actions:
  on_critical:
    - trigger_retraining
    - send_notification
```

## 📊 Example Usage

### Train a Model

```python
from src.training.model_trainer import RecommendationModelTrainer
import yaml

with open('config/data_ingestion_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

trainer = RecommendationModelTrainer(config)
result = trainer.train_all_models()
print(result)
```

### Check for Drift

```python
from src.monitoring.drift_detector import DriftDetector
import pandas as pd

detector = DriftDetector()

# Load data
baseline, _ = detector.load_baseline("features")
current = pd.read_csv("data/features/interaction_features_latest.csv")

# Detect drift
report = detector.detect_drift(baseline, current)
print(f"Drift detected: {report['drift_detected']}")
print(f"Severity: {report['severity']}")
```

## 🎯 Production Readiness Checklist

- ✅ **Data Management**: S3 storage + DVC version control
- ✅ **ETL**: PySpark distributed processing
- ✅ **Orchestration**: Airflow DAGs for automation
- ✅ **Validation**: Great Expectations for data quality
- ✅ **Feature Store**: Engineered features with S3 storage
- ✅ **Training**: Scikit-learn with MLflow tracking
- ✅ **Registry**: MLflow model registry
- ✅ **Drift Detection**: Evidently AI with auto-retraining
- ✅ **Deployment**: EKS with canary releases
- ✅ **Monitoring**: Prometheus + Grafana
- ✅ **CI/CD**: Automated GitHub Actions pipeline
- ✅ **Testing**: Unit & integration tests
- ✅ **Documentation**: Comprehensive docs

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License.

## 📞 Support

For issues or questions, please open an issue on GitHub or contact the MLOps team.

---

**Built with ❤️ for production-ready ML systems**
