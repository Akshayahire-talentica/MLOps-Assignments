# MLOps MovieLens PoC

This repository contains a local, self-hosted MLOps proof-of-concept for a MovieLens recommendation pipeline. It integrates S3, MLflow, Airflow, PySpark ETL, feature engineering, model training, and a Streamlit dashboard.

This README consolidates the project's quickstart, environment setup, common troubleshooting, and development pointers. For detailed guides see `LOCAL_SETUP.md`, `QUICKSTART.md`, and `TROUBLESHOOTING.md`.

---

## What this project includes

- End-to-end pipeline DAG: `airflow/dags/mlops_full_pipeline_dag.py`
- Streamlit UI: `src/ui/streamlit_app.py` and integration helper `src/ui/real_mlops_integration.py`
- MLflow server and registration scripts: `scripts/seed_mlflow.py`
- Docker Compose stack to run locally: `docker-compose.yml`
- Local data mount for features and movies: `data/` (pre-built parquet files included)

---

## One-line Quickstart

1. Ensure AWS credentials and `S3_BUCKET` environment variables are set.
2. Build and run the stack: `docker compose up -d --build`
3. (Optional) Seed MLflow: `python3 scripts/seed_mlflow.py`
4. Open Streamlit: http://localhost:8501 — verify metrics and pipeline control.

See `QUICKSTART.md` for step-by-step commands.

---

## Prerequisites

- Docker & Docker Compose (Docker Desktop recommended on macOS)
- Python 3.10+ (for helper scripts)
- AWS credentials with access to the S3 bucket (or an S3-compatible endpoint)
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION` (default: `ap-south-1`)

Set these via an `.env` file or export before launching the compose stack.

---

## Expected behavior after startup

When the stack and MLflow seeder run successfully, the Streamlit dashboard should show:

- Movies ≈ 3,883
- Ratings ≈ 1,000,209
- Unique Users ≈ 6,040
- Model metrics (if `scripts/seed_mlflow.py` was run)

If counts are wrong (e.g., Ratings ≈ 1,000), see the troubleshooting section below.

---

## Important implementation notes

- Ratings loading: `src/ui/real_mlops_integration.py::load_processed_ratings()` prefers the local `data/features/interaction_features_*.parquet` (fast, full dataset). If local files are missing, it falls back to S3 and reads the largest directory of parquet parts to avoid tiny Spark partitions.

- MLflow compatibility: this project targets MLflow server v2.9.2. The code uses `POST /api/2.0/mlflow/experiments/search` and `POST /api/2.0/mlflow/registered-models/search` endpoints to avoid `404`/`405` issues with legacy endpoints.

- Drift detection: the pipeline uses a SciPy KS-test in the DAG to handle Evidently version mismatches in some containers.

- Airflow credentials in `docker-compose.yml` default to `admin` / `admin123`.

---

## Where to look and edit during development

- Streamlit UI and integration: `src/ui/streamlit_app.py`, `src/ui/real_mlops_integration.py`
- Airflow DAG: `airflow/dags/mlops_full_pipeline_dag.py`
- MLflow seeder: `scripts/seed_mlflow.py`
- Docker Compose: `docker-compose.yml`

---

## Common commands

```bash
# Build & start the full stack
docker compose up -d --build

# Rebuild & restart Streamlit only
docker compose up -d --build streamlit-ui

# Tail Streamlit logs
docker compose logs --follow streamlit-ui

# Seed MLflow (host)
python3 scripts/seed_mlflow.py

# Trigger pipeline via Airflow (inside webserver container)
docker compose exec airflow-webserver airflow dags trigger mlops_full_pipeline
```

---

## Quick troubleshooting highlights

- Streamlit shows only ~1,000 ratings: ensure `data/features/interaction_features_*.parquet` is present and mounted in the container. The UI prefers this local file for the full dataset.
- MLflow 404/405: ensure the MLflow server is healthy and `MLFLOW_TRACKING_URI` points to it. The UI uses `search` POST endpoints for compatibility.
- Airflow 401: confirm `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` or use the compose defaults.

See `TROUBLESHOOTING.md` for more detail.

---

## Where to get help

Collect logs and open an issue if stuck:

- `docker compose ps`
- `docker compose logs --tail=200 streamlit-ui mlflow airflow-webserver api-service`
- Explain observed vs expected metrics

---

If you'd like, I can add `CONTRIBUTING.md` and a `Makefile` with common developer tasks next.
# airflow-on-eks-manual-gitops

# 📘 Airflow on EKS with Helm (Manual GitOps)

This project demonstrates how to deploy **Apache Airflow** on an **Amazon EKS** (Elastic Kubernetes Service) cluster using **Helm** and a **manual GitOps approach** (without Argo CD or Terraform). It's a great way to learn DevOps for data-oriented workflows in Kubernetes.

---

## 🚀 Features

- Apache Airflow deployment on EKS
- PostgreSQL as Airflow's metadata DB
- DAGs managed via ConfigMap (manual GitOps)
- Helm-based component installation
- YAML manifests to manage all Kubernetes objects
- Airflow UI exposed via LoadBalancer

---

## 🧰 Tech Stack

- Amazon EKS (via `eksctl`)
- Kubernetes (`kubectl`)
- Helm
- Apache Airflow
- PostgreSQL
- Git (manual GitOps)
- VS Code (dev environment)

---

## 🧱 Directory Structure

airflow-on-eks-manual-gitops/
├── dags/
│ └── example_dag.py
├── manifests/
│ ├── airflow-init-job.yaml
│ ├── airflow-scheduler-deployment.yaml
│ ├── airflow-web-deployment.yaml
│ ├── airflow-web-service.yaml
│ ├── airflow-dags-configmap.yaml
│ ├── configmap.yaml
│ ├── secret.yaml
│ ├── postgres-deployment.yaml
│ └── postgres-service.yaml


---

## 📦 Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [eksctl](https://eksctl.io/)
- [Helm](https://helm.sh/)
- AWS account with IAM permissions to create EKS clusters

---

## ✅ Setup Guide

### 1. Create EKS Cluster

eksctl create cluster --name airflow-cluster --region ap-south-1 --nodes 2

## 2. Deploy PostgreSQL

kubectl apply -f manifests/postgres-deployment.yaml
kubectl apply -f manifests/postgres-service.yaml

## 3. Apply ConfigMap & Secrets

kubectl apply -f manifests/configmap.yaml
kubectl apply -f manifests/secret.yaml

## 4. Run Airflow Init Job

kubectl apply -f manifests/airflow-init-job.yaml

## 5. Deploy Airflow Web + Scheduler

kubectl apply -f manifests/airflow-web-deployment.yaml
kubectl apply -f manifests/airflow-scheduler-deployment.yaml
kubectl apply -f manifests/airflow-web-service.yaml

## 📁 DAG Deployment (Manual GitOps Style)

Add/Update your DAGs in the dags/ directory

Recreate the ConfigMap:

kubectl delete configmap airflow-dags -n airflow
kubectl create configmap airflow-dags --from-file=dags/ -n airflow

Restart Deployments:

kubectl rollout restart deployment airflow-web -n airflow
kubectl rollout restart deployment airflow-scheduler -n airflow

---

## 🔐 Airflow Login

Credentials are set via the Airflow init job.

🌐 Access Airflow Web UI
Get the LoadBalancer URL:

kubectl get svc airflow-web -n airflow
Open the EXTERNAL-IP in your browser with port 8080.

## 💡 Learning Outcomes
Manual GitOps using Kubernetes + Git

Airflow deployment in real-world cloud infra

Use of ConfigMaps, Secrets, Jobs, Services, and Deployments

Kubernetes YAML authoring from scratch

Understanding DAG deployment methods

## 🔧 Enhancements (Optional)

DAG sync using Git sidecar container

Use S3 or PVC for DAG/log persistence

CI/CD with GitHub Actions

Ingress + cert-manager for HTTPS

Monitoring with Prometheus + Grafana
---
