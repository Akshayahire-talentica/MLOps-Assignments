# Quickstart — Run the MLOps MovieLens POC Locally

This quickstart walks you through bringing up the stack, seeding MLflow with demo runs, and verifying the Streamlit UI.

## 1) Prepare environment

Ensure you exported AWS creds and bucket name (see `LOCAL_SETUP.md`).

```bash
export AWS_ACCESS_KEY_ID=YOURKEY
export AWS_SECRET_ACCESS_KEY=YOURSECRET
export AWS_DEFAULT_REGION=ap-south-1
export S3_BUCKET=mlops-movielens-poc
```

## 2) Build & start the stack

```bash
# From project root
docker compose up -d --build
```

Wait until `mlflow`, `postgres` and `api-service` are healthy. Use `docker compose ps` to check.

## 3) Seed MLflow (optional but recommended)

The repo contains a `scripts/seed_mlflow.py` to create experiments, runs, and register models via MLflow REST API.

```bash
# Run from repo root (in your host python env)
python3 scripts/seed_mlflow.py
```

This will create experiments and registered models used by the Streamlit UI.

## 4) Open UIs

- Streamlit: http://localhost:8501 — Dashboard, Pipeline Control, Monitoring
- MLflow: http://localhost:5000 — Experiments & registered models
- Airflow: http://localhost:8080 — DAG graph & logs

## 5) Trigger a pipeline run

From the Streamlit UI -> "Pipeline Control" tab -> "Run Full Pipeline" or from Airflow UI trigger the `mlops_full_pipeline` DAG.

## 6) Verify data in Streamlit

- Sidebar metrics should show:
  - Movies ≈ 3,883
  - Ratings ≈ 1,000,209
  - Users ≈ 6,040

If you see smaller numbers, restart Streamlit and ensure the `data/features` parquet files are present (mounted at `/app/data/features` in container).

## Useful commands

```bash
# Tail logs
docker compose logs --follow streamlit-ui

# Rebuild + restart one service
docker compose up -d --build streamlit-ui

# Run the pipeline (Airflow)
docker compose exec airflow-webserver airflow dags trigger mlops_full_pipeline
```

## Troubleshooting

See `TROUBLESHOOTING.md` for quick fixes (MLflow endpoints, ratings partition issues, cache). If problems persist, check container logs and open an issue in the repo.
