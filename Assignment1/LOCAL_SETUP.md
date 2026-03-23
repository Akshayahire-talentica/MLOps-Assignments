# Local Setup — MLOps MovieLens POC

This guide explains how to run the entire MLOps MovieLens PoC locally (macOS / Linux) using Docker Compose. It assumes you have the repository checked out.

## Prerequisites

- macOS or Linux with Docker and Docker Compose (Docker Desktop recommended)
- Python 3.10+ (for running helper scripts locally)
- AWS credentials with access to the S3 bucket used by the project (or set up an S3-compatible endpoint)
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION` (default: `ap-south-1`)
- At least 8 GB free RAM (more recommended for PySpark/MLflow)

## Clone repository

```bash
git clone <repo-url> mlops-poc
cd mlops-poc
```

## Environment

Create an `.env` (or export environment variables) with the following keys (example):

```bash
export AWS_ACCESS_KEY_ID=YOURKEY
export AWS_SECRET_ACCESS_KEY=YOURSECRET
export AWS_DEFAULT_REGION=ap-south-1
export S3_BUCKET=mlops-movielens-poc
```

On macOS with Docker Desktop, ensure Docker has enough CPUs and memory (at least 4 CPUs, 8GB RAM).

## Project layout (important paths)

- `docker-compose.yml` — compose stack to run all services locally
- `src/` — application code, Streamlit UI under `src/ui`
- `data/` — local dataset and generated artifacts (mounted into containers)
- `airflow/dags/` — DAG definitions

## Notes on S3

This project uses real S3 for artifacts (MLflow). If you prefer to use a local S3-compatible store, update `docker-compose.yml` and service env vars accordingly.

## Helpful commands (local)

- Build and start services:

```bash
# Build & run all services in background
docker compose up -d --build

# Start only specific service
docker compose up -d --build streamlit-ui
```

- View logs:

```bash
docker compose logs --follow streamlit-ui
docker compose logs --follow mlflow
docker compose logs --follow airflow-webserver
```

- Restart a service:

```bash
docker compose restart streamlit-ui
```

## Where to look if something fails

- Streamlit UI: http://localhost:8501
- MLflow UI: http://localhost:5000
- Airflow UI: http://localhost:8080
- API service: http://localhost:8000
- Model v2: http://localhost:8001

## Next steps

Follow `QUICKSTART.md` for the first run and `TROUBLESHOOTING.md` for common issues.
