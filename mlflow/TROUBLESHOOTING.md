# Troubleshooting — Common Problems & Fixes

This file lists common problems you may encounter when running the stack locally and recommended fixes.

## 1) Streamlit shows only ~1,000 ratings (or small counts)

Cause: Spark writes partitioned output to S3; older code picked a single latest partition file (often tiny). Fixes:

- Confirm `data/features/interaction_features_*.parquet` exists locally. The UI prefers this local parquet (fast, full dataset).

```bash
# Inside streamlit container
docker compose exec -T streamlit-ui ls -la /app/data/features
```

- If you rely on S3, ensure the S3 prefix `processed/ratings/` contains the full dataset. The UI was changed to read the largest directory of parquet parts rather than a single latest file.

## 2) MLflow API returns 404 or 405

Symptoms: Logs show `MLflow API returned 404` or `registered-models/search returned 405`.

Fixes:

- MLflow server version differences (server v2.9.2) require using `/experiments/search` and `/registered-models/search` POST endpoints instead of older `list` endpoints. The code in `src/ui/real_mlops_integration.py` already uses `search` for experiments and registered models.

- If you still see 405, verify the container `MLFLOW_TRACKING_URI` is correct and that MLflow server is healthy:

```bash
docker compose ps
docker compose logs --follow mlflow
curl -sSf http://localhost:5000/health
```

## 3) Airflow authentication failed (401)

- Default credentials in compose: `admin` / `admin123`. Confirm environment or `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` if changed.
- Use Airflow web UI logs for more detail:

```bash
docker compose logs --follow airflow-webserver
```

## 4) Streamlit cache shows stale data

- Clear cached resources and restart Streamlit from the UI or host:

```bash
# Restart container from host
docker compose restart streamlit-ui
# Or clear cache from UI: "Refresh Data" button triggers cache clear
```

## 5) Postgres connection or DB initialization errors

- Ensure `postgres` container is healthy before `mlflow` and `airflow` start; compose uses healthcheck and `depends_on`.
- Inspect `docker/init-db.sql` if DB schema issues appear.

## 6) Files not mounted into containers

- Confirm volumes in `docker-compose.yml` point to the expected host paths (e.g., `./data:/app/data` and `./src/ui:/app/ui` for Streamlit).
- Permissions: on macOS, Docker Desktop may require file sharing permissions for the project folder.

## 7) Where to find logs

- Streamlit: `docker compose logs --follow streamlit-ui`
- MLflow: `docker compose logs --follow mlflow`
- Airflow: `docker compose logs --follow airflow-webserver` and `airflow scheduler`
- API: `docker compose logs --follow api-service`

## 8) Want to run only a subset of services

Start minimal services (MLflow + API + Streamlit):

```bash
docker compose up -d --build mlflow api-service streamlit-ui
```

## 9) Still stuck?

Collect logs and open an issue with:

- `docker compose ps`
- `docker compose logs --tail=200 streamlit-ui mlflow airflow-webserver api-service`
- Description of observed vs expected UI metrics

Include these outputs in a GitHub issue for faster help.
