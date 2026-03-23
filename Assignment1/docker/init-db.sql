-- This script runs automatically on first postgres start (empty data dir).
-- POSTGRES_DB (mlops_db) is already created by the docker entrypoint for MLflow.
-- Create a separate database for Airflow to avoid alembic version conflicts.
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO mlops;
