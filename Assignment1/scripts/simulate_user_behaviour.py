"""
scripts/simulate_user_behaviour.py
===================================
Simulates realistic user interactions against the production recommendation API,
measures prediction quality vs. ground-truth ratings, and demonstrates a
progressive **model performance degradation** by injecting noise.

Three modes
-----------
1. baseline     — compute real RMSE/MAE from ground-truth ratings, log to MLflow
2. degrade      — repeat with increasing noise levels to mimic model drift
3. full         — run baseline then 5 degraded epochs back-to-back

Usage
-----
    python scripts/simulate_user_behaviour.py --mode full   # recommended
    python scripts/simulate_user_behaviour.py --mode baseline
    python scripts/simulate_user_behaviour.py --mode degrade --epochs 5 --noise-start 0.3 --noise-step 0.25

Environment variables (or .env)
---------------------------------
    API_URL              http://localhost:8000
    MLFLOW_TRACKING_URI  http://localhost:5000
    S3_BUCKET            mlops-movielens-poc
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import mlflow
import numpy as np
import pandas as pd
import requests
from sklearn.metrics import mean_absolute_error, mean_squared_error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("simulate")

# ─── Config ──────────────────────────────────────────────────────────────────
API_URL = os.getenv("API_URL", "http://localhost:8000")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
S3_BUCKET = os.getenv("S3_BUCKET", "mlops-movielens-poc")
DATA_DIR = Path(__file__).parent.parent / "data"
EXPERIMENT_NAME = "nmf-recommendation-production"

# How many user interactions to simulate per epoch
USERS_PER_RUN = 200
RATINGS_PER_USER = 5


# ─── Helpers ─────────────────────────────────────────────────────────────────

def load_ground_truth(n_users: int = USERS_PER_RUN, seed: int = 42) -> pd.DataFrame:
    """Sample real MovieLens interactions from the local feature parquet."""
    parquet_files = sorted(
        (DATA_DIR / "features").glob("interaction_features_*.parquet"), reverse=True
    )
    if not parquet_files:
        raise FileNotFoundError(
            f"No interaction_features_*.parquet found in {DATA_DIR / 'features'}"
        )
    df = pd.read_parquet(parquet_files[0])

    # Normalise column names
    df.columns = [c.strip() for c in df.columns]
    user_col   = next((c for c in df.columns if c.lower() == "userid"),   "UserID")
    movie_col  = next((c for c in df.columns if c.lower() == "movieid"),  "MovieID")
    rating_col = next((c for c in df.columns if c.lower() == "rating"),   "Rating")

    rng = random.Random(seed)
    all_users = df[user_col].unique().tolist()
    selected  = rng.sample(all_users, min(n_users, len(all_users)))

    rows = []
    for uid in selected:
        user_df = df[df[user_col] == uid]
        sample  = user_df.sample(
            min(RATINGS_PER_USER, len(user_df)), random_state=seed
        )
        for _, row in sample.iterrows():
            rows.append(
                {
                    "user_id":        int(row[user_col]),
                    "movie_id":       int(row[movie_col]),
                    "actual_rating":  float(row[rating_col]),
                    "user_avg_rating": float(row.get("UserAvgRating", row.get("useravgrating", 3.5))),
                    "user_rating_count": int(row.get("UserRatingCount", row.get("userratingcount", 50))),
                    "movie_avg_rating":  float(row.get("MovieAvgRating", row.get("movieavgrating", 3.5))),
                    "movie_popularity":  float(row.get("MoviePopularity", row.get("moviepopularity", 1.0))),
                }
            )

    gt = pd.DataFrame(rows)
    log.info(f"Ground truth: {len(gt)} interactions from {gt['user_id'].nunique()} users")
    return gt


def call_predict(row: dict, noise: float = 0.0) -> Optional[float]:
    """Hit the /predict endpoint and optionally add Gaussian noise to mimic drift."""
    try:
        resp = requests.post(
            f"{API_URL}/predict",
            json={
                "user_id":            row["user_id"],
                "movie_id":           row["movie_id"],
                "user_avg_rating":    row["user_avg_rating"],
                "user_rating_count":  row["user_rating_count"],
                "movie_avg_rating":   row["movie_avg_rating"],
                "movie_popularity":   row["movie_popularity"],
                "day_of_week":        datetime.now().weekday(),
                "month":              datetime.now().month,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            pred = float(resp.json()["predicted_rating"])
            if noise > 0:
                pred += np.random.normal(0, noise)          # inject degradation
            pred = max(0.5, min(5.0, pred))
            return pred
    except Exception as e:
        log.warning(f"predict call failed for user={row['user_id']}: {e}")
    return None


def evaluate(gt: pd.DataFrame, noise: float = 0.0) -> Tuple[pd.DataFrame, Dict]:
    """Call /predict for every row in gt and compute RMSE/MAE/coverage."""
    results = []
    t0 = time.time()

    for _, row in gt.iterrows():
        pred = call_predict(row.to_dict(), noise=noise)
        results.append(
            {
                "user_id":          row["user_id"],
                "movie_id":         row["movie_id"],
                "actual_rating":    row["actual_rating"],
                "predicted_rating": pred,
            }
        )

    elapsed = time.time() - t0
    df = pd.DataFrame(results)
    df_valid = df.dropna(subset=["predicted_rating"])

    coverage = len(df_valid) / len(df) if len(df) > 0 else 0.0

    if len(df_valid) >= 2:
        mse  = mean_squared_error(df_valid["actual_rating"], df_valid["predicted_rating"])
        rmse = float(np.sqrt(mse))
        mae  = float(mean_absolute_error(df_valid["actual_rating"], df_valid["predicted_rating"]))
        errors = (df_valid["actual_rating"] - df_valid["predicted_rating"]).abs()
        p95    = float(errors.quantile(0.95))
        hit3   = float((df_valid["predicted_rating"] >= df_valid["actual_rating"] - 1).mean())
    else:
        rmse = mae = p95 = 0.0
        hit3 = 0.0

    avg_latency = elapsed / len(gt) if len(gt) > 0 else 0.0

    metrics = {
        "rmse":             round(rmse,       4),
        "mae":              round(mae,        4),
        "coverage":         round(coverage,   4),
        "p95_abs_error":    round(p95,        4),
        "hit_within_1_star":round(hit3,       4),
        "n_requests":       len(gt),
        "n_successful":     len(df_valid),
        "avg_latency_s":    round(avg_latency, 4),
        "noise_level":      round(noise,      4),
    }
    return df, metrics


def log_to_mlflow(metrics: Dict, run_name: str, tags: Dict = None) -> str:
    """Log a simulation epoch to MLflow and return the run_id."""
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=run_name, tags=tags or {}) as run:
        mlflow.log_metrics(metrics)
        mlflow.set_tag("source", "simulate_user_behaviour")
        mlflow.set_tag("simulation_ts", datetime.now(timezone.utc).isoformat())
        log.info(
            f"MLflow run logged → {run.info.run_id}  "
            f"RMSE={metrics['rmse']:.4f}  MAE={metrics['mae']:.4f}  "
            f"coverage={metrics['coverage']:.2%}  noise={metrics['noise_level']:.2f}"
        )
        return run.info.run_id


def upload_drift_report(report: Dict, prefix: str = "reports/drift") -> None:
    """Write a JSON drift report to S3 (best-effort)."""
    try:
        import boto3

        s3 = boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
        )
        ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"{prefix}/drift_report_{ts}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(report, indent=2).encode(),
            ContentType="application/json",
        )
        # Also overwrite the "latest" key so Streamlit always reads the freshest
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/drift_report_latest.json",
            Body=json.dumps(report, indent=2).encode(),
            ContentType="application/json",
        )
        log.info(f"Drift report uploaded → s3://{S3_BUCKET}/{key}")
    except Exception as e:
        log.warning(f"S3 upload failed (non-fatal): {e}")


# ─── Main routines ────────────────────────────────────────────────────────────

def run_baseline(seed: int = 42) -> Tuple[Dict, str]:
    """One clean baseline epoch — no noise."""
    log.info("=== BASELINE epoch ===")
    gt = load_ground_truth(seed=seed)
    _, metrics = evaluate(gt, noise=0.0)
    run_id = log_to_mlflow(metrics, run_name="simulation_baseline",
                           tags={"epoch": "baseline", "noise": "0.0"})
    log.info(f"Baseline  RMSE={metrics['rmse']:.4f}  MAE={metrics['mae']:.4f}")
    return metrics, run_id


def run_degradation(
    epochs: int = 5,
    noise_start: float = 0.3,
    noise_step: float = 0.25,
    seed: int = 99,
) -> List[Dict]:
    """
    Run `epochs` degraded evaluation rounds.
    Each epoch adds more Gaussian noise to the predictions to mimic
    a model that has drifted away from ground truth.
    """
    log.info(f"=== DEGRADATION simulation: {epochs} epochs, "
             f"noise {noise_start:.2f} → {noise_start + noise_step * (epochs-1):.2f} ===")

    history: List[Dict] = []
    noise = noise_start

    for epoch in range(1, epochs + 1):
        log.info(f"  Epoch {epoch}/{epochs}  noise={noise:.2f}")
        gt = load_ground_truth(seed=seed + epoch)
        _, metrics = evaluate(gt, noise=noise)
        run_id = log_to_mlflow(
            metrics,
            run_name=f"simulation_degraded_ep{epoch:02d}",
            tags={"epoch": str(epoch), "noise": str(round(noise, 2)), "type": "degraded"},
        )
        metrics["run_id"] = run_id
        history.append(metrics)
        noise = round(noise + noise_step, 3)

    return history


def build_drift_report(baseline: Dict, history: List[Dict]) -> Dict:
    """Compose a drift report readable by the Streamlit monitoring tab."""
    if not history:
        return {}

    worst = history[-1]
    baseline_rmse  = baseline.get("rmse", 0)
    current_rmse   = worst.get("rmse", 0)
    rmse_delta     = current_rmse - baseline_rmse
    drift_score    = min(1.0, max(0.0, rmse_delta / (baseline_rmse + 1e-9)))
    drift_detected = drift_score > 0.15      # >15% RMSE increase

    return {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "source":            "simulate_user_behaviour",
        "drift_detected":    drift_detected,
        "drift_score":       round(drift_score, 4),
        "baseline_rmse":     round(baseline_rmse, 4),
        "current_rmse":      round(current_rmse, 4),
        "rmse_increase_pct": round(rmse_delta / (baseline_rmse + 1e-9) * 100, 2),
        "epochs":            len(history),
        "recommendation":    (
            "⚠️ Model performance degraded — retraining recommended."
            if drift_detected else
            "✅ Model performance within acceptable bounds."
        ),
        "epoch_history": [
            {k: v for k, v in ep.items() if k not in ("run_id",)}
            for ep in history
        ],
    }


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Simulate user behaviour & model performance")
    p.add_argument("--mode", choices=["baseline", "degrade", "full"], default="full",
                   help="baseline=clean eval; degrade=noise injection; full=both (default)")
    p.add_argument("--epochs",      type=int,   default=5,    help="Degradation epochs")
    p.add_argument("--noise-start", type=float, default=0.30, help="Initial noise std")
    p.add_argument("--noise-step",  type=float, default=0.25, help="Noise increase per epoch")
    p.add_argument("--seed",        type=int,   default=42,   help="Random seed")
    return p.parse_args()


def main():
    args = parse_args()

    # ── Verify API is reachable ───────────────────────────────────────────────
    try:
        health = requests.get(f"{API_URL}/health", timeout=5).json()
        log.info(f"API health: {health.get('status')}  model_loaded={health.get('model_loaded')}")
    except Exception as e:
        log.error(f"Cannot reach API at {API_URL}: {e}")
        log.error("Start services with:  docker compose up -d api-service")
        raise SystemExit(1)

    baseline_metrics: Dict = {}
    degrade_history:  List = []

    if args.mode in ("baseline", "full"):
        baseline_metrics, _ = run_baseline(seed=args.seed)

    if args.mode in ("degrade", "full"):
        degrade_history = run_degradation(
            epochs=args.epochs,
            noise_start=args.noise_start,
            noise_step=args.noise_step,
            seed=args.seed + 10,
        )

    # ── Drift report ─────────────────────────────────────────────────────────
    if baseline_metrics or degrade_history:
        report = build_drift_report(baseline_metrics, degrade_history)
        if report:
            print("\n" + "=" * 60)
            print(json.dumps(report, indent=2))
            print("=" * 60)
            upload_drift_report(report)

    # ── Summary table ─────────────────────────────────────────────────────────
    if degrade_history:
        summary = pd.DataFrame(degrade_history)[
            ["noise_level", "rmse", "mae", "coverage", "hit_within_1_star"]
        ]
        if baseline_metrics:
            base_row = {
                "noise_level": 0.0,
                "rmse":        baseline_metrics["rmse"],
                "mae":         baseline_metrics["mae"],
                "coverage":    baseline_metrics["coverage"],
                "hit_within_1_star": baseline_metrics["hit_within_1_star"],
            }
            summary = pd.concat([pd.DataFrame([base_row]), summary], ignore_index=True)

        print("\n📊 Performance degradation summary:")
        print(summary.to_string(index=False, float_format="{:.4f}".format))
        print("\n✅ All runs logged to MLflow →", MLFLOW_URI)
        print("   Drift report written to S3 → reports/drift/drift_report_latest.json")
        print("   Refresh the Streamlit Monitoring tab to see the degradation trend.")


if __name__ == "__main__":
    main()
