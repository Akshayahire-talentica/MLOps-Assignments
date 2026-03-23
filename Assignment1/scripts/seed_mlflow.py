import os, sys, glob, logging, time
import numpy as np
import pandas as pd
import requests
from pathlib import Path
from sklearn.decomposition import NMF
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

TRACKING_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
FEATURE_DIR   = "data/features"
EXPERIMENT    = "nmf-recommendation-production"
MODEL_NAME    = "nmf_recommendation_production"
MODEL_NAME_V2 = "nmf_recommendation_v2"


def api(_method, _path, **kwargs):
    url = f"{TRACKING_URI}/api/2.0/mlflow/{_path}"
    r = requests.request(_method, url, timeout=30, **kwargs)
    if not r.ok:
        log.error(f"{_method} {url} -> {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    return r.json()


def get_or_create_experiment(name):
    try:
        data = api("GET", "experiments/get-by-name", params={"experiment_name": name})
        eid = data["experiment"]["experiment_id"]
        log.info(f"Using existing experiment id={eid}")
        return eid
    except Exception:
        data = api("POST", "experiments/create", json={"name": name})
        eid = data["experiment_id"]
        log.info(f"Created experiment id={eid}")
        return eid


def create_run(experiment_id, run_name, params, metrics, tags):
    ts = int(time.time() * 1000)
    run = api("POST", "runs/create", json={
        "experiment_id": experiment_id,
        "run_name": run_name,
        "start_time": ts,
        "tags": [{"key": key, "value": str(val)} for key, val in tags.items()],
    })
    run_id = run["run"]["info"]["run_id"]
    api("POST", "runs/log-batch", json={
        "run_id": run_id,
        "params": [{"key": key, "value": str(val)} for key, val in params.items()],
        "metrics": [{"key": key, "value": float(val), "timestamp": ts, "step": 0}
                    for key, val in metrics.items()],
        "tags": [],
    })
    api("POST", "runs/update", json={"run_id": run_id, "status": "FINISHED",
                                      "end_time": int(time.time() * 1000)})
    log.info(f"  run_id={run_id}")
    return run_id


def register_model(name, run_id):
    try:
        api("POST", "registered-models/create", json={"name": name})
    except Exception:
        pass
    try:
        data = api("POST", "model-versions/create", json={
            "name": name, "source": f"runs://{run_id}/model", "run_id": run_id,
        })
        version = data["model_version"]["version"]
        log.info(f"  Registered {name} version {version}")
        try:
            api("POST", "model-versions/transition-stage", json={
                "name": name, "version": version,
                "stage": "Production", "archive_existing_versions": True,
            })
            log.info(f"  -> Promoted to Production")
        except Exception as ex:
            log.warning(f"  Stage transition skipped: {ex}")
    except Exception as ex2:
        log.warning(f"  Model version skipped: {ex2}")


def build_matrix(df):
    user_col   = next((c for c in df.columns if "user" in c.lower()), df.columns[0])
    movie_col  = next((c for c in df.columns if "movie" in c.lower() or "item" in c.lower()), df.columns[1])
    rating_col = next((c for c in df.columns if "rating" in c.lower()), None)
    if not rating_col:
        rating_col = df.select_dtypes(include=[np.number]).columns[0]
    log.info(f"  pivot: user={user_col}, movie={movie_col}, rating={rating_col}")
    mat = df.pivot_table(index=user_col, columns=movie_col, values=rating_col, fill_value=0)
    return mat.values.astype(np.float32)


def train_nmf(matrix, n_components=20, random_state=42):
    n = matrix.shape[0]
    tr_idx, te_idx = train_test_split(range(n), test_size=0.2, random_state=random_state)
    model = NMF(n_components=n_components, max_iter=200, random_state=random_state,
                init="nndsvda", solver="mu")
    model.fit_transform(matrix[tr_idx, :])
    W_te  = model.transform(matrix[te_idx, :])
    recon = np.clip(np.dot(W_te, model.components_), 0, 5)
    mask  = matrix[te_idx, :] > 0
    if mask.sum() == 0:
        mask = np.ones_like(matrix[te_idx, :], dtype=bool)
    y_true = matrix[te_idx, :][mask]
    y_pred = recon[mask]
    return {
        "rmse":          round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 4),
        "mae":           round(float(mean_absolute_error(y_true, y_pred)), 4),
        "coverage":      round(float(np.mean(recon > 0)), 4),
        "sparsity":      round(float(1 - np.count_nonzero(matrix[tr_idx,:]) / matrix[tr_idx,:].size), 4),
        "n_users":       float(len(tr_idx)),
        "n_movies":      float(matrix.shape[1]),
        "training_loss": round(float(model.reconstruction_err_), 4),
    }


def main():
    os.chdir(Path(__file__).parent.parent)
    log.info(f"MLflow tracking URI: {TRACKING_URI}")
    try:
        r = requests.get(f"{TRACKING_URI}/health", timeout=5)
        log.info(f"Server health: {r.text.strip()}")
    except Exception as err:
        log.error(f"Cannot reach MLflow at {TRACKING_URI}: {err}")
        sys.exit(1)

    files = sorted(glob.glob(f"{FEATURE_DIR}/interaction_features_*.parquet"))
    if not files:
        log.error(f"No parquet files in {FEATURE_DIR}/")
        sys.exit(1)
    df = pd.read_parquet(files[-1])
    log.info(f"Loaded {files[-1]}  shape={df.shape}")

    matrix = build_matrix(df)
    log.info(f"Rating matrix {matrix.shape}  non-zero={np.count_nonzero(matrix)}")

    eid = get_or_create_experiment(EXPERIMENT)

    log.info("Training NMF v1 (n_components=20)...")
    m1 = train_nmf(matrix, n_components=20, random_state=42)
    run_id_v1 = create_run(eid, "nmf_v1_production",
        params={"n_components": 20, "max_iter": 200, "solver": "mu",
                "init": "nndsvda", "random_state": 42, "model_version": "v1"},
        metrics=m1,
        tags={"model_type": "NMF", "stage": "production", "version": "v1",
              "dataset": "movielens", "mlflow.runName": "nmf_v1_production"})
    log.info(f"V1 metrics: {m1}")
    register_model(MODEL_NAME, run_id_v1)

    log.info("Training NMF v2 (n_components=30)...")
    m2 = train_nmf(matrix, n_components=30, random_state=0)
    run_id_v2 = create_run(eid, "nmf_v2_canary",
        params={"n_components": 30, "max_iter": 200, "solver": "mu",
                "init": "nndsvda", "random_state": 0, "model_version": "v2"},
        metrics=m2,
        tags={"model_type": "NMF", "stage": "canary", "version": "v2",
              "dataset": "movielens", "mlflow.runName": "nmf_v2_canary"})
    log.info(f"V2 metrics: {m2}")
    register_model(MODEL_NAME_V2, run_id_v2)

    log.info("Done! Open http://localhost:5000 to verify.")


if __name__ == "__main__":
    main()
