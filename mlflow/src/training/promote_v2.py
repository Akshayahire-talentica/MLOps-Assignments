"""
Promote best nmf_recommendation_v2 run to MLflow Registry 'Staging'.

This script finds the best run in experiment 'phase3-training' by
`metrics.reconstruction_rmse` (lower is better), locates the registered
model version that references that run, and transitions it to 'Staging'.

Usage: python3 src/training/promote_v2.py
"""

import logging
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def promote_best_v2_model(experiment_name='phase3-training', model_name='nmf_recommendation_v2'):
    client = MlflowClient()

    # Find experiment
    exp = client.get_experiment_by_name(experiment_name)
    if not exp:
        raise RuntimeError(f"Experiment '{experiment_name}' not found")

    # Search runs in experiment
    runs = client.search_runs([exp.experiment_id], order_by=['metrics.reconstruction_rmse ASC'])
    if not runs:
        raise RuntimeError("No runs found in experiment")

    best_run = runs[0]
    run_id = best_run.info.run_id
    rmse = best_run.data.metrics.get('reconstruction_rmse')

    logger.info(f"Best run: {run_id} with reconstruction_rmse={rmse}")

    # Find registered model versions for the model name
    versions = client.get_latest_versions(model_name)
    target_version = None
    for v in versions:
        if v.run_id == run_id:
            target_version = v.version
            break

    if not target_version:
        # try searching all versions for matching run_id
        all_versions = client.search_model_versions(f"name='{model_name}'")
        for v in all_versions:
            if getattr(v, 'run_id', None) == run_id:
                target_version = v.version
                break

    if not target_version:
        raise RuntimeError(f"No registered model version found for run {run_id}")

    logger.info(f"Promoting model {model_name} version {target_version} to 'Staging'")
    client.transition_model_version_stage(name=model_name, version=target_version, stage='Staging', archive_existing_versions=True)

    logger.info("Promotion complete")


if __name__ == '__main__':
    promote_best_v2_model()
