"""
Create a fresh registered model from the best v2 run and promote it to Staging.

This avoids mutating existing registry metadata that previously failed
when transitioning stages in the file-backed registry.
"""

import time
import logging
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(experiment_name='phase3-training', source_model_name='nmf_recommendation_v2'):
    client = MlflowClient()

    # Find experiment
    exp = client.get_experiment_by_name(experiment_name)
    if not exp:
        raise RuntimeError(f"Experiment '{experiment_name}' not found")

    # Find best run by reconstruction_rmse
    runs = client.search_runs([exp.experiment_id], order_by=['metrics.reconstruction_rmse ASC'], max_results=10)
    if not runs:
        raise RuntimeError("No runs found in experiment")

    best_run = runs[0]
    run_id = best_run.info.run_id
    rmse = best_run.data.metrics.get('reconstruction_rmse')
    logger.info(f"Best run: {run_id} (rmse={rmse})")

    model_uri = f"runs:/{run_id}/model"

    new_model_name = source_model_name + "_copy"
    # Create a fresh registered model name if not exists
    try:
        client.create_registered_model(new_model_name)
        logger.info(f"Created new registered model: {new_model_name}")
    except Exception:
        logger.info(f"Registered model {new_model_name} may already exist; continuing")

    # Create a new model version
    logger.info(f"Creating model version from {model_uri}...")
    mv = client.create_model_version(name=new_model_name, source=model_uri, run_id=run_id)

    # Wait until model version is ready
    for i in range(30):
        v = client.get_model_version(new_model_name, mv.version)
        if v.status == 'READY' or v.status == 'READY':
            break
        logger.info(f"Waiting for model version to become ready (status={v.status})...")
        time.sleep(1)

    logger.info(f"Promoting {new_model_name} version {mv.version} to Staging")
    client.transition_model_version_stage(name=new_model_name, version=mv.version, stage='Staging')

    logger.info("Promotion complete")


if __name__ == '__main__':
    main()
