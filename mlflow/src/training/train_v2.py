"""
Train model v2 using drifted features and log to MLflow.

This is a lightweight wrapper that re-uses RecommendationModelTrainer
to train an NMF model with configurable parameters and register it
as `nmf_recommendation_v2` in MLflow. It also saves a local copy
of the model under `data/models/`.
"""

import json
from datetime import datetime
from pathlib import Path
import logging

import mlflow
import mlflow.sklearn

from model_trainer import RecommendationModelTrainer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    import yaml

    with open('config/data_ingestion_config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Use a dedicated experiment for phase-3 training
    trainer = RecommendationModelTrainer(config, experiment_name="phase3-training")

    # Load features (expects drifted features have been placed into data/features)
    user_features, movie_features, interaction_features = trainer.load_features()

    # Split and train NMF model for v2
    from sklearn.model_selection import train_test_split

    train_interaction, test_interaction = train_test_split(
        interaction_features,
        test_size=0.2,
        random_state=42
    )

    n_components = config.get('training', {}).get('n_components_v2', 30)

    run_name = "nmf_recommendation_v2"
    with mlflow.start_run(run_name=run_name):
        nmf_model, nmf_metrics = trainer.train_nmf_model(train_interaction, n_components=n_components)

        mlflow.log_params({
            'model_type': 'nmf',
            'n_components': n_components,
            'v': 'v2'
        })

        mlflow.log_metrics(nmf_metrics)

        # Register the model under a v2 name
        try:
            mlflow.sklearn.log_model(
                sk_model=nmf_model,
                artifact_path="model",
                registered_model_name="nmf_recommendation_v2"
            )
        except Exception:
            logger.warning("Model registration skipped (MLflow server may be local file store)")

        # Save local copy
        model_dir = Path(config.get('training', {}).get('model_dir', 'data/models'))
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / f"nmf_model_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        import pickle
        with open(model_path, 'wb') as f:
            pickle.dump(nmf_model, f)

        logger.info(f"NMF v2 model saved to {model_path}")

    result = {
        'status': 'SUCCESS',
        'run_name': run_name,
        'metrics': nmf_metrics,
        'model_path': str(model_path),
        'test_set_size': len(test_interaction)
    }

    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
