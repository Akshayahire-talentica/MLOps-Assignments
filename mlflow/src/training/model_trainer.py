"""
Model Training Module with MLflow
==================================

Trains recommendation models using engineered features.
Integrates with MLflow for experiment tracking and model registry.

Input: Features v1
Output: Model v1 registered in MLflow
"""

import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, List
import pickle

from sklearn.decomposition import NMF
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error
import mlflow
import mlflow.sklearn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationModelTrainer:
    """Trains recommendation models and logs to MLflow"""
    
    def __init__(self, config: Dict[str, Any], experiment_name: str = "phase2-training"):
        """Initialize model trainer with MLflow integration"""
        self.config = config.get('training', {})
        self.experiment_name = experiment_name
        
        # Set MLflow tracking
        mlflow.set_experiment(experiment_name)
        logger.info(f"MLflow experiment set to: {experiment_name}")
        
        # Create models directory
        self.model_dir = Path(self.config.get('model_dir', 'data/models'))
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("RecommendationModelTrainer initialized")
    
    def load_features(self, feature_dir: str = 'data/features') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load engineered features"""
        logger.info(f"Loading features from {feature_dir}...")
        
        try:
            import glob
            
            # Find latest feature files
            user_files = glob.glob(f'{feature_dir}/user_features_*.parquet')
            movie_files = glob.glob(f'{feature_dir}/movie_features_*.parquet')
            interaction_files = glob.glob(f'{feature_dir}/interaction_features_*.parquet')
            
            if not all([user_files, movie_files, interaction_files]):
                raise FileNotFoundError("Feature files not found")
            
            # Load latest versions
            user_features = pd.read_parquet(max(user_files))
            movie_features = pd.read_parquet(max(movie_files))
            interaction_features = pd.read_parquet(max(interaction_files))
            
            logger.info(f"Loaded user features: {user_features.shape}")
            logger.info(f"Loaded movie features: {movie_features.shape}")
            logger.info(f"Loaded interaction features: {interaction_features.shape}")
            
            return user_features, movie_features, interaction_features
        
        except Exception as e:
            logger.error(f"Failed to load features: {str(e)}", exc_info=True)
            raise
    
    def prepare_training_data(self, user_features: pd.DataFrame,
                            movie_features: pd.DataFrame,
                            interaction_features: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare training data by merging features"""
        logger.info("Preparing training data...")
        
        try:
            # Merge user features with interactions
            df = interaction_features.copy()
            df = df.merge(user_features, on='UserID', how='left', suffixes=('', '_user'))
            df = df.merge(movie_features, on='MovieID', how='left', suffixes=('', '_movie'))
            
            # Drop rows with missing values
            df = df.dropna()
            
            logger.info(f"Training data shape after merging: {df.shape}")
            
            # Prepare features and target
            target = df['Rating']
            
            # Select feature columns (exclude ID and target columns)
            feature_cols = [col for col in df.columns 
                          if col not in ['UserID', 'MovieID', 'Rating', 'Timestamp']]
            
            X = df[feature_cols].copy()
            
            logger.info(f"Feature set shape: {X.shape}")
            logger.info(f"Features: {list(X.columns)[:10]}... (showing first 10)")
            
            return X, target
        
        except Exception as e:
            logger.error(f"Failed to prepare training data: {str(e)}", exc_info=True)
            raise
    
    def train_nmf_model(self, interaction_features: pd.DataFrame, 
                       n_components: int = 20,
                       max_iter: int = 200) -> Tuple[Any, Dict[str, Any]]:
        """Train NMF recommendation model"""
        logger.info(f"Training NMF model with {n_components} components...")
        
        try:
            # Create user-item matrix
            interaction_features_sorted = interaction_features.sort_values(['UserID', 'MovieID'])
            
            # Create sparse matrix approximation using pivot
            user_item_matrix = interaction_features_sorted.pivot_table(
                index='UserID',
                columns='MovieID',
                values='Rating',
                fill_value=0
            )
            
            logger.info(f"User-item matrix shape: {user_item_matrix.shape}")
            
            # Train NMF model
            model = NMF(n_components=n_components, max_iter=max_iter, random_state=42, verbose=0)
            user_features_nmf = model.fit_transform(user_item_matrix)
            movie_features_nmf = model.components_.T
            
            # Calculate reconstruction error as metric
            reconstruction = user_features_nmf @ model.components_
            rmse = np.sqrt(mean_squared_error(user_item_matrix, reconstruction))
            mae = mean_absolute_error(user_item_matrix, reconstruction)
            
            metrics = {
                'reconstruction_rmse': float(rmse),
                'reconstruction_mae': float(mae),
                'sparsity': float(1 - (interaction_features.shape[0] / (user_item_matrix.shape[0] * user_item_matrix.shape[1])))
            }
            
            logger.info(f"NMF training complete. RMSE: {rmse:.4f}, MAE: {mae:.4f}")
            
            return model, metrics
        
        except Exception as e:
            logger.error(f"NMF training failed: {str(e)}", exc_info=True)
            raise
    
    def train_baseline_model(self, X: pd.DataFrame, y: pd.Series) -> Tuple[Any, Dict[str, Any]]:
        """Train baseline model using mean rating"""
        logger.info("Training baseline model (mean rating)...")
        
        try:
            # Simple baseline: mean rating per user
            baseline_predictions = y.mean()
            rmse = np.sqrt(mean_squared_error(y, [baseline_predictions] * len(y)))
            mae = mean_absolute_error(y, [baseline_predictions] * len(y))
            
            metrics = {
                'baseline_rmse': float(rmse),
                'baseline_mae': float(mae),
                'baseline_value': float(baseline_predictions)
            }
            
            logger.info(f"Baseline RMSE: {rmse:.4f}, MAE: {mae:.4f}")
            
            return {'baseline': baseline_predictions}, metrics
        
        except Exception as e:
            logger.error(f"Baseline model training failed: {str(e)}", exc_info=True)
            raise
    
    def train_all_models(self, user_features: pd.DataFrame = None,
                        movie_features: pd.DataFrame = None,
                        interaction_features: pd.DataFrame = None) -> Dict[str, Any]:
        """Train all models and log to MLflow"""
        logger.info("=" * 80)
        logger.info("STARTING MODEL TRAINING")
        logger.info("=" * 80)
        
        try:
            # Load features if not provided
            if user_features is None or movie_features is None or interaction_features is None:
                user_features, movie_features, interaction_features = self.load_features()
            
            models_trained = {}
            models_metrics = {}
            
            # Split data
            train_interaction, test_interaction = train_test_split(
                interaction_features,
                test_size=0.2,
                random_state=42
            )
            
            logger.info(f"Train set size: {len(train_interaction)}, Test set size: {len(test_interaction)}")
            
            # Train NMF model
            with mlflow.start_run(run_name="nmf_recommendation_v1") as run:
                nmf_model, nmf_metrics = self.train_nmf_model(train_interaction, n_components=20)
                
                # Log parameters
                mlflow.log_params({
                    'model_type': 'nmf',
                    'n_components': 20,
                    'max_iter': 200,
                    'version': 'v1'
                })
                
                # Log metrics
                mlflow.log_metrics(nmf_metrics)
                
                # Save model locally first
                model_path = self.model_dir / f"nmf_model_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
                with open(model_path, 'wb') as f:
                    pickle.dump(nmf_model, f)
                
                # Log model artifact (simpler approach for compatibility)
                mlflow.log_artifact(str(model_path), artifact_path="model")
                
                # Try to register model if server supports it
                try:
                    run_id = run.info.run_id
                    model_uri = f"runs:/{run_id}/model/nmf_model_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
                    mlflow.register_model(model_uri, "nmf_recommendation_v1")
                    logger.info("Model registered in MLflow registry")
                except Exception as e:
                    logger.info(f"Model logged but registration skipped (server may not support model registry): {str(e)}")
                
                logger.info(f"NMF model logged to MLflow and saved to {model_path}")
                
                # Upload model artifacts to S3 if enabled
                self._upload_model_to_s3(model_path, nmf_metrics)
                
                models_trained['nmf'] = nmf_model
                models_metrics['nmf'] = nmf_metrics
            
            # Train baseline model
            with mlflow.start_run(run_name="baseline_mean_v1") as run:
                X, y = self.prepare_training_data(user_features, movie_features, train_interaction)
                baseline_model, baseline_metrics = self.train_baseline_model(X, y)
                
                # Log parameters
                mlflow.log_params({
                    'model_type': 'baseline_mean',
                    'version': 'v1'
                })
                
                # Log metrics
                mlflow.log_metrics(baseline_metrics)
                
                # Save and log as artifact for compatibility (convert numpy types to native Python)
                baseline_path = self.model_dir / f"baseline_model_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                baseline_serializable = {k: float(v) if hasattr(v, 'item') else v for k, v in baseline_model.items()}
                with open(baseline_path, 'w') as f:
                    json.dump(baseline_serializable, f, indent=2)
                
                mlflow.log_artifact(str(baseline_path), artifact_path="model")
                
                logger.info(f"Baseline model logged to MLflow")
                
                models_trained['baseline'] = baseline_model
                models_metrics['baseline'] = baseline_metrics
            
            logger.info("=" * 80)
            logger.info("MODEL TRAINING COMPLETED")
            logger.info("=" * 80)
            
            return {
                'status': 'SUCCESS',
                'timestamp': datetime.now().isoformat(),
                'model_version': 'v1',
                'models_trained': list(models_trained.keys()),
                'metrics': models_metrics,
                'test_set_size': len(test_interaction),
                'experiment_name': self.experiment_name
            }
        
        except Exception as e:
            logger.error(f"Model training failed: {str(e)}", exc_info=True)
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    def _upload_model_to_s3(self, model_path: Path, metrics: Dict[str, Any]) -> None:
        """Upload trained model and metadata to S3 bucket"""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))
            from data_ingestion.s3_storage import S3DataStorage
            import yaml
            
            # Load S3 configuration
            config_file = 'config/data_ingestion_config.yaml'
            if not os.path.exists(config_file):
                logger.warning(f"Config file {config_file} not found, skipping S3 upload")
                return
            
            with open(config_file, 'r') as f:
                full_config = yaml.safe_load(f)
            
            s3_config = full_config.get('s3', {})
            
            if not s3_config.get('enabled', False):
                logger.info("S3 upload disabled in configuration")
                return
            
            bucket_name = s3_config.get('bucket_name')
            region = s3_config.get('region', 'ap-south-1')
            profile = s3_config.get('profile')
            
            storage = S3DataStorage(
                bucket_name=bucket_name,
                region=region,
                profile=profile
            )
            
            # Upload model file
            s3_key = f"models/{model_path.name}"
            logger.info(f"Uploading model to s3://{bucket_name}/{s3_key}")
            
            storage.upload_file(
                str(model_path),
                s3_key,
                metadata={
                    'type': 'model',
                    'framework': 'sklearn',
                    'algorithm': 'nmf',
                    'version': 'v1',
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            # Upload metrics metadata
            metadata_key = f"models/{model_path.stem}_metrics.json"
            metadata_file = model_path.parent / f"{model_path.stem}_metrics.json"
            
            with open(metadata_file, 'w') as f:
                json.dump(metrics, f, indent=2)
            
            storage.upload_file(str(metadata_file), metadata_key)
            
            logger.info(f"✓ Model artifacts uploaded to S3: {s3_key}")
            
            # Cleanup local metadata file
            if metadata_file.exists():
                metadata_file.unlink()
                
        except ImportError:
            logger.warning("boto3 not available, skipping S3 upload")
        except Exception as e:
            logger.warning(f"Failed to upload model to S3: {str(e)}")
    
    def register_best_model(self, model_name: str = "recommendation-model-v1") -> Dict[str, Any]:
        """Register best model in MLflow registry"""
        logger.info(f"Registering model: {model_name}...")
        
        try:
            # Get best run from current experiment
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            if not experiment:
                raise ValueError(f"Experiment {self.experiment_name} not found")
            
            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
            
            if runs.empty:
                raise ValueError("No runs found in experiment")
            
            # Find best run by RMSE (lower is better)
            best_run = runs.loc[runs['metrics.reconstruction_rmse'].idxmin()]
            
            logger.info(f"Best run: {best_run['run_id']}")
            
            # Register model
            model_uri = f"runs:/{best_run['run_id']}/model"
            model_details = mlflow.register_model(model_uri, model_name)
            
            logger.info(f"Model registered: {model_details.name}, Version: {model_details.version}")
            
            return {
                'status': 'SUCCESS',
                'model_name': model_details.name,
                'model_version': model_details.version,
                'run_id': best_run['run_id'],
                'best_metric_rmse': float(best_run.get('metrics.reconstruction_rmse', 0))
            }
        
        except Exception as e:
            logger.warning(f"Model registration failed: {str(e)}")
            return {
                'status': 'PARTIAL_SUCCESS',
                'message': 'Training completed but registration skipped',
                'error': str(e)
            }


if __name__ == '__main__':
    import yaml
    
    with open('config/data_ingestion_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    trainer = RecommendationModelTrainer(config)
    result = trainer.train_all_models()
    
    print(json.dumps(result, indent=2))
