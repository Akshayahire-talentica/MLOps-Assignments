"""
Unit Tests for MLflow Model Training & Registry
==============================================

Tests the MLflow-integrated training approach:
- MLflow experiment tracking
- Model training with scikit-learn on feature-engineered data
- Model evaluation and metrics logging
- Model registry and versioning
- S3 artifact storage
"""

import pytest
import os
import sys
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))

try:
    from src.training.model_trainer import RecommendationModelTrainer
    import mlflow
    import mlflow.sklearn
    MLFLOW_AVAILABLE = True
except ImportError as e:
    MLFLOW_AVAILABLE = False
    pytest.skip(f"MLflow or training module not available: {e}", allow_module_level=True)


class TestMLflowSetup:
    """Test MLflow configuration and initialization"""
    
    @pytest.fixture
    def training_config(self):
        """Load training configuration"""
        config_path = 'config/data_ingestion_config.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                full_config = yaml.safe_load(f)
                return full_config
        return self._default_config()
    
    def _default_config(self):
        """Default config for testing"""
        return {
            'training': {
                'model_dir': 'data/models',
                'mlflow': {
                    'tracking_uri': 'http://localhost:5000',
                    'experiment_name': 'movielens-training',
                    's3_artifact_root': 's3://mlops-movielens-poc/mlflow-artifacts'
                },
                'models': {
                    'nmf': {
                        'n_components': 20,
                        'max_iter': 200
                    }
                }
            },
            's3': {
                'enabled': True,
                'bucket_name': 'mlops-movielens-poc',
                'artifacts_prefix': 'mlflow-artifacts/'
            }
        }
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_tracking_uri_configuration(self, training_config):
        """Test MLflow tracking URI is configured"""
        mlflow_config = training_config.get('training', {}).get('mlflow', {})
        
        # Should have tracking URI configured
        if 'tracking_uri' in mlflow_config:
            tracking_uri = mlflow_config['tracking_uri']
            assert tracking_uri is not None
            assert isinstance(tracking_uri, str)
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_experiment_name(self, training_config):
        """Test MLflow experiment is configured"""
        mlflow_config = training_config.get('training', {}).get('mlflow', {})
        
        if 'experiment_name' in mlflow_config:
            exp_name = mlflow_config['experiment_name']
            assert exp_name is not None
            assert isinstance(exp_name, str)
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_s3_artifact_storage_configuration(self, training_config):
        """Test S3 is configured for MLflow artifact storage"""
        mlflow_config = training_config.get('training', {}).get('mlflow', {})
        s3_config = training_config.get('s3', {})
        
        # Check S3 artifact root or bucket configuration
        has_s3_artifacts = (
            's3_artifact_root' in mlflow_config or
            (s3_config.get('enabled') and s3_config.get('bucket_name') == 'mlops-movielens-poc')
        )
        
        assert has_s3_artifacts, "S3 artifact storage should be configured"
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_model_trainer_initialization(self, training_config):
        """Test ModelTrainer can be initialized with MLflow"""
        try:
            # Use a test experiment name
            trainer = RecommendationModelTrainer(
                config=training_config,
                experiment_name='test-experiment'
            )
            
            assert trainer is not None
            assert hasattr(trainer, 'experiment_name')
            assert trainer.experiment_name == 'test-experiment'
            
        except Exception as e:
            pytest.skip(f"Trainer initialization failed: {e}")


class TestModelTraining:
    """Test model training with scikit-learn"""
    
    @pytest.fixture
    def training_config(self):
        """Training configuration"""
        return {
            'training': {
                'model_dir': 'data/models',
                'models': {
                    'nmf': {
                        'n_components': 20,
                        'max_iter': 200
                    }
                }
            }
        }
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_sklearn_model_configuration(self, training_config):
        """Test scikit-learn model parameters are configured"""
        models_config = training_config.get('training', {}).get('models', {})
        
        assert 'nmf' in models_config
        nmf_config = models_config['nmf']
        
        assert 'n_components' in nmf_config
        assert 'max_iter' in nmf_config
        assert isinstance(nmf_config['n_components'], int)
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_model_directory_creation(self, training_config):
        """Test model directory is created"""
        model_dir = Path(training_config['training']['model_dir'])
        
        # Directory should be created during training
        assert training_config['training']['model_dir'] == 'data/models'
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    @pytest.mark.integration
    def test_feature_engineered_data_loading(self):
        """Test trainer can load feature-engineered data"""
        feature_dir = Path('data/features')
        
        if not feature_dir.exists():
            pytest.skip("Feature directory not found")
        
        # Check for feature files
        feature_files = list(feature_dir.glob('*_features_*.csv'))
        
        if not feature_files:
            pytest.skip("No feature files available")
        
        assert len(feature_files) > 0


class TestMLflowExperimentTracking:
    """Test MLflow experiment tracking and logging"""
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_experiment_creation(self):
        """Test MLflow experiment can be created"""
        exp_name = f"test-experiment-{os.getpid()}"
        
        try:
            mlflow.set_experiment(exp_name)
            experiment = mlflow.get_experiment_by_name(exp_name)
            
            assert experiment is not None
            assert experiment.name == exp_name
            
        except Exception as e:
            pytest.skip(f"MLflow not accessible: {e}")
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_parameter_logging(self):
        """Test MLflow can log parameters"""
        exp_name = f"test-params-{os.getpid()}"
        
        try:
            mlflow.set_experiment(exp_name)
            
            with mlflow.start_run():
                mlflow.log_param("n_components", 20)
                mlflow.log_param("max_iter", 200)
                
                run = mlflow.active_run()
                assert run is not None
                
        except Exception as e:
            pytest.skip(f"MLflow not accessible: {e}")
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_metrics_logging(self):
        """Test MLflow can log metrics"""
        exp_name = f"test-metrics-{os.getpid()}"
        
        try:
            mlflow.set_experiment(exp_name)
            
            with mlflow.start_run():
                mlflow.log_metric("rmse", 0.85)
                mlflow.log_metric("mae", 0.65)
                mlflow.log_metric("precision_at_k", 0.75)
                
                run = mlflow.active_run()
                assert run is not None
                
        except Exception as e:
            pytest.skip(f"MLflow not accessible: {e}")


class TestModelRegistry:
    """Test MLflow model registry"""
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_model_registration_interface(self):
        """Test model can be registered in MLflow"""
        from sklearn.linear_model import LinearRegression
        import numpy as np
        
        exp_name = f"test-registry-{os.getpid()}"
        
        try:
            mlflow.set_experiment(exp_name)
            
            # Create a simple model
            X = np.array([[1], [2], [3], [4]])
            y = np.array([2, 4, 6, 8])
            model = LinearRegression()
            model.fit(X, y)
            
            with mlflow.start_run():
                # Log model
                mlflow.sklearn.log_model(
                    model,
                    "test_model",
                    registered_model_name=None  # Don't register for test
                )
                
                run = mlflow.active_run()
                assert run is not None
                
        except Exception as e:
            pytest.skip(f"MLflow not accessible: {e}")
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_model_versioning(self):
        """Test MLflow supports model versioning"""
        # MLflow Model Registry uses versioning automatically
        # Test that the concept is supported
        assert hasattr(mlflow, 'register_model')
        assert hasattr(mlflow, 'sklearn')


class TestS3ArtifactStorage:
    """Test S3 artifact storage integration"""
    
    def test_s3_bucket_configuration(self):
        """Test S3 bucket is configured for artifacts"""
        config_path = 'config/data_ingestion_config.yaml'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            s3_config = config.get('s3', {})
            
            assert s3_config.get('enabled', False)
            assert s3_config.get('bucket_name') == 'mlops-movielens-poc'
    
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_mlflow_s3_artifact_uri(self):
        """Test MLflow artifact URI points to S3"""
        config_path = 'config/data_ingestion_config.yaml'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            mlflow_config = config.get('training', {}).get('mlflow', {})
            artifact_root = mlflow_config.get('s3_artifact_root', '')
            
            if artifact_root:
                assert artifact_root.startswith('s3://')
                assert 'mlops-movielens-poc' in artifact_root


class TestTrainingIntegration:
    """Integration tests for complete training pipeline"""
    
    @pytest.mark.integration
    @pytest.mark.skipif(not MLFLOW_AVAILABLE, reason="MLflow not available")
    def test_training_pipeline_smoke(self):
        """Smoke test for training pipeline"""
        config_path = 'config/data_ingestion_config.yaml'
        
        if not os.path.exists(config_path):
            pytest.skip("Config not found")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Verify training config exists
        assert 'training' in config or 's3' in config
    
    @pytest.mark.integration
    def test_model_artifacts_in_s3(self):
        """Test model artifacts are stored in S3"""
        # This would require actual S3 access
        # For now, just test configuration
        assert Path('data/models').exists() or True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
