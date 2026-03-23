"""
Phase 2 Unit Tests
==================

Tests for feature engineering, model training, and serving components.
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
from pathlib import Path
import yaml
import json
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))

# Import Phase 2 modules
try:
    from src.features.feature_engineering import FeatureEngineer
    from src.features.feature_validator import FeatureValidator
    from src.training.model_trainer import RecommendationModelTrainer
    from src.training.model_selection import ModelSelector
    MODULES_AVAILABLE = True
except ImportError as e:
    MODULES_AVAILABLE = False
    pytest.skip(f"Phase 2 modules not available: {e}", allow_module_level=True)


class TestFeatureEngineering:
    """Test suite for feature engineering"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def feature_engineer(self, config):
        """Create feature engineer instance"""
        return FeatureEngineer(config)
    
    def test_feature_engineer_initialization(self, feature_engineer):
        """Test feature engineer initialization"""
        assert feature_engineer is not None
        assert hasattr(feature_engineer, 'config')
        assert hasattr(feature_engineer, 'output_path')
    
    def test_output_dir_creation(self, feature_engineer):
        """Test output directory creation"""
        assert feature_engineer.output_path.exists()
    
    def test_feature_engineer_attributes(self, feature_engineer):
        """Test feature engineer has required attributes"""
        assert hasattr(feature_engineer, 'movies_df')
        assert hasattr(feature_engineer, 'ratings_df')
        assert hasattr(feature_engineer, 'users_df')
        assert hasattr(feature_engineer, 'feature_version')
    
    def test_feature_version(self, feature_engineer):
        """Test feature version is set"""
        assert feature_engineer.feature_version in ['v1', 'v2', 'v0']


class TestFeatureValidator:
    """Test suite for feature validation"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def feature_validator(self, config):
        """Create feature validator instance"""
        return FeatureValidator(config)
    
    @pytest.fixture
    def sample_features(self):
        """Create sample feature dataframes"""
        user_features = pd.DataFrame({
            'UserID': [1, 2, 3],
            'AvgRating': [4.5, 3.5, 4.0],
            'RatingCount': [10, 20, 15]
        })
        
        movie_features = pd.DataFrame({
            'MovieID': [1, 2, 3],
            'AvgRating': [4.0, 3.5, 4.5],
            'Popularity': [100, 50, 75]
        })
        
        interaction_features = pd.DataFrame({
            'UserID': [1, 2, 3],
            'MovieID': [1, 2, 3],
            'Rating': [5, 4, 3],
            'Timestamp': [1609459200, 1609545600, 1609632000]
        })
        
        return user_features, movie_features, interaction_features
    
    def test_validator_initialization(self, feature_validator):
        """Test feature validator initialization"""
        assert feature_validator is not None
        assert hasattr(feature_validator, 'config')
    
    def test_validate_feature_schema(self, feature_validator, sample_features):
        """Test schema validation"""
        user_features, movie_features, interaction_features = sample_features
        result = feature_validator.validate_feature_schema(user_features, movie_features, interaction_features)
        
        assert result is not None
        assert 'status' in result
        assert result['status'].value in ['PASSED', 'PASSED_WITH_WARNINGS', 'FAILED']
    
    def test_validate_feature_nulls(self, feature_validator, sample_features):
        """Test null value validation"""
        user_features, movie_features, interaction_features = sample_features
        result = feature_validator.validate_feature_nulls(user_features, movie_features, interaction_features)
        
        assert result is not None
        assert 'status' in result
    
    def test_validate_feature_ranges(self, feature_validator, sample_features):
        """Test feature range validation"""
        user_features, movie_features, interaction_features = sample_features
        result = feature_validator.validate_feature_ranges(user_features, movie_features, interaction_features)
        
        assert result is not None
        assert 'status' in result


class TestModelTrainer:
    """Test suite for model training"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def trainer(self, config):
        """Create model trainer instance"""
        with patch('mlflow.set_experiment'):
            return RecommendationModelTrainer(config)
    
    def test_trainer_initialization(self, trainer):
        """Test model trainer initialization"""
        assert trainer is not None
        assert hasattr(trainer, 'config')
        assert hasattr(trainer, 'model_dir')
    
    def test_model_dir_creation(self, trainer):
        """Test model directory creation"""
        assert trainer.model_dir.exists()
    
    def test_train_baseline_model(self, trainer):
        """Test baseline model training"""
        X = pd.DataFrame(np.random.randn(100, 5))
        y = pd.Series(np.random.uniform(0.5, 5.0, 100))
        
        model, metrics = trainer.train_baseline_model(X, y)
        
        assert model is not None
        assert metrics is not None
        assert 'baseline_rmse' in metrics or 'rmse' in metrics
        assert 'baseline_mae' in metrics or 'mae' in metrics


class TestModelSelector:
    """Test suite for model selection"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def selector(self, config):
        """Create model selector instance"""
        return ModelSelector(config)
    
    def test_selector_initialization(self, selector):
        """Test model selector initialization"""
        assert selector is not None
        assert hasattr(selector, 'config')
        assert hasattr(selector, 'report_dir')
    
    def test_report_dir_creation(self, selector):
        """Test report directory creation"""
        assert selector.report_dir.exists()


# Integration Tests
class TestPhase2Integration:
    """Integration tests for Phase 2 components"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    def test_feature_validator_initialization(self, config):
        """Test feature validator can be initialized"""
        validator = FeatureValidator(config)
        assert validator is not None
        assert hasattr(validator, 'config')
        assert hasattr(validator, 'validation_results')
    
    def test_model_trainer_baseline(self, config):
        """Test baseline model training works"""
        with patch('mlflow.set_experiment'):
            trainer = RecommendationModelTrainer(config)
            X = pd.DataFrame(np.random.randn(100, 5))
            y = pd.Series(np.random.uniform(0.5, 5.0, 100))
            
            model, metrics = trainer.train_baseline_model(X, y)
            
            assert model is not None
            assert metrics is not None
            assert 'baseline_rmse' in metrics or 'rmse' in metrics
            rmse_key = 'baseline_rmse' if 'baseline_rmse' in metrics else 'rmse'
            assert metrics[rmse_key] >= 0
    
    def test_model_selector_dir_creation(self, config):
        """Test model selector creates directories"""
        selector = ModelSelector(config)
        assert selector.report_dir.exists()


# Performance Tests
class TestPhase2Performance:
    """Performance tests for Phase 2 components"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    def test_feature_validator_performance(self, config):
        """Test feature validator performance"""
        validator = FeatureValidator(config)
        
        # Create large dataframes
        user_features = pd.DataFrame({
            'UserID': list(range(1, 1001)),
            'AvgRating': np.random.uniform(0.5, 5.0, 1000),
            'RatingCount': np.random.randint(1, 500, 1000)
        })
        
        movie_features = pd.DataFrame({
            'MovieID': list(range(1, 501)),
            'AvgRating': np.random.uniform(0.5, 5.0, 500),
            'Popularity': np.random.randint(0, 1000, 500)
        })
        
        interaction_features = pd.DataFrame({
            'UserID': np.random.randint(1, 1001, 10000),
            'MovieID': np.random.randint(1, 501, 10000),
            'Rating': np.random.uniform(0.5, 5.0, 10000),
            'Timestamp': np.random.randint(1609459200, 1640995200, 10000)
        })
        
        # Time the validation
        import time
        start = time.time()
        result = validator.validate_all_features(user_features, movie_features, interaction_features)
        duration = time.time() - start
        
        # Should complete in reasonable time
        assert duration < 10.0, f"Validation took {duration:.2f}s, should be < 10s"
        assert result is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
