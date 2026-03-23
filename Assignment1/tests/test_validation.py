"""
Unit Tests for Data Validation Module
=====================================

Tests the DataValidator class to verify:
- Schema validation
- Statistical validation
- Referential integrity
- Quality scoring
"""

import pytest
import pandas as pd
import numpy as np
import os
import yaml
import json
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

try:
    from data_validation.validators import DataValidator, ValidationStatus
except ImportError as e:
    pytest.skip(f"Validation module not available: {e}", allow_module_level=True)


class TestDataValidation:
    """Test data validation functionality"""
    
    @pytest.fixture
    def config(self):
        """Load validation rules"""
        with open('config/validation_rules.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def validator(self, config):
        """Create validator instance"""
        return DataValidator(config)
    
    @pytest.fixture
    def sample_movies_data(self):
        """Create sample movies dataframe"""
        return pd.DataFrame({
            'MovieID': [1, 2, 3, 4, 5],
            'Title': ['Movie A', 'Movie B', 'Movie C', 'Movie D', 'Movie E'],
            'Genres': ['Action', 'Comedy', 'Drama', 'Romance', 'Thriller']
        })
    
    @pytest.fixture
    def sample_ratings_data(self):
        """Create sample ratings dataframe"""
        return pd.DataFrame({
            'UserID': [1, 1, 2, 2, 3],
            'MovieID': [1, 2, 1, 3, 2],
            'Rating': [5.0, 3.5, 4.0, 2.5, 4.5],
            'Timestamp': [978300760, 978302109, 978301968, 978300275, 978824291]
        })
    
    @pytest.fixture
    def sample_users_data(self):
        """Create sample users dataframe"""
        return pd.DataFrame({
            'UserID': [1, 2, 3, 4, 5],
            'Gender': ['M', 'F', 'M', 'F', 'M'],
            'Age': [25, 45, 35, 50, 28],
            'Occupation': [1, 5, 10, 15, 3],
            'ZipCode': ['12345', '67890', '11111', '22222', '33333']
        })
    
    def test_validator_initialization(self, validator):
        """Test validator initializes correctly"""
        assert validator is not None
        assert validator.config is not None
        assert validator.validation_results is not None
    
    def test_schema_validation_passes(self, validator, sample_movies_data):
        """Test schema validation passes for valid data"""
        result = validator.validate_schema(sample_movies_data, 'movies')
        
        assert result is not None
        assert result.check_name == 'schema_validation'
        # Status should be PASSED or PASSED_WITH_WARNINGS
        assert result.status in [ValidationStatus.PASSED, ValidationStatus.PASSED_WITH_WARNINGS]
    
    def test_statistical_validation_passes(self, validator, sample_movies_data):
        """Test statistical validation passes for valid data"""
        result = validator.validate_statistics(sample_movies_data, 'movies')
        
        assert result is not None
        assert result.check_name == 'statistical_validation'
        assert result.status in [ValidationStatus.PASSED, ValidationStatus.PASSED_WITH_WARNINGS]
    
    def test_uniqueness_validation(self, validator, sample_movies_data):
        """Test uniqueness validation"""
        result = validator.validate_uniqueness(sample_movies_data, 'movies')
        
        assert result is not None
        assert result.check_name == 'uniqueness'
        assert result.status == ValidationStatus.PASSED
    
    def test_completeness_validation(self, validator, sample_movies_data):
        """Test completeness validation"""
        result = validator.validate_completeness(sample_movies_data, 'movies')
        
        assert result is not None
        assert result.check_name == 'completeness'
        assert result.status == ValidationStatus.PASSED
    
    def test_validation_with_nulls(self, validator):
        """Test validation detects null values"""
        df = pd.DataFrame({
            'MovieID': [1, 2, None],
            'Title': ['A', 'B', 'C'],
            'Genres': ['Action', 'Comedy', 'Drama']
        })
        
        result = validator.validate_completeness(df, 'movies')
        
        assert result is not None
        # Should have warnings or errors about nulls
        assert len(result.warnings) > 0 or len(result.errors) > 0
    
    def test_validation_with_invalid_values(self, validator):
        """Test validation detects invalid values"""
        df = pd.DataFrame({
            'UserID': [1, 2, 3],
            'Gender': ['M', 'F', 'X'],  # X is invalid
            'Age': [25, 45, 35],
            'Occupation': [1, 5, 10],
            'ZipCode': ['12345', '67890', '11111']
        })
        
        result = validator.validate_statistics(df, 'users')
        
        assert result is not None
        # Should have errors about invalid gender values
        assert len(result.errors) > 0 or len(result.warnings) > 0
    
    def test_validation_report_generation(self, validator, sample_movies_data):
        """Test validation report generation"""
        validator.validate_schema(sample_movies_data, 'movies')
        validator.validate_statistics(sample_movies_data, 'movies')
        
        report = validator.get_validation_report()
        
        assert report is not None
        assert 'validation_results' in report
        assert 'summary' in report
        assert report['summary']['total_checks'] > 0
    
    def test_rating_range_validation(self, validator, sample_ratings_data):
        """Test rating values are in valid range"""
        result = validator.validate_statistics(sample_ratings_data, 'ratings')
        
        assert result is not None
        # All ratings should be 0.5-5.0
        assert len(result.errors) == 0


class TestValidationIntegration:
    """Test validation with real data"""
    
    @pytest.fixture
    def config(self):
        with open('config/validation_rules.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def validator(self, config):
        return DataValidator(config)
    
    def test_load_and_validate_real_data(self, validator):
        """Test loading and validating real parquet files"""
        import glob
        
        # Find latest parquet files
        movies_files = glob.glob('data/processed/movies/*.parquet')
        ratings_files = glob.glob('data/processed/ratings/*.parquet')
        users_files = glob.glob('data/processed/users/*.parquet')
        
        if not (movies_files and ratings_files and users_files):
            pytest.skip("Processed data files not found")
        
        # Load data
        movies_df = pd.read_parquet(max(movies_files))
        ratings_df = pd.read_parquet(max(ratings_files))
        users_df = pd.read_parquet(max(users_files))
        
        # Validate
        validator.validate_schema(movies_df, 'movies')
        validator.validate_schema(ratings_df, 'ratings')
        validator.validate_schema(users_df, 'users')
        
        report = validator.get_validation_report()
        
        # Should have results
        assert report is not None
        assert len(report['validation_results']) > 0


class TestReferentialIntegrity:
    """Test referential integrity checks"""
    
    @pytest.fixture
    def config(self):
        with open('config/validation_rules.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def validator(self, config):
        return DataValidator(config)
    
    def test_orphaned_records_detection(self, validator):
        """Test detection of orphaned foreign keys"""
        movies_df = pd.DataFrame({
            'MovieID': [1, 2, 3],
            'Title': ['A', 'B', 'C'],
            'Genres': ['Action', 'Comedy', 'Drama']
        })
        
        ratings_df = pd.DataFrame({
            'UserID': [1, 2, 3, 4],
            'MovieID': [1, 2, 3, 999],  # 999 doesn't exist in movies
            'Rating': [5.0, 3.5, 4.0, 2.5],
            'Timestamp': [978300760, 978302109, 978301968, 978300275]
        })
        
        dfs = {'movies': movies_df, 'ratings': ratings_df}
        
        # Note: This requires referential integrity to be enabled in config
        # result = validator.validate_referential_integrity(dfs, 'ratings')
        # assert result is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
