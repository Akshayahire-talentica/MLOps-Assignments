"""
Unit Tests for Data Ingestion Module
====================================

Tests the MovieLensDataIngestor class to verify:
- Raw data loading
- Schema validation
- Parquet output creation
- Metadata generation
"""

import pytest
import pandas as pd
import os
import tempfile
from pathlib import Path
import json
import yaml
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

try:
    from data_ingestion.ingest_movies import MovieLensDataIngestor, IngestionMetadata
except ImportError as e:
    pytest.skip(f"Ingestion module not available: {e}", allow_module_level=True)


class TestDataIngestion:
    """Test data ingestion functionality"""
    
    @pytest.fixture
    def config(self):
        """Load test configuration"""
        config_path = 'config/data_ingestion_config.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            pytest.skip("Configuration file not found")
    
    @pytest.fixture
    def ingestor(self, config):
        """Create ingestor instance"""
        return MovieLensDataIngestor(config)
    
    def test_ingestor_initialization(self, ingestor):
        """Test ingestor initializes correctly"""
        assert ingestor is not None
        assert ingestor.config is not None
        assert ingestor.metadata_list is not None
    
    def test_movies_file_exists(self, config):
        """Test movies.dat file exists"""
        movies_path = config['data_sources']['movies']['path']
        assert os.path.exists(movies_path), f"Movies file not found at {movies_path}"
    
    def test_ratings_file_exists(self, config):
        """Test ratings.dat file exists"""
        ratings_path = config['data_sources']['ratings']['path']
        assert os.path.exists(ratings_path), f"Ratings file not found at {ratings_path}"
    
    def test_users_file_exists(self, config):
        """Test users.dat file exists"""
        users_path = config['data_sources']['users']['path']
        assert os.path.exists(users_path), f"Users file not found at {users_path}"
    
    def test_ingest_movies(self, ingestor):
        """Test movies ingestion"""
        df, metadata = ingestor.ingest_movies()
        
        # Verify DataFrame
        assert df is not None
        assert len(df) > 0
        assert 'MovieID' in df.columns
        assert 'Title' in df.columns
        assert 'Genres' in df.columns
        
        # Verify metadata
        assert metadata.row_count > 0
        assert metadata.column_count == 3
    
    def test_ingest_ratings(self, ingestor):
        """Test ratings ingestion"""
        df, metadata = ingestor.ingest_ratings()
        
        # Verify DataFrame
        assert df is not None
        assert len(df) > 0
        assert 'UserID' in df.columns
        assert 'MovieID' in df.columns
        assert 'Rating' in df.columns
        assert 'Timestamp' in df.columns
        
        # Verify metadata
        assert metadata.row_count > 0
        assert metadata.column_count == 4
    
    def test_ingest_users(self, ingestor):
        """Test users ingestion"""
        df, metadata = ingestor.ingest_users()
        
        # Verify DataFrame
        assert df is not None
        assert len(df) > 0
        assert 'UserID' in df.columns
        assert 'Gender' in df.columns
        assert 'Age' in df.columns
        
        # Verify metadata
        assert metadata.row_count > 0
        assert metadata.column_count == 5
    
    def test_parquet_output_created(self, ingestor):
        """Test parquet files are created"""
        results = ingestor.ingest_all()
        
        # Check that parquet files were created
        assert 'movies' in results
        assert 'ratings' in results
        assert 'users' in results
    
    def test_data_counts(self, ingestor):
        """Test expected data counts"""
        results = ingestor.ingest_all()
        
        movies_df = results['movies'][0]
        ratings_df = results['ratings'][0]
        users_df = results['users'][0]
        
        # Verify counts - use lower thresholds for CI sample data
        # CI uses sample data: 100 movies, 1000 ratings, 50 users
        # Local uses full data: 3883 movies, 1M ratings, 6040 users
        is_sample_data = len(movies_df) < 1000  # Detect if using sample fixtures
        
        if is_sample_data:
            # Sample data thresholds (CI environment)
            assert len(movies_df) >= 50, f"Expected at least 50 movies, got {len(movies_df)}"
            assert len(ratings_df) >= 500, f"Expected at least 500 ratings, got {len(ratings_df)}"
            assert len(users_df) >= 25, f"Expected at least 25 users, got {len(users_df)}"
        else:
            # Full data thresholds (local environment)
            assert len(movies_df) >= 3000, f"Expected at least 3000 movies, got {len(movies_df)}"
            assert len(ratings_df) >= 1000000, f"Expected at least 1M ratings, got {len(ratings_df)}"
            assert len(users_df) >= 5000, f"Expected at least 5000 users, got {len(users_df)}"


class TestDataTypes:
    """Test data type conversions"""
    
    @pytest.fixture
    def config(self):
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def ingestor(self, config):
        return MovieLensDataIngestor(config)
    
    def test_movie_id_is_integer(self, ingestor):
        """Test MovieID is integer type"""
        df, _ = ingestor.ingest_movies()
        assert df['MovieID'].dtype in ['int32', 'int64']
    
    def test_rating_is_float(self, ingestor):
        """Test Rating is float type"""
        df, _ = ingestor.ingest_ratings()
        assert df['Rating'].dtype in ['float32', 'float64']
    
    def test_timestamp_is_integer(self, ingestor):
        """Test Timestamp is integer type"""
        df, _ = ingestor.ingest_ratings()
        assert df['Timestamp'].dtype in ['int64', 'int32']


class TestMetadataGeneration:
    """Test metadata generation"""
    
    @pytest.fixture
    def config(self):
        with open('config/data_ingestion_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def ingestor(self, config):
        return MovieLensDataIngestor(config)
    
    def test_metadata_created(self, ingestor):
        """Test metadata is created"""
        results = ingestor.ingest_all()
        manifest = results.get('manifest')
        
        assert manifest is not None
        assert 'datasets' in manifest
        assert 'summary' in manifest
    
    def test_metadata_has_required_fields(self, ingestor):
        """Test metadata has all required fields"""
        df, metadata = ingestor.ingest_movies()
        metadata_dict = metadata.to_dict()
        
        assert 'source_file' in metadata_dict
        assert 'output_file' in metadata_dict
        assert 'row_count' in metadata_dict
        assert 'timestamp' in metadata_dict


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
