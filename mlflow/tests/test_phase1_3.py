"""
Unit Tests for Phase 1.3: Data Versioning & Lineage Tracking

Tests the following components:
- MetadataManager: Metadata extraction and version tracking
- DataLineage: Lineage tracking and graph management
- DataVersioning: Integration with DVC and dataset tracking
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from datetime import datetime
import shutil

import pandas as pd
import pyarrow.parquet as pq

from src.data_ingestion.data_versioning import (
    MetadataManager,
    DataLineage,
    DataVersioning,
    create_phase1_pipeline,
)


@pytest.fixture
def temp_dir():
    """Create temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 15.8, 25.1, 30.2],
        "category": ["A", "B", "A", "C", "B"]
    })
    
    file_path = os.path.join(temp_dir, "test_data.parquet")
    df.to_parquet(file_path, engine="pyarrow", compression="snappy")
    
    return file_path, df


class TestMetadataManager:
    """Test MetadataManager class."""

    def test_metadata_manager_initialization(self, temp_dir):
        """Test MetadataManager initialization."""
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        assert manager.metadata_dir.exists()
        assert manager.metadata_cache == {}

    def test_compute_file_hash(self, temp_dir, sample_parquet_file):
        """Test file hash computation."""
        file_path, _ = sample_parquet_file
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        
        hash1 = manager.compute_file_hash(file_path)
        hash2 = manager.compute_file_hash(file_path)
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hash length
        assert isinstance(hash1, str)

    def test_extract_parquet_metadata(self, temp_dir, sample_parquet_file):
        """Test Parquet metadata extraction."""
        file_path, df = sample_parquet_file
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        
        metadata = manager.extract_parquet_metadata(file_path)
        
        assert metadata["num_rows"] == 5
        assert metadata["num_columns"] == 4
        assert set(metadata["columns"]) == {"id", "name", "value", "category"}
        assert "file_hash" in metadata
        assert "file_size" in metadata
        assert metadata["shape"] == [5, 4]

    def test_create_version(self, temp_dir, sample_parquet_file):
        """Test version creation."""
        file_path, _ = sample_parquet_file
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        
        version_id = manager.create_version(
            dataset_name="test_dataset",
            file_path=file_path,
            description="Test version",
            tags=["test", "v1"]
        )
        
        cache_key = f"test_dataset_{version_id}"
        assert cache_key in manager.metadata_cache
        assert len(version_id) == 15  # YYYYMMDD_HHMMSS
        
        # Check metadata file was created
        metadata_files = list(manager.metadata_dir.glob("test_dataset_*.json"))
        assert len(metadata_files) == 1

    def test_get_version_history(self, temp_dir, sample_parquet_file):
        """Test version history retrieval."""
        file_path, _ = sample_parquet_file
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        
        # Create multiple versions
        import time
        version1 = manager.create_version(
            dataset_name="test_dataset",
            file_path=file_path,
            description="Version 1"
        )
        
        time.sleep(1)  # Ensure different timestamp
        
        version2 = manager.create_version(
            dataset_name="test_dataset",
            file_path=file_path,
            description="Version 2"
        )
        
        history = manager.get_version_history("test_dataset")
        
        assert len(history) == 2
        assert history[0]["version_id"] == version2  # Most recent first
        assert history[1]["version_id"] == version1

    def test_compare_versions(self, temp_dir, sample_parquet_file):
        """Test version comparison."""
        file_path, _ = sample_parquet_file
        manager = MetadataManager(metadata_dir=os.path.join(temp_dir, "metadata"))
        
        version1 = manager.create_version(
            dataset_name="test_dataset",
            file_path=file_path,
            description="Version 1"
        )
        
        version2 = manager.create_version(
            dataset_name="test_dataset",
            file_path=file_path,
            description="Version 2"
        )
        
        comparison = manager.compare_versions("test_dataset", version1, version2)
        
        assert comparison["version1"] == version1
        assert comparison["version2"] == version2
        assert "changes" in comparison
        # File hashes should be the same since it's the same file
        assert comparison["changes"]["file_hash"]["changed"] == False


class TestDataLineage:
    """Test DataLineage class."""

    def test_data_lineage_initialization(self, temp_dir):
        """Test DataLineage initialization."""
        lineage = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage"))
        assert lineage.lineage_dir.exists()
        assert lineage.lineage_graph == {}

    def test_record_transform(self, temp_dir):
        """Test recording a transformation."""
        lineage = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage"))
        
        lineage.record_transform(
            source_name="raw_data",
            target_name="validated_data",
            transform_type="validation",
            operation="Applied schema checks"
        )
        
        assert "validated_data" in lineage.lineage_graph
        assert len(lineage.lineage_graph["validated_data"]) == 1
        
        record = lineage.lineage_graph["validated_data"][0]
        assert record["source"] == "raw_data"
        assert record["transform_type"] == "validation"

    def test_get_lineage(self, temp_dir):
        """Test lineage retrieval."""
        lineage = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage"))
        
        lineage.record_transform("source1", "target1", "filter", "Removed nulls")
        lineage.record_transform("source2", "target1", "join", "Joined datasets")
        
        target_lineage = lineage.get_lineage("target1")
        
        assert len(target_lineage) == 2
        assert target_lineage[0]["source"] == "source1"
        assert target_lineage[1]["source"] == "source2"

    def test_get_upstream_lineage(self, temp_dir):
        """Test upstream lineage traversal."""
        lineage = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage"))
        
        # Create lineage chain: raw -> validated -> features
        lineage.record_transform("raw_data", "validated_data", "validation", "Validated")
        lineage.record_transform("validated_data", "featured_data", "feature_eng", "Features")
        lineage.record_transform("reference_data", "featured_data", "join", "Added reference")
        
        upstream = lineage.get_upstream_lineage("featured_data")
        
        assert upstream["dataset"] == "featured_data"
        assert "raw_data" in upstream["sources"]
        assert "validated_data" in upstream["sources"]
        assert "reference_data" in upstream["sources"]
        assert len(upstream["lineage"]) == 3

    def test_save_load_lineage_graph(self, temp_dir):
        """Test saving and loading lineage graph."""
        lineage = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage"))
        
        lineage.record_transform("source", "target", "filter", "Removed nulls")
        
        output_file = os.path.join(temp_dir, "lineage.json")
        lineage.save_lineage_graph(output_file)
        
        assert os.path.exists(output_file)
        
        # Create new lineage and load
        lineage2 = DataLineage(lineage_dir=os.path.join(temp_dir, "lineage2"))
        lineage2.load_lineage_graph(output_file)
        
        assert lineage2.lineage_graph == lineage.lineage_graph


class TestDataVersioning:
    """Test DataVersioning class."""

    def test_data_versioning_initialization(self, temp_dir):
        """Test DataVersioning initialization."""
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        assert dvc.dvc_repo_path == Path(temp_dir)
        assert dvc.metadata_manager is not None
        assert dvc.lineage_tracker is not None

    def test_create_dvc_pipeline(self, temp_dir):
        """Test DVC pipeline creation."""
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        pipeline_config = {
            "stages": {
                "prepare": {
                    "cmd": "python prepare.py",
                    "deps": ["data.csv"],
                    "outs": ["prepared.csv"]
                }
            }
        }
        
        output_file = dvc.create_dvc_pipeline(pipeline_config, "dvc.yaml")
        
        assert os.path.exists(output_file)
        
        # Verify YAML content
        import yaml
        with open(output_file, "r") as f:
            loaded_config = yaml.safe_load(f)
        
        assert "stages" in loaded_config
        assert "prepare" in loaded_config["stages"]

    def test_track_dataset(self, temp_dir, sample_parquet_file):
        """Test dataset tracking."""
        file_path, _ = sample_parquet_file
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        tracking_info = dvc.track_dataset(
            dataset_name="test_data",
            file_path=file_path,
            version_description="Test version",
            tags=["test"]
        )
        
        assert tracking_info["dataset_name"] == "test_data"
        assert "version_id" in tracking_info
        assert tracking_info["metadata"]["num_rows"] == 5

    def test_add_lineage(self, temp_dir):
        """Test adding lineage information."""
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        dvc.add_lineage(
            source_name="raw",
            target_name="validated",
            transform_type="validation",
            operation="Schema check"
        )
        
        lineage = dvc.get_dataset_lineage("validated")
        
        assert "raw" in lineage["sources"]
        assert len(lineage["lineage"]) > 0

    def test_generate_data_report(self, temp_dir, sample_parquet_file):
        """Test data report generation."""
        file_path, _ = sample_parquet_file
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        # Track a dataset
        dvc.track_dataset(
            dataset_name="test_data",
            file_path=file_path,
            version_description="Test"
        )
        
        # Add lineage
        dvc.add_lineage("source", "test_data", "validation", "Validated")
        
        # Generate report
        report_file = dvc.generate_data_report(
            output_file=os.path.join(temp_dir, "report.json")
        )
        
        assert os.path.exists(report_file)
        
        with open(report_file, "r") as f:
            report = json.load(f)
        
        assert "summary" in report
        assert "metadata_tracking" in report
        assert "lineage_graph" in report
        assert report["summary"]["total_datasets"] > 0

    def test_export_lineage_graph(self, temp_dir):
        """Test lineage graph export."""
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        dvc.add_lineage("source", "target", "filter", "Removed nulls")
        
        output_file = dvc.export_lineage_graph(
            output_file=os.path.join(temp_dir, "lineage.json")
        )
        
        assert os.path.exists(output_file)
        
        with open(output_file, "r") as f:
            graph = json.load(f)
        
        assert "target" in graph


class TestPhase1Pipeline:
    """Test Phase 1 pipeline configuration."""

    def test_create_phase1_pipeline(self):
        """Test Phase 1 pipeline configuration creation."""
        pipeline = create_phase1_pipeline()
        
        assert "stages" in pipeline
        assert "data_ingestion" in pipeline["stages"]
        assert "data_validation" in pipeline["stages"]
        assert "data_versioning" in pipeline["stages"]
        
        # Verify data_ingestion stage
        ingestion = pipeline["stages"]["data_ingestion"]
        assert "cmd" in ingestion
        assert "deps" in ingestion
        assert "outs" in ingestion
        
        # Verify data_validation stage
        validation = pipeline["stages"]["data_validation"]
        assert "cmd" in validation
        assert len(validation["deps"]) >= 4  # 3 parquet files + validators
        
        # Verify data_versioning stage
        versioning = pipeline["stages"]["data_versioning"]
        assert "cmd" in versioning
        assert "metrics" in versioning


class TestIntegration:
    """Integration tests for complete workflow."""

    def test_complete_versioning_workflow(self, temp_dir, sample_parquet_file):
        """Test complete data versioning workflow."""
        file_path, _ = sample_parquet_file
        dvc = DataVersioning(dvc_repo_path=temp_dir)
        
        # Track dataset
        tracking_info = dvc.track_dataset(
            dataset_name="workflow_test",
            file_path=file_path,
            version_description="Initial version"
        )
        
        # Add lineage
        dvc.add_lineage(
            source_name="raw_input",
            target_name="workflow_test",
            transform_type="validation",
            operation="Applied checks"
        )
        
        # Generate report
        report_file = dvc.generate_data_report(
            output_file=os.path.join(temp_dir, "report.json")
        )
        
        # Verify everything is in place
        assert os.path.exists(report_file)
        
        with open(report_file, "r") as f:
            report = json.load(f)
        
        assert report["summary"]["total_datasets"] > 0
        assert report["summary"]["total_versions"] > 0
        assert report["summary"]["lineage_entries"] > 0
        
        # Verify metadata directory
        metadata_files = list(dvc.metadata_manager.metadata_dir.glob("*.json"))
        assert len(metadata_files) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
