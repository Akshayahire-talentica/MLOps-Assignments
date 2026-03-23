"""
Data Versioning & Lineage Tracking Module (Phase 1.3)

This module implements data versioning, lineage tracking, and metadata management
using DVC (Data Version Control) for the MLOps POC.

Features:
- DVC integration for reproducible data pipelines
- Automatic metadata capture (source, schema, row counts, checksums)
- Data lineage tracking (transforms and dependencies)
- Version history management
- Data quality metrics versioning
- Configuration management for versioned datasets

Classes:
- DataVersioning: Main class for data versioning and DVC integration
- DataLineage: Tracks data transformation lineage
- MetadataManager: Manages dataset metadata and versions
"""

import os
import json
import hashlib
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

import pandas as pd
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetadataManager:
    """Manages metadata for datasets including versioning and history."""

    def __init__(self, metadata_dir: str = ".dvc/metadata"):
        """
        Initialize MetadataManager.

        Args:
            metadata_dir: Directory to store metadata files
        """
        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_cache: Dict[str, Dict[str, Any]] = {}

    def compute_file_hash(self, file_path: str) -> str:
        """
        Compute SHA256 hash of a file.

        Args:
            file_path: Path to the file

        Returns:
            SHA256 hash of the file
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def extract_parquet_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from a Parquet file.

        Args:
            file_path: Path to the Parquet file

        Returns:
            Dictionary containing metadata
        """
        metadata = {
            "file_path": str(file_path),
            "file_size": os.path.getsize(file_path),
            "file_hash": self.compute_file_hash(file_path),
            "timestamp": datetime.now().isoformat(),
        }

        try:
            # Read Parquet file metadata
            parquet_file = pq.ParquetFile(file_path)
            metadata["num_rows"] = parquet_file.metadata.num_rows
            metadata["num_columns"] = parquet_file.metadata.num_columns
            metadata["columns"] = parquet_file.schema.names
            metadata["compression"] = parquet_file.metadata.format_version

            # Read actual data to get statistics
            df = pd.read_parquet(file_path)
            metadata["dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
            metadata["null_counts"] = {col: int(df[col].isna().sum()) for col in df.columns}
            metadata["shape"] = list(df.shape)

        except Exception as e:
            logger.warning(f"Error extracting Parquet metadata from {file_path}: {e}")
            metadata["error"] = str(e)

        return metadata

    def create_version(
        self,
        dataset_name: str,
        file_path: str,
        description: str = "",
        tags: Optional[List[str]] = None,
    ) -> str:
        """
        Create a new version for a dataset.

        Args:
            dataset_name: Name of the dataset
            file_path: Path to the data file
            description: Version description
            tags: Optional list of tags

        Returns:
            Version ID (timestamp-based)
        """
        version_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        metadata = self.extract_parquet_metadata(file_path)
        metadata["dataset_name"] = dataset_name
        metadata["description"] = description
        metadata["tags"] = tags or []
        metadata["version_id"] = version_id

        # Store metadata
        metadata_file = self.metadata_dir / f"{dataset_name}_{version_id}.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        # Update cache
        self.metadata_cache[f"{dataset_name}_{version_id}"] = metadata

        logger.info(f"Created version {version_id} for dataset {dataset_name}")
        return version_id

    def get_version_history(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Get version history for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            List of version metadata dictionaries
        """
        history = []
        metadata_files = self.metadata_dir.glob(f"{dataset_name}_*.json")

        for metadata_file in sorted(metadata_files, reverse=True):
            with open(metadata_file, "r") as f:
                history.append(json.load(f))

        return history

    def compare_versions(self, dataset_name: str, version1: str, version2: str) -> Dict[str, Any]:
        """
        Compare two versions of a dataset.

        Args:
            dataset_name: Name of the dataset
            version1: First version ID
            version2: Second version ID

        Returns:
            Dictionary with comparison results
        """
        file1 = self.metadata_dir / f"{dataset_name}_{version1}.json"
        file2 = self.metadata_dir / f"{dataset_name}_{version2}.json"

        if not file1.exists() or not file2.exists():
            raise FileNotFoundError(f"Version metadata not found")

        with open(file1, "r") as f:
            meta1 = json.load(f)
        with open(file2, "r") as f:
            meta2 = json.load(f)

        comparison = {
            "version1": version1,
            "version2": version2,
            "changes": {}
        }

        # Compare key metrics
        for key in ["num_rows", "num_columns", "file_size", "file_hash"]:
            if key in meta1 and key in meta2:
                comparison["changes"][key] = {
                    "version1": meta1[key],
                    "version2": meta2[key],
                    "changed": meta1[key] != meta2[key]
                }

        return comparison


class DataLineage:
    """Tracks data transformation lineage and dependencies."""

    def __init__(self, lineage_dir: str = ".dvc/lineage"):
        """
        Initialize DataLineage tracker.

        Args:
            lineage_dir: Directory to store lineage files
        """
        self.lineage_dir = Path(lineage_dir)
        self.lineage_dir.mkdir(parents=True, exist_ok=True)
        self.lineage_graph: Dict[str, List[Dict[str, Any]]] = {}

    def record_transform(
        self,
        source_name: str,
        target_name: str,
        transform_type: str,
        operation: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a data transformation in the lineage.

        Args:
            source_name: Name of source dataset
            target_name: Name of target dataset
            transform_type: Type of transformation (e.g., 'filter', 'join', 'aggregate')
            operation: Description of the operation
            metadata: Additional metadata about the transformation
        """
        if target_name not in self.lineage_graph:
            self.lineage_graph[target_name] = []

        lineage_record = {
            "source": source_name,
            "target": target_name,
            "transform_type": transform_type,
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }

        self.lineage_graph[target_name].append(lineage_record)
        logger.info(f"Recorded lineage: {source_name} -> {target_name} ({transform_type})")

    def get_lineage(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Get lineage for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            List of lineage records
        """
        return self.lineage_graph.get(dataset_name, [])

    def get_upstream_lineage(self, dataset_name: str, depth: int = 5) -> Dict[str, Any]:
        """
        Get upstream lineage (all sources) for a dataset.

        Args:
            dataset_name: Name of the dataset
            depth: Maximum depth to traverse

        Returns:
            Dictionary with upstream lineage
        """
        upstream = {
            "dataset": dataset_name,
            "lineage": [],
            "sources": set()
        }

        def traverse(name: str, current_depth: int = 0):
            if current_depth >= depth:
                return

            for record in self.get_lineage(name):
                source = record["source"]
                upstream["lineage"].append(record)
                upstream["sources"].add(source)
                traverse(source, current_depth + 1)

        traverse(dataset_name)
        upstream["sources"] = list(upstream["sources"])
        return upstream

    def save_lineage_graph(self, output_file: Optional[str] = None) -> str:
        """
        Save lineage graph to a file.

        Args:
            output_file: Output file path (default: .dvc/lineage/graph.json)

        Returns:
            Path to the saved file
        """
        output_file = output_file or str(self.lineage_dir / "graph.json")
        with open(output_file, "w") as f:
            json.dump(self.lineage_graph, f, indent=2, default=str)

        logger.info(f"Saved lineage graph to {output_file}")
        return output_file

    def load_lineage_graph(self, input_file: str) -> None:
        """
        Load lineage graph from a file.

        Args:
            input_file: Input file path
        """
        with open(input_file, "r") as f:
            self.lineage_graph = json.load(f)

        logger.info(f"Loaded lineage graph from {input_file}")


class DataVersioning:
    """Main class for data versioning and DVC integration."""

    def __init__(self, dvc_repo_path: str = "."):
        """
        Initialize DataVersioning.

        Args:
            dvc_repo_path: Path to DVC repository (default: current directory)
        """
        self.dvc_repo_path = Path(dvc_repo_path)
        self.metadata_manager = MetadataManager()
        self.lineage_tracker = DataLineage()
        self.version_config: Dict[str, Any] = {}

        logger.info(f"Initialized DataVersioning for repository: {dvc_repo_path}")

    def create_dvc_pipeline(
        self,
        pipeline_config: Dict[str, Any],
        output_file: str = "dvc.yaml"
    ) -> str:
        """
        Create a DVC pipeline configuration.

        Args:
            pipeline_config: Pipeline configuration dictionary
            output_file: Output file path

        Returns:
            Path to the created DVC pipeline file
        """
        output_path = self.dvc_repo_path / output_file

        # Write DVC pipeline YAML
        with open(output_path, "w") as f:
            yaml.dump(pipeline_config, f, default_flow_style=False)

        logger.info(f"Created DVC pipeline configuration: {output_path}")
        return str(output_path)

    def track_dataset(
        self,
        dataset_name: str,
        file_path: str,
        version_description: str = "",
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Track a dataset with DVC.

        Args:
            dataset_name: Name of the dataset
            file_path: Path to the dataset file
            version_description: Description of this version
            tags: Optional tags for the version

        Returns:
            Dictionary with tracking information
        """
        # Create version
        version_id = self.metadata_manager.create_version(
            dataset_name=dataset_name,
            file_path=file_path,
            description=version_description,
            tags=tags
        )

        tracking_info = {
            "dataset_name": dataset_name,
            "version_id": version_id,
            "file_path": str(file_path),
            "timestamp": datetime.now().isoformat(),
            "metadata": self.metadata_manager.extract_parquet_metadata(file_path)
        }

        return tracking_info

    def add_lineage(
        self,
        source_name: str,
        target_name: str,
        transform_type: str,
        operation: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add lineage information for a data transformation.

        Args:
            source_name: Source dataset name
            target_name: Target dataset name
            transform_type: Type of transformation
            operation: Description of the operation
            metadata: Additional metadata
        """
        self.lineage_tracker.record_transform(
            source_name=source_name,
            target_name=target_name,
            transform_type=transform_type,
            operation=operation,
            metadata=metadata
        )

    def get_dataset_lineage(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get complete lineage for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dictionary with lineage information
        """
        return self.lineage_tracker.get_upstream_lineage(dataset_name)

    def get_version_history(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Get version history for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            List of version records
        """
        return self.metadata_manager.get_version_history(dataset_name)

    def generate_data_report(self, output_file: str = "data_versioning_report.json") -> str:
        """
        Generate a comprehensive data versioning report.

        Args:
            output_file: Output file path

        Returns:
            Path to the generated report
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "metadata_tracking": {},
            "lineage_graph": self.lineage_tracker.lineage_graph,
            "summary": {}
        }

        # Add metadata information
        metadata_files = self.metadata_manager.metadata_dir.glob("*.json")
        for metadata_file in metadata_files:
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
                dataset_name = metadata.get("dataset_name", "unknown")
                if dataset_name not in report["metadata_tracking"]:
                    report["metadata_tracking"][dataset_name] = []
                report["metadata_tracking"][dataset_name].append(metadata)

        # Generate summary
        report["summary"]["total_datasets"] = len(report["metadata_tracking"])
        report["summary"]["total_versions"] = sum(
            len(versions) for versions in report["metadata_tracking"].values()
        )
        report["summary"]["lineage_entries"] = sum(
            len(lineage) for lineage in report["lineage_graph"].values()
        )

        # Write report
        output_path = self.dvc_repo_path / output_file
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Generated data versioning report: {output_path}")
        return str(output_path)

    def export_lineage_graph(self, output_file: str = ".dvc/lineage/graph.json") -> str:
        """
        Export lineage graph to a file.

        Args:
            output_file: Output file path

        Returns:
            Path to the exported file
        """
        return self.lineage_tracker.save_lineage_graph(output_file)


def create_phase1_pipeline() -> Dict[str, Any]:
    """
    Create DVC pipeline configuration for Phase 1.

    Returns:
        DVC pipeline configuration dictionary
    """
    pipeline_config = {
        "stages": {
            "data_ingestion": {
                "cmd": "python3 src/data_ingestion/run_ingestion.py",
                "deps": ["src/data_ingestion/ingest_movies.py"],
                "outs": [
                    {"data/raw/movies.parquet": {"cache": True}},
                    {"data/raw/ratings.parquet": {"cache": True}},
                    {"data/raw/users.parquet": {"cache": True}},
                ],
                "metrics": [{"data/ingestion_metrics.json": {"cache": False}}],
            },
            "data_validation": {
                "cmd": "python3 src/data_validation/run_validation.py",
                "deps": [
                    "data/raw/movies.parquet",
                    "data/raw/ratings.parquet",
                    "data/raw/users.parquet",
                    "src/data_validation/validators.py",
                    "config/validation_rules.yaml",
                ],
                "outs": [{"data/validated": {"cache": True}}],
                "metrics": [{"data/validation_metrics.json": {"cache": False}}],
            },
            "data_versioning": {
                "cmd": "python3 src/data_ingestion/run_versioning.py",
                "deps": [
                    "data/validated",
                    "src/data_ingestion/data_versioning.py",
                ],
                "metrics": [
                    {".dvc/metadata": {"cache": False}},
                    {"data_versioning_report.json": {"cache": False}},
                ],
            },
        }
    }
    return pipeline_config
