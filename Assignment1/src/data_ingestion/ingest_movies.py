"""
Data Ingestion Module with Apache Spark
=======================================

Responsible for loading raw data files and storing in production-ready format using PySpark.
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass
from enum import Enum

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F

from src.data_ingestion.s3_storage import S3DataStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSource(Enum):
    """Supported data sources"""
    MOVIES = "movies"
    RATINGS = "ratings"
    USERS = "users"

@dataclass
class IngestionMetadata:
    """Metadata for ingested dataset"""
    source_file: str
    output_file: str
    row_count: int
    column_count: int
    processing_time_ms: float
    data_version: str
    timestamp: str
    
    def to_dict(self) -> Dict:
        return {
            'source_file': self.source_file,
            'output_file': self.output_file,
            'row_count': self.row_count,
            'column_count': self.column_count,
            'processing_time_ms': self.processing_time_ms,
            'data_version': self.data_version,
            'timestamp': self.timestamp
        }

class MovieLensDataIngestor:
    """
    Ingests MovieLens dataset files using Apache Spark.
    """
    
    # Spark Schema definitions
    MOVIES_SCHEMA = T.StructType([
        T.StructField("MovieID", T.IntegerType(), False),
        T.StructField("Title", T.StringType(), True),
        T.StructField("Genres", T.StringType(), True)
    ])
    
    RATINGS_SCHEMA = T.StructType([
        T.StructField("UserID", T.IntegerType(), False),
        T.StructField("MovieID", T.IntegerType(), False),
        T.StructField("Rating", T.FloatType(), True),
        T.StructField("Timestamp", T.LongType(), True)
    ])
    
    USERS_SCHEMA = T.StructType([
        T.StructField("UserID", T.IntegerType(), False),
        T.StructField("Gender", T.StringType(), True),
        T.StructField("Age", T.IntegerType(), True),
        T.StructField("Occupation", T.IntegerType(), True),
        T.StructField("ZipCode", T.StringType(), True)
    ])
    
    def __init__(self, config: Dict, spark: SparkSession):
        """
        Initialize data ingestor with configuration.
        
        Args:
            config: Configuration dictionary
            spark: Active SparkSession
        """
        self.config = config
        self.spark = spark
        self.data_sources = config.get('data_sources', {})
        self.output_config = config.get('output', {})
        self.metadata_list = []
        
        # S3 Config
        self.s3_config = config.get('s3', {})
        self.s3_enabled = self.s3_config.get('enabled', False)
        
        # Initialize S3 storage if enabled
        self.s3_storage = None
        if self.s3_enabled:
            from src.data_ingestion.s3_storage import S3DataStorage
            self.s3_storage = S3DataStorage(
                bucket_name=self.s3_config.get('bucket_name'),
                region=self.s3_config.get('region', 'ap-south-1'),
                profile=self.s3_config.get('profile')
            )
        
        # Create output directories
        self._create_output_dirs()
        
    def _create_output_dirs(self):
        """Create necessary output directories"""
        output_path = self.output_config.get('storage_path', 'data/raw/ingested')
        Path(output_path).mkdir(parents=True, exist_ok=True)
        
    def _get_data_version(self) -> str:
        """Generate data version identifier"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def _load_data(self, dataset_name: str, schema: T.StructType) -> DataFrame:
        """Load raw data into Spark DataFrame"""
        dataset_config = self.data_sources.get(dataset_name, {})
        input_path = dataset_config.get('path')
        delimiter = dataset_config.get('delimiter', '::')
        
        # In a real Spark EKS setup, we would use s3a:// direct reading
        # For this POC, we'll read the local files (which DVC ensures are present)
        logger.info(f"Loading {dataset_name} from {input_path} using Spark...")
        
        return self.spark.read.format("csv") \
            .option("sep", delimiter) \
            .option("header", "false") \
            .option("encoding", "iso-8859-1") \
            .schema(schema) \
            .load(input_path)

    def ingest_dataset(self, dataset_name: str, schema: T.StructType) -> Tuple[DataFrame, IngestionMetadata]:
        """Generic ingestion logic for Spark"""
        logger.info(f"Ingesting {dataset_name}...")
        start_time = datetime.now()
        
        df = self._load_data(dataset_name, schema)
        row_count = df.count()
        col_count = len(df.columns)
        
        output_path = self._get_output_path(dataset_name)
        
        # Write to Parquet using Spark (distributed)
        df.write.mode("overwrite").parquet(output_path)
        
        # Upload to S3 if enabled
        if self.s3_enabled and self.s3_storage:
            # Note: For Spark, the output_path is a directory. 
            # In a real Spark setup, we'd use S3A to write directly.
            # Here, we'll upload the local directory contents to S3.
            prefix = self.s3_config.get('processed_data_prefix', 'processed/')
            for file in Path(output_path).glob("*.parquet"):
                s3_key = f"{prefix}{dataset_name}/{self._get_data_version()}/{file.name}"
                self.s3_storage.upload_file(str(file), s3_key)

        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        data_version = self._get_data_version()
        
        metadata = IngestionMetadata(
            source_file=self.data_sources.get(dataset_name, {}).get('path'),
            output_file=output_path,
            row_count=row_count,
            column_count=col_count,
            processing_time_ms=processing_time,
            data_version=data_version,
            timestamp=start_time.isoformat()
        )
        
        self.metadata_list.append(metadata)
        logger.info(f"[OK] {dataset_name} ingestion completed: {row_count} rows")
        
        return df, metadata

    def _get_output_path(self, dataset_name: str) -> str:
        output_dir = self.output_config.get('storage_path', 'data/processed')
        data_version = self._get_data_version()
        
        dataset_dir = Path(output_dir) / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Spark writes to a directory
        return str(dataset_dir / data_version)

    def ingest_all(self) -> Dict:
        """Run all ingestion jobs"""
        results = {}
        results['movies'] = self.ingest_dataset('movies', self.MOVIES_SCHEMA)
        results['ratings'] = self.ingest_dataset('ratings', self.RATINGS_SCHEMA)
        results['users'] = self.ingest_dataset('users', self.USERS_SCHEMA)
        
        return results
