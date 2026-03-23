#!/usr/bin/env python3
"""
Data Ingestion Pipeline Runner (Spark Edition)
==============================================

This script orchestrates the complete data ingestion pipeline using Apache Spark.
"""

import os
import sys
import yaml
import logging
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from src.data_ingestion.ingest_movies import MovieLensDataIngestor

def setup_logging() -> logging.Logger:
    """Setup console logging"""
    logger = logging.getLogger('ingestion_pipeline')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def create_spark_session(app_name: str = "MovieLensIngestion") -> SparkSession:
    """Initialize SparkSession with S3 support"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark

def run_pipeline(config_path: str = 'config/data_ingestion_config.yaml'):
    """Run data ingestion pipeline"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("STARTING MOVIELENS DATA INGESTION PIPELINE (SPARK)")
    logger.info("=" * 80)

    spark = None
    try:
        # Load config
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Initialize Spark
        spark = create_spark_session()
        logger.info("[SPARK] SparkSession initialized")

        # Initialize ingestor
        ingestor = MovieLensDataIngestor(config, spark)

        # Run ingestion
        results = ingestor.ingest_all()

        # Create final report and upload to S3
        s3_config = config.get('s3', {})
        if s3_config.get('enabled', False):
            storage = ingestor.s3_storage
            metadata_prefix = s3_config.get('metadata_prefix', 'metadata/')
            s3_key = f"{metadata_prefix}ingestion_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

            report = {
                'timestamp': datetime.now().isoformat(),
                'status': 'SUCCESS',
                'engine': 'Apache Spark',
                'summary': {
                    'datasets_processed': len(results),
                    'total_rows': sum(m.row_count for k, (df, m) in results.items())
                }
            }

            temp_report = "report_temp.json"
            with open(temp_report, 'w') as f:
                json.dump(report, f, indent=2)

            logger.info(f"[S3] Uploading ingestion report to s3://{storage.bucket_name}/{s3_key}")
            storage.upload_file(temp_report, s3_key)

            if os.path.exists(temp_report):
                os.remove(temp_report)

        logger.info("\n" + "=" * 80)
        logger.info("INGESTION PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"[FAILED] INGESTION FAILED: {str(e)}", exc_info=True)
        return 1
    finally:
        if spark:
            spark.stop()
            logger.info("[SPARK] SparkSession stopped")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config/data_ingestion_config.yaml')
    args = parser.parse_args()
    
    sys.exit(run_pipeline(args.config))
