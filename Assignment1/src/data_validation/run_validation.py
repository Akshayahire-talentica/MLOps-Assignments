#!/usr/bin/env python3
"""
Data Validation Pipeline Runner (Spark Edition)
Data Validation Pipeline Runner (Great Expectations)

This runner now uses Great Expectations ONLY.
Legacy pandas-based validation has been replaced per project requirement.
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
import yaml

from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from src.data_validation.run_ge_validation import run_ge_validation

def setup_logging() -> logging.Logger:
    """Setup console logging"""
    logger = logging.getLogger('validation_pipeline')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def create_spark_session(app_name: str = "MovieLensValidation") -> SparkSession:
    """Initialize SparkSession"""
    return SparkSession.builder.appName(app_name).getOrCreate()

def main():
    """Main validation pipeline (Great Expectations only)"""
    parser = argparse.ArgumentParser(description='Run data validation pipeline (GE only)')
    parser.add_argument(
        '--ge-config',
        type=str,
        default='config/ge_validation.yaml',
        help='Path to Great Expectations config'
    )
    parser.add_argument(
        '--stage',
        type=str,
        default='processed',
        help='Stage to validate (raw, processed, features, etc.)'
    )
    parser.add_argument(
        '--force-profile',
        action='store_true',
        help='Force GE suite profiling even if suite exists'
    )
    parser.add_argument(
        '--skip-upload',
        action='store_true',
        help='Skip uploading GE reports to S3'
    )
    # Deprecated args preserved for compatibility
    parser.add_argument(
        '--config',
        type=str,
        default='config/validation_rules.yaml',
        help='(deprecated) legacy validation rules path'
    )
    parser.add_argument(
        '--ingestion-config',
        type=str,
        default='config/data_ingestion_config.yaml',
        help='(deprecated) legacy ingestion config path'
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("STARTING DATA VALIDATION PIPELINE (GE ONLY)")
    logger.info("=" * 80)

    if args.config != 'config/validation_rules.yaml' or args.ingestion_config != 'config/data_ingestion_config.yaml':
        logger.warning("Legacy args --config/--ingestion-config are ignored (GE-only mode).")

    try:
        exit_code = run_ge_validation(
            config_path=args.ge_config,
            stage=args.stage,
            force_profile=args.force_profile,
            skip_upload=args.skip_upload,
            logger=logger,
        )
        
        if exit_code == 0:
            logger.info("\n" + "=" * 80)
            logger.info("VALIDATION PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"[FAILED] VALIDATION FAILED: {str(e)}", exc_info=True)
        return 1
    finally:
        # Ensure DVC output directory exists
        validated_dir = Path('data/validated')
        validated_dir.mkdir(parents=True, exist_ok=True)
        (validated_dir / ".validated").touch()


if __name__ == '__main__':
    sys.exit(main())
