"""
Feature Engineering Pipeline Runner
===================================

Orchestrates the feature engineering pipeline for MovieLens data.
Handles data loading, feature creation, and output management.

Production-Ready Features:
- Automatic detection of data sources (raw .dat or processed Parquet)
- Comprehensive error handling and logging
- Configuration-driven execution
- Metadata tracking
- S3 upload support
"""

import sys
import logging
import json
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Ensure logs directory exists before configuring logging
Path('logs').mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/feature_engineering.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

from features.feature_engineering import FeatureEngineer


def load_config(config_path: str = 'config/data_ingestion_config.yaml') -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Add default feature engineering config if not present
        if 'features' not in config:
            config['features'] = {
                'version': 'v1',
                'output_path': 'data/features',
                'output_format': 'parquet',
                'save_csv': True
            }
        
        logger.info(f"Configuration loaded from {config_path}")
        return config
    
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise


def setup_directories():
    """Create necessary directories for feature engineering"""
    directories = [
        'data/features',
        'data/processed',
        'logs',
        'reports'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Directory ensured: {directory}")


def save_pipeline_metadata(result: Dict[str, Any], config: Dict[str, Any]):
    """Save pipeline execution metadata"""
    try:
        metadata = {
            'pipeline': 'feature_engineering',
            'timestamp': datetime.now().isoformat(),
            'status': result.get('status', 'UNKNOWN'),
            'feature_version': config.get('features', {}).get('version', 'v1'),
            'config': {
                'output_path': config.get('features', {}).get('output_path'),
                'output_format': config.get('features', {}).get('output_format')
            }
        }
        
        if result.get('status') == 'SUCCESS':
            metadata['file_paths'] = result.get('file_paths', {})
            metadata['statistics'] = result.get('statistics', {})
        else:
            metadata['error'] = result.get('error')
        
        # Save to reports directory
        reports_dir = Path('reports')
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_path = reports_dir / 'feature_engineering_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Pipeline metadata saved to {metadata_path}")
    
    except Exception as e:
        logger.warning(f"Failed to save pipeline metadata: {str(e)}")


def main():
    """Main execution function for feature engineering pipeline"""
    logger.info("=" * 100)
    logger.info("FEATURE ENGINEERING PIPELINE - STARTING")
    logger.info("=" * 100)
    
    start_time = datetime.now()
    
    try:
        # Setup
        setup_directories()
        config = load_config()
        
        # Initialize feature engineer
        logger.info("Initializing Feature Engineer...")
        engineer = FeatureEngineer(config)
        
        # Run feature engineering pipeline
        logger.info("Running feature engineering pipeline...")
        result = engineer.engineer_all_features()
        
        # Save metadata
        save_pipeline_metadata(result, config)
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log results
        logger.info("=" * 100)
        if result['status'] == 'SUCCESS':
            logger.info("[SUCCESS] FEATURE ENGINEERING PIPELINE - COMPLETED SUCCESSFULLY")
            logger.info(f"  Execution Time: {execution_time:.2f} seconds")
            logger.info(f"  Feature Version: {result['feature_version']}")
            logger.info(f"  Output Files:")
            for feature_type, path in result['file_paths'].items():
                logger.info(f"    - {feature_type}: {path}")
            
            # Print feature statistics summary
            stats = result.get('statistics', {})
            logger.info(f"  Feature Statistics:")
            for feature_type, feature_stats in stats.items():
                logger.info(f"    - {feature_type}: {feature_stats['shape']}")
        else:
            logger.error("[FAILED] FEATURE ENGINEERING PIPELINE - FAILED")
            logger.error(f"  Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        
        logger.info("=" * 100)
        
        # Output result as JSON for DVC
        print(json.dumps(result, indent=2, default=str))
        
        return 0
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        logger.info("=" * 100)
        logger.error("[FAILED] FEATURE ENGINEERING PIPELINE - FAILED WITH EXCEPTION")
        logger.info("=" * 100)
        sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
