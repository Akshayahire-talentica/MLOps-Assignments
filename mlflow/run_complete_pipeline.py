#!/usr/bin/env python3
"""
Master MLOps Pipeline Orchestrator
==================================

Orchestrates the complete ML lifecycle:
1. Data Ingestion (S3 + DVC)
2. PySpark ETL Processing
3. Great Expectations Validation
4. Feature Engineering
5. Model Training (Scikit-learn + MLflow)
6. Model Evaluation & Registry
7. Drift Detection (Evidently AI)
8. Auto-Retraining (if drift detected)

Usage:
    python run_complete_pipeline.py
    python run_complete_pipeline.py --skip-ingestion
    python run_complete_pipeline.py --force-retrain
"""

import argparse
import json
import logging
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLOpsPipeline:
    """Complete MLOps pipeline orchestrator"""
    
    def __init__(self, config_path: str = 'config/data_ingestion_config.yaml'):
        self.config_path = config_path
        self.results: Dict[str, Any] = {}
        self.start_time = datetime.now()
        
    def run_command(self, cmd: List[str], stage_name: str) -> Dict[str, Any]:
        """Execute a pipeline stage command"""
        logger.info(f"{'='*80}")
        logger.info(f"STAGE: {stage_name}")
        logger.info(f"{'='*80}")
        logger.info(f"Command: {' '.join(cmd)}")
        
        stage_start = datetime.now()
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            
            duration = (datetime.now() - stage_start).total_seconds()
            
            logger.info(f"✅ {stage_name} completed successfully in {duration:.2f}s")
            
            return {
                'status': 'SUCCESS',
                'duration_seconds': duration,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
        except subprocess.CalledProcessError as e:
            duration = (datetime.now() - stage_start).total_seconds()
            
            logger.error(f"❌ {stage_name} failed after {duration:.2f}s")
            logger.error(f"Error: {e.stderr}")
            
            return {
                'status': 'FAILED',
                'duration_seconds': duration,
                'error': str(e),
                'stderr': e.stderr
            }
    
    def stage_1_data_ingestion(self) -> Dict[str, Any]:
        """Stage 1: Data ingestion with PySpark ETL"""
        return self.run_command(
            ['python', 'src/data_ingestion/run_ingestion.py'],
            'Data Ingestion & PySpark ETL'
        )
    
    def stage_2_data_validation(self) -> Dict[str, Any]:
        """Stage 2: Great Expectations validation"""
        return self.run_command(
            ['python', 'src/data_validation/run_ge_validation.py', '--stage', 'processed'],
            'Great Expectations Validation'
        )
    
    def stage_3_feature_engineering(self) -> Dict[str, Any]:
        """Stage 3: Feature engineering"""
        return self.run_command(
            ['python', 'src/features/feature_engineering.py'],
            'Feature Engineering'
        )
    
    def stage_4_model_training(self) -> Dict[str, Any]:
        """Stage 4: Model training with MLflow"""
        return self.run_command(
            ['python', 'src/training/model_trainer.py'],
            'Model Training & MLflow Registry'
        )
    
    def stage_5_model_selection(self) -> Dict[str, Any]:
        """Stage 5: Model evaluation and selection"""
        return self.run_command(
            ['python', 'src/training/model_selection.py'],
            'Model Evaluation & Selection'
        )
    
    def stage_6_drift_detection(self) -> Dict[str, Any]:
        """Stage 6: Drift detection with Evidently AI"""
        result = self.run_command(
            ['python', 'src/monitoring/drift_detector.py'],
            'Drift Detection (Evidently AI)'
        )
        
        # Check if critical drift was detected
        try:
            drift_report_files = sorted(Path('reports/drift').glob('drift_report_latest.json'))
            if drift_report_files:
                with open(drift_report_files[0], 'r') as f:
                    drift_report = json.load(f)
                    result['drift_severity'] = drift_report.get('severity', 'unknown')
                    result['drift_detected'] = drift_report.get('drift_detected', False)
                    result['drift_score'] = drift_report.get('max_drift_score', 0.0)
        except Exception as e:
            logger.warning(f"Could not parse drift report: {e}")
        
        return result
    
    def run_full_pipeline(self, skip_ingestion: bool = False, 
                         force_retrain: bool = False) -> Dict[str, Any]:
        """Execute complete ML pipeline"""
        logger.info("="*80)
        logger.info("STARTING COMPLETE MLOPS PIPELINE")
        logger.info("="*80)
        logger.info(f"Start time: {self.start_time.isoformat()}")
        logger.info(f"Configuration: {self.config_path}")
        logger.info(f"Skip ingestion: {skip_ingestion}")
        logger.info(f"Force retrain: {force_retrain}")
        logger.info("="*80)
        
        # Stage 1: Data Ingestion
        if not skip_ingestion:
            self.results['data_ingestion'] = self.stage_1_data_ingestion()
            if self.results['data_ingestion']['status'] == 'FAILED':
                logger.error("Pipeline failed at Data Ingestion stage")
                return self._generate_summary()
        else:
            logger.info("⏭️  Skipping data ingestion stage")
        
        # Stage 2: Data Validation
        self.results['data_validation'] = self.stage_2_data_validation()
        if self.results['data_validation']['status'] == 'FAILED':
            logger.error("Pipeline failed at Data Validation stage")
            return self._generate_summary()
        
        # Stage 3: Feature Engineering
        self.results['feature_engineering'] = self.stage_3_feature_engineering()
        if self.results['feature_engineering']['status'] == 'FAILED':
            logger.error("Pipeline failed at Feature Engineering stage")
            return self._generate_summary()
        
        # Stage 4: Model Training
        self.results['model_training'] = self.stage_4_model_training()
        if self.results['model_training']['status'] == 'FAILED':
            logger.error("Pipeline failed at Model Training stage")
            return self._generate_summary()
        
        # Stage 5: Model Selection
        self.results['model_selection'] = self.stage_5_model_selection()
        if self.results['model_selection']['status'] == 'FAILED':
            logger.error("Pipeline failed at Model Selection stage")
            return self._generate_summary()
        
        # Stage 6: Drift Detection
        self.results['drift_detection'] = self.stage_6_drift_detection()
        
        # Check if retraining is needed due to drift
        drift_result = self.results['drift_detection']
        if drift_result.get('drift_severity') == 'critical' or force_retrain:
            logger.warning("⚠️  Critical drift detected or forced retrain - triggering retraining")
            
            # Re-run training and selection
            self.results['retrain_training'] = self.stage_4_model_training()
            self.results['retrain_selection'] = self.stage_5_model_selection()
            
            logger.info("✅ Retraining completed")
        
        return self._generate_summary()
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate pipeline execution summary"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        # Count successful stages
        successful_stages = sum(
            1 for result in self.results.values() 
            if isinstance(result, dict) and result.get('status') == 'SUCCESS'
        )
        total_stages = len(self.results)
        
        # Determine overall status
        overall_status = 'SUCCESS' if successful_stages == total_stages else 'PARTIAL_SUCCESS'
        if successful_stages == 0:
            overall_status = 'FAILED'
        
        summary = {
            'pipeline_name': 'Complete MLOps Pipeline',
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_duration_seconds': total_duration,
            'overall_status': overall_status,
            'stages_completed': successful_stages,
            'stages_total': total_stages,
            'stage_results': self.results
        }
        
        # Save summary
        report_dir = Path('reports/pipeline')
        report_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = report_dir / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info("")
        logger.info("="*80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Overall Status: {overall_status}")
        logger.info(f"Stages Completed: {successful_stages}/{total_stages}")
        logger.info(f"Total Duration: {total_duration:.2f}s")
        logger.info(f"Report saved: {report_file}")
        logger.info("="*80)
        
        # Print stage-by-stage results
        for stage_name, result in self.results.items():
            if isinstance(result, dict):
                status_icon = "✅" if result.get('status') == 'SUCCESS' else "❌"
                duration = result.get('duration_seconds', 0)
                logger.info(f"{status_icon} {stage_name}: {result.get('status')} ({duration:.2f}s)")
        
        logger.info("="*80)
        
        return summary


def main():
    parser = argparse.ArgumentParser(description='Run complete MLOps pipeline')
    parser.add_argument(
        '--skip-ingestion',
        action='store_true',
        help='Skip data ingestion stage (use existing data)'
    )
    parser.add_argument(
        '--force-retrain',
        action='store_true',
        help='Force model retraining regardless of drift'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/data_ingestion_config.yaml',
        help='Configuration file path'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = MLOpsPipeline(config_path=args.config)
    
    # Run pipeline
    summary = pipeline.run_full_pipeline(
        skip_ingestion=args.skip_ingestion,
        force_retrain=args.force_retrain
    )
    
    # Exit with appropriate code
    if summary['overall_status'] == 'SUCCESS':
        sys.exit(0)
    elif summary['overall_status'] == 'PARTIAL_SUCCESS':
        sys.exit(2)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
