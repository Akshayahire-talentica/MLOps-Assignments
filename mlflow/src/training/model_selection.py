"""
Model Selection Module
======================

Compares trained models and selects the best one.
Uses MLflow runs for model evaluation and comparison.

Input: MLflow runs with trained models
Output: Best model chosen and documented
"""

import pandas as pd
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import mlflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelSelector:
    """Selects best model from MLflow experiments"""
    
    def __init__(self, config: Dict[str, Any], experiment_name: str = "phase2-training"):
        """Initialize model selector"""
        self.config = config.get('training', {})
        self.experiment_name = experiment_name
        self.report_dir = Path(self.config.get('report_dir', 'reports'))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("ModelSelector initialized")
    
    def get_experiment_runs(self) -> pd.DataFrame:
        """Retrieve all runs from experiment"""
        logger.info(f"Retrieving runs from experiment: {self.experiment_name}...")
        
        try:
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            if not experiment:
                raise ValueError(f"Experiment '{self.experiment_name}' not found")
            
            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
            
            if runs.empty:
                raise ValueError("No runs found in experiment")
            
            logger.info(f"Found {len(runs)} runs")
            return runs
        
        except Exception as e:
            logger.error(f"Failed to retrieve runs: {str(e)}", exc_info=True)
            raise
    
    def evaluate_runs(self, runs: pd.DataFrame) -> Dict[str, Any]:
        """Evaluate and compare all runs"""
        logger.info("Evaluating runs...")
        
        try:
            evaluation_results = []
            
            for idx, row in runs.iterrows():
                run_id = row['run_id']
                run_name = row.get('tags.mlflow.runName', 'unknown')
                
                # Extract metrics
                metrics = {
                    'rmse': row.get('metrics.reconstruction_rmse'),
                    'mae': row.get('metrics.reconstruction_mae'),
                    'baseline_rmse': row.get('metrics.rmse'),
                    'baseline_mae': row.get('metrics.mae'),
                }
                
                # Extract parameters
                params = {
                    'model_type': row.get('params.model_type'),
                    'n_components': row.get('params.n_components'),
                }
                
                run_result = {
                    'rank': idx + 1,
                    'run_id': run_id,
                    'run_name': run_name,
                    'status': row['status'],
                    'metrics': metrics,
                    'params': params,
                    'start_time': row.get('start_time'),
                    'end_time': row.get('end_time')
                }
                
                evaluation_results.append(run_result)
                
                logger.info(f"Run {idx + 1}: {run_name} (RMSE: {metrics.get('rmse')}, MAE: {metrics.get('mae')})")
            
            return {
                'total_runs': len(evaluation_results),
                'runs': evaluation_results
            }
        
        except Exception as e:
            logger.error(f"Failed to evaluate runs: {str(e)}", exc_info=True)
            raise
    
    def compare_models(self, evaluation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Compare models and create comparison metrics"""
        logger.info("Comparing models...")
        
        try:
            runs = evaluation_results['runs']
            
            # Calculate scores for each model
            comparison_data = []
            
            for run in runs:
                metrics = run['metrics']
                
                # Calculate overall score (lower RMSE is better)
                rmse = metrics.get('rmse')
                mae = metrics.get('mae')
                
                if rmse and mae:
                    # Normalize and combine metrics
                    score = (rmse + mae) / 2
                else:
                    score = float('inf')
                
                comparison_data.append({
                    'run_id': run['run_id'],
                    'run_name': run['run_name'],
                    'model_type': run['params'].get('model_type'),
                    'rmse': rmse,
                    'mae': mae,
                    'score': score,
                    'params': run['params']
                })
            
            # Sort by score
            comparison_data.sort(key=lambda x: x['score'] if isinstance(x['score'], (int, float)) else float('inf'))
            
            # Create comparison report
            comparison_report = {
                'timestamp': datetime.now().isoformat(),
                'total_models': len(comparison_data),
                'models': comparison_data,
                'comparison_metrics': {
                    'best_score': comparison_data[0]['score'] if comparison_data else None,
                    'worst_score': comparison_data[-1]['score'] if comparison_data else None,
                    'average_score': sum(m['score'] for m in comparison_data if isinstance(m['score'], (int, float))) / len(comparison_data) if comparison_data else None
                }
            }
            
            return comparison_report
        
        except Exception as e:
            logger.error(f"Failed to compare models: {str(e)}", exc_info=True)
            raise
    
    def select_best_model(self, comparison_report: Dict[str, Any]) -> Dict[str, Any]:
        """Select best model based on metrics"""
        logger.info("Selecting best model...")
        
        try:
            models = comparison_report['models']
            
            if not models:
                raise ValueError("No models to select from")
            
            # Best model is first in sorted list (lowest score)
            best_model = models[0]
            
            selection_result = {
                'status': 'SUCCESS',
                'selected_model': {
                    'run_id': best_model['run_id'],
                    'run_name': best_model['run_name'],
                    'model_type': best_model['model_type'],
                    'metrics': {
                        'rmse': best_model['rmse'],
                        'mae': best_model['mae'],
                        'score': best_model['score']
                    },
                    'parameters': best_model['params']
                },
                'runner_up_models': [
                    {
                        'run_id': m['run_id'],
                        'run_name': m['run_name'],
                        'score': m['score']
                    }
                    for m in models[1:3]  # Top 2 runner-ups
                ],
                'selection_criteria': 'Lowest combined RMSE + MAE score',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Best model selected: {best_model['run_name']} (Score: {best_model['score']:.4f})")
            
            return selection_result
        
        except Exception as e:
            logger.error(f"Failed to select best model: {str(e)}", exc_info=True)
            raise
    
    def generate_selection_report(self, selection_result: Dict[str, Any], 
                                 comparison_report: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed selection report"""
        logger.info("Generating selection report...")
        
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'experiment_name': self.experiment_name,
                'selection_summary': selection_result,
                'model_comparison': comparison_report,
                'recommendations': [
                    f"Selected model: {selection_result['selected_model']['run_name']}",
                    f"Model type: {selection_result['selected_model']['model_type']}",
                    f"Performance score: {selection_result['selected_model']['metrics']['score']:.4f}",
                    f"RMSE: {selection_result['selected_model']['metrics']['rmse']:.4f}",
                    f"MAE: {selection_result['selected_model']['metrics']['mae']:.4f}"
                ]
            }
            
            # Save report
            report_path = self.report_dir / f"model_selection_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"Selection report saved to {report_path}")
            
            return report
        
        except Exception as e:
            logger.error(f"Failed to generate report: {str(e)}", exc_info=True)
            raise
    
    def select_best_model_complete(self) -> Dict[str, Any]:
        """Complete model selection pipeline"""
        logger.info("=" * 80)
        logger.info("STARTING MODEL SELECTION")
        logger.info("=" * 80)
        
        try:
            # Get runs from experiment
            runs = self.get_experiment_runs()
            
            # Evaluate runs
            evaluation_results = self.evaluate_runs(runs)
            
            # Compare models
            comparison_report = self.compare_models(evaluation_results)
            
            # Select best model
            selection_result = self.select_best_model(comparison_report)
            
            # Generate report
            final_report = self.generate_selection_report(selection_result, comparison_report)
            
            logger.info("=" * 80)
            logger.info("MODEL SELECTION COMPLETED")
            logger.info("=" * 80)
            
            return final_report
        
        except Exception as e:
            logger.error(f"Model selection pipeline failed: {str(e)}", exc_info=True)
            return {
                'status': 'FAILED',
                'error': str(e)
            }


if __name__ == '__main__':
    import yaml
    
    with open('config/data_ingestion_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    selector = ModelSelector(config)
    result = selector.select_best_model_complete()
    
    print(json.dumps(result, indent=2, default=str))
