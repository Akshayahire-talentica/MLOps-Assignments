"""
Model Evaluation Gates
======================

Production-ready evaluation gates to prevent bad model deployments.

Gates:
1. Performance gates (RMSE, MAE, R2)
2. Business metric gates (CTR, watch rate)
3. System metric gates (latency, errors)
4. Data quality gates (coverage, diversity)
5. Statistical significance tests
"""

import logging
from typing import Dict, Tuple, Optional
import numpy as np
from scipy import stats
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# GATE CONFIGURATION
# ============================================================

DEFAULT_GATES = {
    # Performance Gates
    'max_rmse_degradation': 0.10,     # 10% worse than baseline
    'max_mae_degradation': 0.10,      # 10% worse than baseline
    'min_r2_score': 0.50,             # Minimum R2 score
    
    # Business Metric Gates
    'min_ctr_ratio': 0.95,            # 95% of baseline CTR
    'min_watch_rate_ratio': 0.95,    # 95% of baseline watch rate
    'min_avg_engagement': 2.0,        # Minimum average engagement score
    
    # System Metric Gates
    'max_p95_latency_ms': 200,        # P95 latency under 200ms
    'max_error_rate': 0.01,           # Max 1% error rate
    
    # Data Quality Gates
    'min_user_coverage': 0.90,        # 90% user coverage
    'min_item_coverage': 0.80,        # 80% item coverage
    'min_prediction_diversity': 0.30, # 30% unique items in top-100
    
    # Statistical Gates
    'min_sample_size': 1000,          # Minimum validation samples
    'max_pvalue': 0.05,               # Statistical significance threshold
}

# ============================================================
# EVALUATION GATE CLASS
# ============================================================

class ModelEvaluationGate:
    """
    Evaluation gate to approve/reject model deployments
    """
    
    def __init__(self, gates_config: Optional[Dict] = None, db_config: Optional[Dict] = None):
        self.gates_config = gates_config or DEFAULT_GATES
        self.db_config = db_config or {
            'host': 'postgres',
            'port': 5432,
            'database': 'mlops_db',
            'user': 'mlops',
            'password': 'mlops123'
        }
    
    def evaluate(
        self,
        candidate_metrics: Dict,
        baseline_metrics: Dict,
        validation_data: Optional[Dict] = None
    ) -> Tuple[bool, Dict]:
        """
        Evaluate candidate model against all gates
        
        Args:
            candidate_metrics: Metrics from new model
            baseline_metrics: Metrics from current production model
            validation_data: Optional validation dataset metadata
        
        Returns:
            (passed, results) where:
                passed: bool - True if all gates passed
                results: dict - Detailed results for each gate
        """
        logger.info("=" * 80)
        logger.info("EVALUATING MODEL AGAINST GATES")
        logger.info("=" * 80)
        
        results = {
            'gates_passed': [],
            'gates_failed': [],
            'gate_results': {},
            'overall_passed': True
        }
        
        # Gate 1: Performance Gates
        self._evaluate_performance_gates(
            candidate_metrics, baseline_metrics, results
        )
        
        # Gate 2: Business Metric Gates
        self._evaluate_business_gates(
            candidate_metrics, baseline_metrics, results
        )
        
        # Gate 3: System Metric Gates
        self._evaluate_system_gates(
            candidate_metrics, results
        )
        
        # Gate 4: Data Quality Gates
        if validation_data:
            self._evaluate_data_quality_gates(
                validation_data, results
            )
        
        # Gate 5: Statistical Significance
        if validation_data:
            self._evaluate_statistical_significance(
                candidate_metrics, baseline_metrics, validation_data, results
            )
        
        # Determine overall result
        results['overall_passed'] = len(results['gates_failed']) == 0
        
        # Log summary
        self._log_results(results)
        
        return results['overall_passed'], results
    
    def _evaluate_performance_gates(
        self, candidate: Dict, baseline: Dict, results: Dict
    ):
        """Evaluate model performance gates"""
        logger.info("Evaluating Performance Gates:")
        
        # RMSE gate
        if 'rmse' in candidate and 'rmse' in baseline:
            rmse_degradation = (candidate['rmse'] - baseline['rmse']) / baseline['rmse']
            passed = rmse_degradation <= self.gates_config['max_rmse_degradation']
            
            gate_name = 'rmse_degradation'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'candidate_value': candidate['rmse'],
                'baseline_value': baseline['rmse'],
                'degradation': rmse_degradation,
                'threshold': self.gates_config['max_rmse_degradation']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ RMSE: {rmse_degradation:+.2%} degradation (threshold: {self.gates_config['max_rmse_degradation']:.2%})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ RMSE: {rmse_degradation:+.2%} degradation exceeds threshold")
        
        # MAE gate
        if 'mae' in candidate and 'mae' in baseline:
            mae_degradation = (candidate['mae'] - baseline['mae']) / baseline['mae']
            passed = mae_degradation <= self.gates_config['max_mae_degradation']
            
            gate_name = 'mae_degradation'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'candidate_value': candidate['mae'],
                'baseline_value': baseline['mae'],
                'degradation': mae_degradation,
                'threshold': self.gates_config['max_mae_degradation']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ MAE: {mae_degradation:+.2%} degradation (threshold: {self.gates_config['max_mae_degradation']:.2%})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ MAE: {mae_degradation:+.2%} degradation exceeds threshold")
        
        # R2 score gate
        if 'r2_score' in candidate:
            passed = candidate['r2_score'] >= self.gates_config['min_r2_score']
            
            gate_name = 'r2_score'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'value': candidate['r2_score'],
                'threshold': self.gates_config['min_r2_score']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ R² Score: {candidate['r2_score']:.3f} (threshold: {self.gates_config['min_r2_score']:.3f})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ R² Score: {candidate['r2_score']:.3f} below threshold")
    
    def _evaluate_business_gates(
        self, candidate: Dict, baseline: Dict, results: Dict
    ):
        """Evaluate business metric gates"""
        logger.info("Evaluating Business Metric Gates:")
        
        # CTR gate
        if 'ctr' in candidate and 'ctr' in baseline:
            ctr_ratio = candidate['ctr'] / baseline['ctr']
            passed = ctr_ratio >= self.gates_config['min_ctr_ratio']
            
            gate_name = 'ctr_ratio'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'candidate_value': candidate['ctr'],
                'baseline_value': baseline['ctr'],
                'ratio': ctr_ratio,
                'threshold': self.gates_config['min_ctr_ratio']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ CTR Ratio: {ctr_ratio:.2%} (threshold: {self.gates_config['min_ctr_ratio']:.2%})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ CTR Ratio: {ctr_ratio:.2%} below threshold")
        
        # Watch rate gate
        if 'watch_rate' in candidate and 'watch_rate' in baseline:
            watch_rate_ratio = candidate['watch_rate'] / baseline['watch_rate']
            passed = watch_rate_ratio >= self.gates_config['min_watch_rate_ratio']
            
            gate_name = 'watch_rate_ratio'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'candidate_value': candidate['watch_rate'],
                'baseline_value': baseline['watch_rate'],
                'ratio': watch_rate_ratio,
                'threshold': self.gates_config['min_watch_rate_ratio']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ Watch Rate Ratio: {watch_rate_ratio:.2%} (threshold: {self.gates_config['min_watch_rate_ratio']:.2%})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ Watch Rate Ratio: {watch_rate_ratio:.2%} below threshold")
    
    def _evaluate_system_gates(
        self, candidate: Dict, results: Dict
    ):
        """Evaluate system metric gates"""
        logger.info("Evaluating System Metric Gates:")
        
        # Latency gate
        if 'p95_latency_ms' in candidate:
            passed = candidate['p95_latency_ms'] <= self.gates_config['max_p95_latency_ms']
            
            gate_name = 'p95_latency'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'value': candidate['p95_latency_ms'],
                'threshold': self.gates_config['max_p95_latency_ms']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ P95 Latency: {candidate['p95_latency_ms']:.1f}ms (threshold: {self.gates_config['max_p95_latency_ms']}ms)")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ P95 Latency: {candidate['p95_latency_ms']:.1f}ms exceeds threshold")
        
        # Error rate gate
        if 'error_rate' in candidate:
            passed = candidate['error_rate'] <= self.gates_config['max_error_rate']
            
            gate_name = 'error_rate'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'value': candidate['error_rate'],
                'threshold': self.gates_config['max_error_rate']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ Error Rate: {candidate['error_rate']:.2%} (threshold: {self.gates_config['max_error_rate']:.2%})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ Error Rate: {candidate['error_rate']:.2%} exceeds threshold")
    
    def _evaluate_data_quality_gates(
        self, validation_data: Dict, results: Dict
    ):
        """Evaluate data quality gates"""
        logger.info("Evaluating Data Quality Gates:")
        
        # Sample size gate
        if 'sample_size' in validation_data:
            passed = validation_data['sample_size'] >= self.gates_config['min_sample_size']
            
            gate_name = 'sample_size'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'value': validation_data['sample_size'],
                'threshold': self.gates_config['min_sample_size']
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ Sample Size: {validation_data['sample_size']:,} (threshold: {self.gates_config['min_sample_size']:,})")
            else:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ Sample Size: {validation_data['sample_size']:,} below threshold")
    
    def _evaluate_statistical_significance(
        self, candidate: Dict, baseline: Dict, validation_data: Dict, results: Dict
    ):
        """Evaluate statistical significance of improvements"""
        logger.info("Evaluating Statistical Significance:")
        
        if 'predictions_candidate' in validation_data and 'predictions_baseline' in validation_data:
            # Perform paired t-test on squared errors
            errors_candidate = np.array(validation_data['predictions_candidate'])
            errors_baseline = np.array(validation_data['predictions_baseline'])
            
            t_stat, p_value = stats.ttest_rel(
                np.square(errors_candidate),
                np.square(errors_baseline)
            )
            
            is_significant = p_value < self.gates_config['max_pvalue']
            is_improvement = t_stat < 0  # Candidate has lower errors
            
            passed = is_significant and is_improvement
            
            gate_name = 'statistical_significance'
            results['gate_results'][gate_name] = {
                'passed': passed,
                'p_value': p_value,
                't_statistic': t_stat,
                'threshold': self.gates_config['max_pvalue'],
                'is_significant': is_significant,
                'is_improvement': is_improvement
            }
            
            if passed:
                results['gates_passed'].append(gate_name)
                logger.info(f"  ✓ Statistical Significance: p={p_value:.4f}, t={t_stat:.3f} (significant improvement)")
            elif is_significant and not is_improvement:
                results['gates_failed'].append(gate_name)
                logger.warning(f"  ✗ Statistical Significance: p={p_value:.4f}, t={t_stat:.3f} (significant degradation!)")
            else:
                logger.info(f"  ○ Statistical Significance: p={p_value:.4f}, t={t_stat:.3f} (not significant)")
    
    def _log_results(self, results: Dict):
        """Log evaluation summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("EVALUATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Gates Passed: {len(results['gates_passed'])}")
        logger.info(f"Gates Failed: {len(results['gates_failed'])}")
        logger.info("")
        
        if results['overall_passed']:
            logger.info("✓✓✓ ALL GATES PASSED - MODEL APPROVED FOR DEPLOYMENT ✓✓✓")
        else:
            logger.warning("✗✗✗ SOME GATES FAILED - MODEL REJECTED ✗✗✗")
            logger.warning("")
            logger.warning("Failed Gates:")
            for gate_name in results['gates_failed']:
                logger.warning(f"  - {gate_name}: {results['gate_results'][gate_name]}")
        
        logger.info("=" * 80)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_model_metrics_from_db(model_name: str, model_version: str, db_config: Dict) -> Dict:
    """Fetch model metrics from database"""
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT rmse, mae, r2_score, ctr, watch_rate, avg_watch_completion,
               p95_latency_ms, error_rate
        FROM model_performance
        WHERE model_name = %s AND model_version = %s
        ORDER BY evaluation_date DESC
        LIMIT 1
    """, (model_name, model_version))
    
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not row:
        raise ValueError(f"No metrics found for {model_name}:{model_version}")
    
    return {
        'rmse': row[0],
        'mae': row[1],
        'r2_score': row[2],
        'ctr': row[3],
        'watch_rate': row[4],
        'avg_watch_completion': row[5],
        'p95_latency_ms': row[6],
        'error_rate': row[7]
    }

# ============================================================
# CLI USAGE
# ============================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Evaluate candidate model against gates")
    parser.add_argument('--candidate-model', required=True, help='Candidate model name')
    parser.add_argument('--candidate-version', required=True, help='Candidate model version')
    parser.add_argument('--baseline-model', required=True, help='Baseline model name')
    parser.add_argument('--baseline-version', required=True, help='Baseline model version')
    
    args = parser.parse_args()
    
    # Initialize gate
    gate = ModelEvaluationGate()
    
    # Fetch metrics
    candidate_metrics = get_model_metrics_from_db(
        args.candidate_model, args.candidate_version,
        gate.db_config
    )
    
    baseline_metrics = get_model_metrics_from_db(
        args.baseline_model, args.baseline_version,
        gate.db_config
    )
    
    # Evaluate
    passed, results = gate.evaluate(candidate_metrics, baseline_metrics)
    
    exit(0 if passed else 1)
