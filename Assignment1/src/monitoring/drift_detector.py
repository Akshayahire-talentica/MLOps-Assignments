#!/usr/bin/env python3
"""
Data Drift Detector - Monitors feature distributions and model performance
Task: Drift detection job (Owner: Vikas)
Input: Baseline data
Output: Drift report with statistical tests
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd
import yaml
from scipy import stats
from scipy.stats import ks_2samp, chi2_contingency
from prometheus_client import Gauge, Counter, CollectorRegistry, push_to_gateway

try:
    # Evidently AI for richer data/model drift diagnostics
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset

    EVIDENTLY_AVAILABLE = True
except ImportError:
    EVIDENTLY_AVAILABLE = False


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics for drift monitoring
registry = CollectorRegistry()
DRIFT_SCORE = Gauge('drift_score', 'Current drift score', ['feature'], registry=registry)
DRIFT_DETECTED = Gauge('drift_detected', 'Whether drift was detected (1=yes, 0=no)', registry=registry)
DRIFT_SEVERITY = Gauge('drift_severity', 'Drift severity level (0=none, 1=warning, 2=critical)', registry=registry)
BASELINE_SAMPLES = Gauge('drift_baseline_samples', 'Number of samples in baseline', registry=registry)
CURRENT_SAMPLES = Gauge('drift_current_samples', 'Number of samples being analyzed', registry=registry)
DRIFT_CHECKS_TOTAL = Counter('drift_checks_total', 'Total number of drift checks performed', ['status'], registry=registry)


class DriftDetector:
    """
    Detects data drift using statistical tests:
    - KS Test (Kolmogorov-Smirnov) for numerical features
    - PSI (Population Stability Index) for distributions
    - Chi-Square test for categorical features
    """
    
    def __init__(self, config_path: str = "config/drift_config.yaml"):
        """Initialize drift detector with configuration"""
        self.config = self._load_config(config_path)
        self.baseline_dir = Path(self.config['baseline']['storage_path'])
        self.report_dir = Path(self.config['reporting']['output_path'])
        
        # Create directories
        self.baseline_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_config(self, config_path: str) -> Dict:
        """Load drift detection configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found. Using defaults.")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """Default configuration if file not found"""
        return {
            'detection': {
                'enabled': True,
                'tests': {
                    'ks_test': {'enabled': True, 'significance_level': 0.05},
                    'psi': {'enabled': True, 'threshold': 0.2}
                }
            },
            'thresholds': {
                'warning': 0.15,
                'critical': 0.25,
                'rmse_degradation_percent': 10
            },
            'baseline': {
                'storage_path': 'data/baseline',
                'minimum_samples': 1000
            },
            'reporting': {
                'output_path': 'reports/drift',
                'format': 'json'
            }
        }
    
    def save_baseline(self, data: pd.DataFrame, name: str = "baseline"):
        """Save baseline data for future drift comparison"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        baseline_file = self.baseline_dir / f"{name}_{timestamp}.csv"
        
        data.to_csv(baseline_file, index=False)
        logger.info(f"[OK] Baseline saved: {baseline_file}")
        
        # Save metadata
        metadata = {
            'timestamp': timestamp,
            'rows': len(data),
            'columns': list(data.columns),
            'dtypes': {col: str(dtype) for col, dtype in data.dtypes.items()},
            'file': str(baseline_file)
        }
        
        metadata_file = self.baseline_dir / f"{name}_{timestamp}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return str(baseline_file)
    
    def load_baseline(self, name: str = "baseline") -> Tuple[pd.DataFrame, Dict]:
        """Load most recent baseline data"""
        baseline_files = sorted(self.baseline_dir.glob(f"{name}_*.csv"))
        
        if not baseline_files:
            raise FileNotFoundError(f"No baseline files found for '{name}'")
        
        # Load most recent
        latest_baseline = baseline_files[-1]
        logger.info(f"Loading baseline: {latest_baseline}")
        
        data = pd.read_csv(latest_baseline)
        
        # Load metadata
        metadata_file = latest_baseline.with_suffix('').with_suffix('.csv').parent / \
                       f"{latest_baseline.stem}_metadata.json"
        
        metadata = {}
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
        
        return data, metadata
    
    def calculate_psi(self, baseline: np.ndarray, current: np.ndarray, 
                      bins: int = 10) -> float:
        """
        Calculate Population Stability Index (PSI)
        PSI < 0.1: No significant change
        PSI < 0.2: Minor change
        PSI >= 0.2: Major change requiring investigation
        """
        # Create bins based on baseline
        breakpoints = np.percentile(baseline, np.linspace(0, 100, bins + 1))
        breakpoints = np.unique(breakpoints)
        
        # Ensure we have valid bins
        if len(breakpoints) < 2:
            logger.warning("Insufficient unique values for PSI calculation")
            return 0.0
        
        # Calculate distribution for baseline and current
        baseline_dist, _ = np.histogram(baseline, bins=breakpoints)
        current_dist, _ = np.histogram(current, bins=breakpoints)
        
        # Add small epsilon to avoid division by zero
        epsilon = 1e-6
        baseline_dist = (baseline_dist + epsilon) / (baseline_dist.sum() + epsilon * len(baseline_dist))
        current_dist = (current_dist + epsilon) / (current_dist.sum() + epsilon * len(current_dist))
        
        # Calculate PSI
        psi = np.sum((current_dist - baseline_dist) * np.log(current_dist / baseline_dist))
        
        return float(psi)
    
    def ks_test(self, baseline: np.ndarray, current: np.ndarray) -> Tuple[float, float]:
        """
        Kolmogorov-Smirnov test for distribution comparison
        Returns (statistic, p_value)
        """
        statistic, p_value = ks_2samp(baseline, current)
        return float(statistic), float(p_value)

    def run_evidently_drift(self, baseline_data: pd.DataFrame,
                            current_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Run Evidently DataDriftPreset to get dataset-level drift diagnostics.
        Returns a dict with the raw Evidently report and a small summary.
        """
        if not EVIDENTLY_AVAILABLE:
            logger.warning("Evidently is not installed - skipping Evidently-based drift detection")
            return {}

        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=baseline_data, current_data=current_data)
            report_dict = report.as_dict()

            summary: Dict[str, Any] = {}
            # Best-effort extraction of dataset-level drift summary
            for metric in report_dict.get("metrics", []):
                # Different versions may name this slightly differently; be defensive
                metric_name = metric.get("metric") or metric.get("metric_name")
                if metric_name and "DataDrift" in str(metric_name):
                    result = metric.get("result", {}) or {}
                    summary = {
                        "dataset_drift": result.get("dataset_drift"),
                        "share_of_drifted_columns": result.get("share_of_drifted_columns")
                        or result.get("share_drifted_features"),
                        "n_drifted_columns": result.get("number_of_drifted_columns")
                        or result.get("number_of_drifted_features"),
                        "n_columns": result.get("number_of_columns"),
                    }
                    break

            return {
                "summary": summary,
                "raw_report": report_dict,
            }
        except Exception as e:
            logger.warning("Evidently drift detection failed: %s", e)
            return {}

    def detect_drift(self, baseline_data: pd.DataFrame,
                     current_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect drift between baseline and current data
        Returns comprehensive drift report
        """
        logger.info("Starting drift detection analysis...")
        
        drift_report: Dict[str, Any] = {
            'timestamp': datetime.now().isoformat(),
            'baseline_samples': len(baseline_data),
            'current_samples': len(current_data),
            'features_analyzed': [],
            'drift_detected': False,
            'severity': 'none',
            'details': {},
        }
        
        # Get numerical columns
        numerical_cols = baseline_data.select_dtypes(include=[np.number]).columns

        thresholds = self.config['thresholds']
        tests_config = self.config['detection']['tests']

        max_drift_score = 0.0
        
        for col in numerical_cols:
            if col not in current_data.columns:
                logger.warning(f"Column {col} not in current data, skipping")
                continue
            
            baseline_values = baseline_data[col].dropna().values
            current_values = current_data[col].dropna().values
            
            if len(baseline_values) < 10 or len(current_values) < 10:
                logger.warning(f"Insufficient data for {col}, skipping")
                continue
            
            feature_drift = {
                'type': 'numerical',
                'baseline_stats': {
                    'mean': float(baseline_values.mean()),
                    'std': float(baseline_values.std()),
                    'min': float(baseline_values.min()),
                    'max': float(baseline_values.max())
                },
                'current_stats': {
                    'mean': float(current_values.mean()),
                    'std': float(current_values.std()),
                    'min': float(current_values.min()),
                    'max': float(current_values.max())
                },
                'tests': {}
            }
            
            # KS Test
            if tests_config.get('ks_test', {}).get('enabled', True):
                ks_stat, ks_pvalue = self.ks_test(baseline_values, current_values)
                feature_drift['tests']['ks_test'] = {
                    'statistic': ks_stat,
                    'p_value': ks_pvalue,
                    'drift_detected': ks_pvalue < tests_config['ks_test']['significance_level']
                }
            
            # PSI
            if tests_config.get('psi', {}).get('enabled', True):
                psi_score = self.calculate_psi(baseline_values, current_values)
                feature_drift['tests']['psi'] = {
                    'score': psi_score,
                    'drift_detected': psi_score >= tests_config['psi']['threshold']
                }
                max_drift_score = max(max_drift_score, psi_score)
            
            drift_report['features_analyzed'].append(col)
            drift_report['details'][col] = feature_drift

        # Run Evidently-based drift analysis (data drift preset) and fold into report
        evidently_summary = self.run_evidently_drift(baseline_data, current_data)
        drift_report['evidently'] = evidently_summary

        evidently_score = 0.0
        if evidently_summary.get("summary"):
            share = evidently_summary["summary"].get("share_of_drifted_columns")
            try:
                if share is not None:
                    evidently_score = float(share)
            except (TypeError, ValueError):
                evidently_score = 0.0

        # Use the worst of PSI-based score and Evidently share-of-drifted-features
        max_drift_score = max(max_drift_score, evidently_score)

        # Determine overall drift status
        if max_drift_score >= thresholds['critical']:
            drift_report['drift_detected'] = True
            drift_report['severity'] = 'critical'
        elif max_drift_score >= thresholds['warning']:
            drift_report['drift_detected'] = True
            drift_report['severity'] = 'warning'
        else:
            drift_report['drift_detected'] = False
            drift_report['severity'] = 'none'
        
        drift_report['max_drift_score'] = max_drift_score
        
        # Record Prometheus metrics
        DRIFT_SCORE.labels(feature='overall').set(max_drift_score)
        DRIFT_DETECTED.set(1 if drift_report['drift_detected'] else 0)
        
        # Set severity as numeric: 0=none, 1=warning, 2=critical
        severity_map = {'none': 0, 'warning': 1, 'critical': 2}
        DRIFT_SEVERITY.set(severity_map.get(drift_report['severity'], 0))
        
        BASELINE_SAMPLES.set(len(baseline_data))
        CURRENT_SAMPLES.set(len(current_data))
        DRIFT_CHECKS_TOTAL.labels(status=drift_report['severity']).inc()
        
        logger.info(f"[OK] Drift analysis complete. Severity: {drift_report['severity']}")
        
        return drift_report
    
    def save_report(self, drift_report: Dict[str, Any], 
                    report_name: str = "drift_report") -> str:
        """Save drift report to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.report_dir / f"{report_name}_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(drift_report, f, indent=2)
        
        logger.info(f"[OK] Drift report saved: {report_file}")
        
        # Create summary file (latest)
        summary_file = self.report_dir / f"{report_name}_latest.json"
        with open(summary_file, 'w') as f:
            json.dump(drift_report, f, indent=2)
        
        return str(report_file)
    
    def generate_prometheus_metrics(self, drift_report: Dict[str, Any]) -> str:
        """Generate Prometheus metrics file"""
        metrics = []
        
        # Overall drift score
        metrics.append(
            f"mlops_drift_score {drift_report.get('max_drift_score', 0.0)}"
        )
        
        # Drift detected (1 or 0)
        drift_detected = 1 if drift_report['drift_detected'] else 0
        metrics.append(f"mlops_drift_detected {drift_detected}")
        
        # Severity mapping
        severity_map = {'none': 0, 'warning': 1, 'critical': 2}
        severity_value = severity_map.get(drift_report['severity'], 0)
        metrics.append(f"mlops_drift_severity {severity_value}")
        
        # Features analyzed
        metrics.append(f"mlops_drift_features_analyzed {len(drift_report['features_analyzed'])}")
        
        # Save metrics
        metrics_file = self.report_dir / "drift_metrics.prom"
        with open(metrics_file, 'w') as f:
            f.write('\n'.join(metrics) + '\n')
        
        logger.info(f"[OK] Prometheus metrics saved: {metrics_file}")
        return str(metrics_file)


def main():
    """Main execution for drift detection"""
    logger.info("="*70)
    logger.info("DATA DRIFT DETECTION")
    logger.info("="*70)
    
    # Initialize detector
    detector = DriftDetector()
    
    # Check if baseline exists
    try:
        baseline_data, baseline_metadata = detector.load_baseline("features")
        logger.info(f"[OK] Loaded baseline with {len(baseline_data)} samples")
    except FileNotFoundError:
        logger.warning("[WARNING] No baseline found. Creating from current data...")
        
        # Load current features for baseline
        feature_files = sorted(Path("data/features").glob("*_features_*.csv"))
        
        if not feature_files:
            logger.error("[FAILED] No feature files found")
            return 1
        
        # Use oldest features as baseline
        baseline_data = pd.read_csv(feature_files[0])
        detector.save_baseline(baseline_data, "features")
        logger.info("[OK] Baseline created from existing features")
        return 0
    
    # Load current data (latest features)
    feature_files = sorted(Path("data/features").glob("*_features_*.csv"))
    
    if not feature_files:
        logger.error("[FAILED] No current feature files found")
        return 1
    
    latest_features = feature_files[-1]
    current_data = pd.read_csv(latest_features)
    logger.info(f"[OK] Loaded current data: {latest_features.name}")
    
    # Detect drift
    drift_report = detector.detect_drift(baseline_data, current_data)

    # Save report
    detector.save_report(drift_report, "drift_report")

    # Generate Prometheus metrics
    detector.generate_prometheus_metrics(drift_report)

    # Print summary
    logger.info("")
    logger.info("="*70)
    logger.info("DRIFT DETECTION SUMMARY")
    logger.info("="*70)
    logger.info(f"Drift Detected: {drift_report['drift_detected']}")
    logger.info(f"Severity: {drift_report['severity']}")
    logger.info(f"Max Drift Score: {drift_report['max_drift_score']:.4f}")
    logger.info(f"Features Analyzed: {len(drift_report['features_analyzed'])}")
    logger.info("="*70)

    # On critical drift: optionally re-run full pipeline (ingestion → validation → features → training → selection)
    actions_cfg = detector.config.get("actions", {})
    critical_actions = actions_cfg.get("on_critical", []) or []
    if drift_report['severity'] == 'critical' and "trigger_retraining" in critical_actions:
        logger.warning("[ACTION] Critical drift detected and 'trigger_retraining' enabled - re-running full pipeline")
        import subprocess

        def _run_step(cmd: list, name: str) -> None:
            logger.info("Running step '%s': %s", name, " ".join(cmd))
            completed = subprocess.run(cmd, check=True)
            logger.info("[OK] Step '%s' completed (return code %s)", name, completed.returncode)

        try:
            # 1) Rebuild data & features
            _run_step(["python3", "src/data_ingestion/run_ingestion.py"], "data_ingestion")
            _run_step(["python3", "src/data_validation/run_validation.py"], "data_validation")
            _run_step(["python3", "src/features/feature_engineering.py"], "feature_engineering")

            # 2) Retrain and compare models
            _run_step(["python3", "src/training/model_trainer.py"], "model_training")
            _run_step(["python3", "src/training/model_selection.py"], "model_selection")

            # 3) Refresh baseline from latest features so future checks compare against new state
            try:
                feature_files = sorted(Path("data/features").glob("*_features_*.csv"))
                if feature_files:
                    latest = feature_files[-1]
                    new_baseline = pd.read_csv(latest)
                    detector.save_baseline(new_baseline, "features")
                    logger.info("[OK] Updated baseline after retraining from %s", latest)
            except Exception as e:
                logger.warning("Failed to update baseline after retraining: %s", e)
        except subprocess.CalledProcessError as e:
            logger.error("[FAILED] Automated pipeline re-run failed at step '%s': %s", e.cmd, e)
        except Exception as e:
            logger.error("[FAILED] Unexpected error during automated pipeline re-run: %s", e, exc_info=True)

    # Exit code based on severity
    if drift_report['severity'] == 'critical':
        logger.warning("[WARNING] Critical drift detected - automated retraining may have been triggered")
        return 2  # Exit code 2 for critical
    elif drift_report['severity'] == 'warning':
        logger.info("[OK] Minor drift detected - Continue monitoring")
        return 0
    else:
        logger.info("[OK] No significant drift detected")
        return 0


if __name__ == "__main__":
    sys.exit(main())
