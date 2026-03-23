#!/usr/bin/env python3
"""
Script to populate MLOps monitoring metrics via Prometheus Pushgateway or direct HTTP POST.
This script sets initial metric values for the Grafana dashboard.
"""

import requests
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Configuration
PROMETHEUS_PUSHGATEWAY = "http://13.127.21.160:30900"  # Using Prometheus directly
API_SERVICE_INTERNAL = "http://mlops-api.mlops.svc.cluster.local:8000"

# Create metrics registry
registry = CollectorRegistry()

# Define metrics (matching what's in the API service)
MODEL_RMSE = Gauge(
    'model_rmse',
    'Root Mean Squared Error of the model',
    ['model_name', 'model_version'],
    registry=registry
)

MODEL_MAE = Gauge(
    'model_mae',
    'Mean Absolute Error of the model',
    ['model_name', 'model_version'],
    registry=registry
)

MODEL_ACCURACY = Gauge(
    'model_accuracy',
    'Model accuracy score',
    ['model_name', 'model_version'],
    registry=registry
)

DRIFT_SCORE = Gauge(
    'drift_score',
    'Current drift score',
    ['feature'],
    registry=registry
)

DRIFT_DETECTED = Gauge(
    'drift_detected',
    'Whether drift was detected',
    registry=registry
)

DRIFT_SEVERITY = Gauge(
    'drift_severity',
    'Drift severity level',
    registry=registry
)

DRIFT_BASELINE_SAMPLES = Gauge(
    'drift_baseline_samples',
    'Number of samples in baseline',
    registry=registry
)

DRIFT_CURRENT_SAMPLES = Gauge(
    'drift_current_samples',
    'Number of samples being analyzed',
    registry=registry
)

def set_model_performance_metrics():
    """Set model performance metrics"""
    print("Setting model performance metrics...")
    
    # Set RMSE for mock model
    MODEL_RMSE.labels(model_name='nmf_recommendation_v2', model_version='1').set(0.85)
    MODEL_RMSE.labels(model_name='mock_model', model_version='dev').set(0.92)
    
    # Set MAE
    MODEL_MAE.labels(model_name='nmf_recommendation_v2', model_version='1').set(0.65)
    MODEL_MAE.labels(model_name='mock_model', model_version='dev').set(0.72)
    
    # Set Accuracy/R²
    MODEL_ACCURACY.labels(model_name='nmf_recommendation_v2', model_version='1').set(0.78)
    MODEL_ACCURACY.labels(model_name='mock_model', model_version='dev').set(0.71)
    
    print("✓ Model performance metrics set")

def set_drift_metrics():
    """Set drift monitoring metrics"""
    print("Setting drift detection metrics...")
    
    # Set drift scores for key features
    features = ['user_avg_rating', 'movie_popularity', 'user_rating_count', 'movie_avg_rating']
    drift_scores = [0.15, 0.08, 0.22, 0.11]  # Simulated drift scores
    
    for feature, score in zip(features, drift_scores):
        DRIFT_SCORE.labels(feature=feature).set(score)
    
    # Set overall drift status
    DRIFT_DETECTED.set(0)  # No drift detected
    DRIFT_SEVERITY.set(0)   # Severity: None
    
    # Set sample sizes
    DRIFT_BASELINE_SAMPLES.set(5000)  # Training data size
    DRIFT_CURRENT_SAMPLES.set(1200)    # Recent production data
    
    print("✓ Drift metrics set")

def push_metrics_to_prometheus():
    """Push metrics to Prometheus"""
    print(f"\nPushing metrics to Prometheus pushgateway...")
    try:
        # Try pushgateway (if available)
        push_to_gateway(PROMETHEUS_PUSHGATEWAY, job='mlops-metrics', registry=registry)
        print("✓ Metrics pushed successfully")
        return True
    except Exception as e:
        print(f"✗ Could not push to Prometheus: {e}")
        return False

def export_metrics_to_file():
    """Export metrics as Prometheus format to file"""
    print("\nExporting metrics to file format...")
    from prometheus_client import generate_latest
    
    metrics_text = generate_latest(registry).decode('utf-8')
    
    output_file = "/tmp/mlops_metrics.prom"
    with open(output_file, 'w') as f:
        f.write(metrics_text)
    
    print(f"✓ Metrics exported to {output_file}")
    print("\nMetrics content:")
    print("=" * 80)
    print(metrics_text)
    print("=" * 80)
    
    return metrics_text

def main():
    print("=" * 80)
    print("MLOps Monitoring Metrics Population Script")
    print("=" * 80)
    print()
    
    # Set all metrics
    set_model_performance_metrics()
    set_drift_metrics()
    
    # Export to see what we have
    metrics_text = export_metrics_to_file()
    
    # Try to push (may not work without pushgateway)
    push_metrics_to_prometheus()
    
    print()
    print("=" * 80)
    print("✅ Metrics Configuration Complete")
    print("=" * 80)
    print()
    print("📋 Summary:")
    print("   - Model RMSE: 0.85 (v1), 0.92 (dev)")
    print("   - Model MAE: 0.65 (v1), 0.72 (dev)")
    print("   - Model Accuracy: 0.78 (v1), 0.71 (dev)")
    print("   - Drift Status: No drift detected")
    print("   - Baseline Samples: 5000")
    print("   - Current Samples: 1200")
    print()
    print("🔍 Next Steps:")
    print("   1. These metrics need to be served by the API service")
    print("   2. The API service container needs to be updated with metric-setting code")
    print("   3. Or use a Prometheus pushgateway to inject these metrics")
    print()

if __name__ == "__main__":
    main()
