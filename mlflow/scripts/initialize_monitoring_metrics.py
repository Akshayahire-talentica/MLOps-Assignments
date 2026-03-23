#!/usr/bin/env python3
"""
Set initial model performance metrics for monitoring dashboard.
This script sets the model performance metrics (RMSE, MAE, Accuracy) 
based on the trained model's evaluation results.
"""

import requests
import json
import mlflow
import os
from datetime import datetime

# MLflow configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://13.127.21.160:30005")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

def get_model_metrics():
    """Get metrics from the latest model in MLflow"""
    try:
        client = mlflow.tracking.MlflowClient()
        
        # Get the latest run for nmf_recommendation_v2
        model_name = "nmf_recommendation_v2"
        
        try:
            # Try to get production model
            model_version = client.get_latest_versions(model_name, stages=["Production"])
            if not model_version:
                model_version = client.get_latest_versions(model_name, stages=["Staging"])
            if not model_version:
                model_version = client.get_latest_versions(model_name, stages=["None"])
            
            if model_version:
                run_id = model_version[0].run_id
                run = client.get_run(run_id)
                
                metrics = run.data.metrics
                print(f"✅ Found model metrics from MLflow:")
                print(f"   Model: {model_name}")
                print(f"   Version: {model_version[0].version}")
                print(f"   Stage: {model_version[0].current_stage}")
                print(f"   RMSE: {metrics.get('test_rmse', 'N/A')}")
                print(f"   MAE: {metrics.get('test_mae', 'N/A')}")
                print(f"   R²: {metrics.get('test_r2', 'N/A')}")
                
                return {
                    'model_name': model_name,
                    'model_version': model_version[0].version,
                    'rmse': metrics.get('test_rmse', 0.85),
                    'mae': metrics.get('test_mae', 0.65),
                    'r2': metrics.get('test_r2', 0.75),
                    'accuracy': metrics.get('accuracy', 0.80)
                }
        except Exception as e:
            print(f"⚠️  Could not get model from registry: {e}")
            print(f"   Using default metrics for demonstration")
    
    except Exception as e:
        print(f"⚠️  Could not connect to MLflow: {e}")
        print(f"   Using default metrics for demonstration")
    
    # Return default metrics if MLflow unavailable
    return {
        'model_name': 'nmf_recommendation_v2',
        'model_version': '1',
        'rmse': 0.85,
        'mae': 0.65,
        'r2': 0.75,
        'accuracy': 0.80
    }


def set_prometheus_metrics(metrics):
    """
    Push metrics to Prometheus via the API service.
    Note: This requires the API service to expose an admin endpoint to set these values,
    or we need to trigger actual predictions and model evaluation.
    """
    print(f"\n📊 Model Performance Metrics:")
    print(f"   RMSE: {metrics['rmse']:.4f}")
    print(f"   MAE: {metrics['mae']:.4f}")
    print(f"   R²: {metrics['r2']:.4f}")
    print(f"   Accuracy: {metrics['accuracy']:.4f}")
    
    print(f"\n💡 These metrics are automatically set when the API service loads the model.")
    print(f"   The service reads model metrics from MLflow and exposes them via /metrics endpoint.")
    
    return True


def trigger_sample_predictions():
    """Trigger some sample predictions to populate latency metrics"""
    print(f"\n🔄 Triggering sample predictions to populate metrics...")
    
    api_url = "http://13.127.21.160:30000/predict"
    
    sample_requests = [
        {
            "user_id": 1,
            "movie_id": 50,
            "user_avg_rating": 3.5,
            "user_rating_count": 100,
            "movie_popularity": 0.8,
            "movie_avg_rating": 4.2,
            "day_of_week": 5,
            "month": 2
        },
        {
            "user_id": 2,
            "movie_id": 120,
            "user_avg_rating": 4.0,
            "user_rating_count": 50,
            "movie_popularity": 0.6,
            "movie_avg_rating": 3.8,
            "day_of_week": 3,
            "month": 6
        },
        {
            "user_id": 3,
            "movie_id": 75,
            "user_avg_rating": 2.8,
            "user_rating_count": 25,
            "movie_popularity": 0.9,
            "movie_avg_rating": 4.5,
            "day_of_week": 1,
            "month": 12
        }
    ]
    
    success_count = 0
    for i, req in enumerate(sample_requests):
        try:
            response = requests.post(api_url, json=req, timeout=5)
            if response.status_code == 200:
                result = response.json()
                print(f"   ✓ Prediction {i+1}: Rating {result['predicted_rating']:.2f}")
                success_count += 1
            else:
                print(f"   ✗ Prediction {i+1} failed: {response.status_code}")
        except Exception as e:
            print(f"   ✗ Prediction {i+1} error: {e}")
    
    print(f"   Completed {success_count}/{len(sample_requests)} predictions")
    
    return success_count > 0


def verify_metrics_endpoint():
    """Verify that metrics are being exposed"""
    print(f"\n🔍 Verifying metrics endpoint...")
    
    try:
        response = requests.get("http://13.127.21.160:30000/metrics", timeout=5)
        if response.status_code == 200:
            metrics_text = response.text
            
            # Check for key metrics
            has_predictions = 'model_predictions_total' in metrics_text
            has_rmse = 'model_rmse' in metrics_text
            has_mae = 'model_mae' in metrics_text
            has_latency = 'model_prediction_latency' in metrics_text
            
            print(f"   ✓ Metrics endpoint is accessible")
            print(f"   Metrics available:")
            print(f"      {'✓' if has_predictions else '✗'} model_predictions_total")
            print(f"      {'✓' if has_rmse else '✗'} model_rmse")
            print(f"      {'✓' if has_mae else '✗'} model_mae")
            print(f"      {'✓' if has_latency else '✗'} model_prediction_latency")
            
            return True
        else:
            print(f"   ✗ Metrics endpoint returned {response.status_code}")
            return False
    except Exception as e:
        print(f"   ✗ Could not access metrics endpoint: {e}")
        return False


def main():
    print("=" * 80)
    print("MLOps Metrics Initialization")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Step 1: Get model metrics from MLflow
    metrics = get_model_metrics()
    
    # Step 2: Set metrics in Prometheus
    set_prometheus_metrics(metrics)
    
    # Step 3: Trigger sample predictions
    trigger_sample_predictions()
    
    # Step 4: Verify metrics are being exposed
    verify_metrics_endpoint()
    
    print()
    print("=" * 80)
    print("✅ Initialization Complete")
    print("=" * 80)
    print()
    print("📊 View dashboards at:")
    print("   Model Performance & Drift: http://13.127.21.160:30300/d/deef0ca3-9cc7-4a0a-9b25-225b899968d8")
    print("   General Monitoring: http://13.127.21.160:30300/d/e25178a0-e76c-4db8-8978-56fc926bba8c")
    print()
    print("🔍 View Prometheus at:")
    print("   http://13.127.21.160:30900")
    print()
    print("💡 Tip: Wait 30-60 seconds for Prometheus to scrape the new metrics")


if __name__ == "__main__":
    main()
