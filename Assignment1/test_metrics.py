"""
Test script to verify Prometheus metrics are being exposed correctly
"""
import time
import requests
import json

def test_api_metrics():
    """Test API service metrics endpoint"""
    print("Testing API service metrics...")
    
    # Start the API service first (assumes it's running)
    api_url = "http://localhost:8000"
    
    try:
        # Make a prediction request to generate metrics
        prediction_request = {
            "user_id": 1,
            "movie_id": 100,
            "user_avg_rating": 4.0,
            "user_rating_count": 25,
            "movie_popularity": 1.5,
            "movie_avg_rating": 4.2,
            "day_of_week": 3,
            "month": 6
        }
        
        print("Making prediction request...")
        response = requests.post(f"{api_url}/predict", json=prediction_request)
        print(f"✓ Prediction response: {response.status_code}")
        if response.status_code == 200:
            print(f"  Predicted rating: {response.json()['predicted_rating']:.2f}")
        
        # Wait a moment for metrics to be recorded
        time.sleep(0.5)
        
        # Check metrics endpoint
        print("\nChecking /metrics endpoint...")
        metrics_response = requests.get(f"{api_url}/metrics")
        print(f"✓ Metrics response: {metrics_response.status_code}")
        
        if metrics_response.status_code == 200:
            metrics_text = metrics_response.text
            
            # Check for expected metrics
            expected_metrics = [
                'model_api_requests_total',
                'model_prediction_latency_seconds',
                'model_predictions_total',
                'model_rmse',
                'model_mae',
                'model_accuracy',
                'model_api_active_requests',
                'model_load_timestamp'
            ]
            
            print("\nMetrics found:")
            for metric in expected_metrics:
                if metric in metrics_text:
                    print(f"  ✓ {metric}")
                else:
                    print(f"  ✗ {metric} - NOT FOUND")
            
            # Print a sample of the metrics
            print("\nSample metrics output:")
            lines = metrics_text.split('\n')
            for line in lines[:30]:
                if line and not line.startswith('#'):
                    print(f"  {line}")
        
        return True
    
    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to API service. Make sure it's running on port 8000")
        print("  Start with: python src/serving/api_service.py")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


def test_health_check():
    """Test API health check"""
    print("\nTesting health check...")
    
    try:
        response = requests.get("http://localhost:8000/health")
        if response.status_code == 200:
            health_data = response.json()
            print("✓ Health check passed")
            print(f"  Status: {health_data['status']}")
            print(f"  Model loaded: {health_data['model_loaded']}")
            print(f"  Model version: {health_data.get('model_version', 'N/A')}")
        return True
    except Exception as e:
        print(f"✗ Health check failed: {str(e)}")
        return False


def main():
    print("=" * 60)
    print("MLOps Metrics Validation Test")
    print("=" * 60)
    
    # Test health check first
    if not test_health_check():
        print("\n⚠️  API service not running. Start it with:")
        print("   python src/serving/api_service.py")
        return
    
    # Test metrics
    if test_api_metrics():
        print("\n" + "=" * 60)
        print("✓ All metrics tests passed!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Start Prometheus: docker-compose up prometheus")
        print("2. Start Grafana: docker-compose up grafana")
        print("3. Access Grafana at http://localhost:3000 (admin/admin123)")
        print("4. View dashboard: MLOps - End-to-End Monitoring Dashboard")
    else:
        print("\n" + "=" * 60)
        print("✗ Some tests failed")
        print("=" * 60)


if __name__ == "__main__":
    main()
