# MLOps Monitoring Setup - Quick Start

This guide shows how to test the Prometheus + Grafana monitoring setup locally.

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- prometheus_client library (`pip install prometheus-client`)

## Quick Start (Local Testing)

### Step 1: Start the API Service

```powershell
# From the project root
cd C:\Users\mangeshd\Documents\mlops-poc

# Install dependencies if needed
pip install -r requirements.txt

# Start the API service
python src/serving/api_service.py
```

The API will start on `http://localhost:8000`

### Step 2: Verify Metrics are Exposed

```powershell
# In a new terminal, run the test script
python test_metrics.py
```

You should see output confirming all metrics are being exposed:
- ✓ model_api_requests_total
- ✓ model_prediction_latency_seconds
- ✓ model_predictions_total
- ✓ model_rmse
- ✓ model_mae
- ✓ model_accuracy

### Step 3: Start Prometheus (Local)

Create a simple docker-compose file:

```yaml
# docker-compose.monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:v2.48.0
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    networks:
      - mlops

  grafana:
    image: grafana/grafana:10.2.0
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus
    networks:
      - mlops

networks:
  mlops:
    driver: bridge
```

Start monitoring stack:

```powershell
docker-compose -f docker-compose.monitoring.yml up -d
```

### Step 4: Access Monitoring Tools

1. **Prometheus**: http://localhost:9090
   - Go to "Status" > "Targets" to verify scrape targets
   - Query metrics directly (e.g., `model_prediction_latency_seconds`)

2. **Grafana**: http://localhost:3000
   - Login: `admin` / `admin123`
   - Dashboard should auto-load: "MLOps - End-to-End Monitoring Dashboard"

### Step 5: Generate Traffic for Metrics

```powershell
# Make some prediction requests to generate metrics
python -c "
import requests
for i in range(10):
    response = requests.post('http://localhost:8000/predict', json={
        'user_id': i,
        'movie_id': 100 + i,
        'user_avg_rating': 3.5,
        'user_rating_count': 20,
        'movie_popularity': 1.0,
        'movie_avg_rating': 4.0,
        'day_of_week': 3,
        'month': 6
    })
    print(f'Request {i+1}: {response.status_code}')
"
```

## Dashboard Panels

The Grafana dashboard includes:

### Model Performance
- **Model RMSE** - Current RMSE value (gauge)
- **Model MAE** - Current MAE value (gauge)  
- **Model Performance Over Time** - RMSE and MAE timeline

### Prediction Latency
- **Prediction Latency Percentiles** - p50, p95, p99 latency
- **Active Requests** - Current number of requests being processed
- **Request Rate** - Success vs error rates

### Drift Detection
- **Drift Detection Status** - Current drift status (No Drift / Drift Detected)
- **Drift Score Timeline** - Historical drift scores with thresholds
- **Drift Severity Level** - None / Warning / Critical
- **Drift Checks** - Number of checks by status

### Statistics
- **Model Load Time** - When the model was last loaded
- **Total Predictions (24h)** - Prediction count in last 24 hours
- **Baseline Sample Count** - Number of samples in baseline
- **Current Sample Count** - Number of samples being analyzed

## Troubleshooting

### Metrics not showing in Prometheus

1. Check Prometheus targets: http://localhost:9090/targets
2. Verify API is running: http://localhost:8000/health
3. Check metrics endpoint: http://localhost:8000/metrics

### Dashboard not loading in Grafana

1. Check provisioning configuration:
   ```powershell
   cat monitoring/grafana/provisioning/dashboards/dashboard.yml
   ```

2. Verify dashboard file exists:
   ```powershell
   ls monitoring/grafana/dashboards/mlops-dashboard.json
   ```

3. Restart Grafana:
   ```powershell
   docker-compose -f docker-compose.monitoring.yml restart grafana
   ```

### No data in dashboard panels

1. Generate traffic using the script above
2. Wait 15-30 seconds for scrape interval
3. Check Prometheus for metrics: http://localhost:9090/graph
   - Query: `model_prediction_latency_seconds`
4. Verify time range in Grafana (top right) is set to "Last 1 hour"

## Kubernetes Deployment

For EKS deployment, the configuration is already in:
- `k8s-minimal/05-prometheus-minimal.yaml` - Prometheus deployment
- `k8s-minimal/06-grafana.yaml` - Grafana deployment

Apply with:
```bash
kubectl apply -f k8s-minimal/05-prometheus-minimal.yaml
kubectl apply -f k8s-minimal/06-grafana.yaml
```

Access via port-forward:
```bash
kubectl port-forward -n mlops svc/prometheus 9090:9090
kubectl port-forward -n mlops svc/grafana 3000:3000
```

## Metrics Reference

### API Metrics
- `model_api_requests_total{method, endpoint, status}` - Total requests
- `model_prediction_latency_seconds{endpoint}` - Prediction latency histogram
- `model_predictions_total{model_type}` - Total predictions by model type
- `model_api_active_requests` - Active request count
- `model_load_timestamp` - When model was loaded

### Model Performance Metrics
- `model_rmse{model_name, model_version}` - Root Mean Squared Error
- `model_mae{model_name, model_version}` - Mean Absolute Error
- `model_accuracy{model_name, model_version}` - Model accuracy

### Drift Detection Metrics
- `drift_score{feature}` - Drift score per feature
- `drift_detected` - Whether drift was detected (0/1)
- `drift_severity` - Severity level (0=none, 1=warning, 2=critical)
- `drift_baseline_samples` - Baseline sample count
- `drift_current_samples` - Current sample count
- `drift_checks_total{status}` - Total drift checks by status

## Next Steps

1. ✅ Test locally (covered above)
2. Deploy to EKS (see Kubernetes deployment section)
3. Set up alerts in Prometheus (optional)
4. Configure Slack/email notifications (optional)
5. Add custom dashboards as needed
