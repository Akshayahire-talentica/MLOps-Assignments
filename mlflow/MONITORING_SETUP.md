# MLOps Monitoring Setup - Prometheus & Grafana

## ✅ Current Status

### Deployed Components
- **Prometheus**: Running on http://13.127.21.160:30900
- **Grafana**: Running on http://13.127.21.160:30300
  - Username: `admin`
  - Password: `admin123`

### Active Metrics Collection
Prometheus is successfully scraping:
1. ✅ **mlops-api** - FastAPI application metrics
2. ✅ **mlops-model-v2** - Model serving metrics  
3. ✅ **prometheus** - Prometheus self-monitoring
4. ⚠️ **mlflow** - Configured but MLflow doesn't expose /metrics endpoint (404)
5. ⚠️ **mlops-router** - Pod may be down

## 📊 Available Dashboards

Grafana has been configured with MLOps-specific dashboards:
- **MLOps Overview** - Model & Data Monitoring dashboard
- Located at: Dashboards → MLOps folder

### Dashboard Panels Configured
1. **Model Performance**:
   - RMSE (Root Mean Square Error)
   - MAE (Mean Absolute Error)
   - Reconstruction Error
   - Model Components

2. **Data Drift Detection**:
   - Drift Detection Status
   - Drift Score
   - Number of Drifted Columns
   - Share of Drifted Columns

3. **Pipeline Health**:
   - Data Files Count in S3
   - Feature Files Count
   - Model Files Count
   - Last Data Update Timestamp

4. **API Performance**:
   - Request Rate
   - Response Time (p95 latency)
   - Error Rate

## 🎯 Important Metrics for MLOps

### Model Quality Metrics
These metrics help track model performance degradation:
- **RMSE (Root Mean Square Error)**: Lower is better, measures prediction accuracy
- **MAE (Mean Absolute Error)**: Average absolute difference between predictions and actual values
- **Reconstruction Error**: From PCA/feature extraction, indicates feature quality

**Threshold Alerts**:
- 🟢 Green: RMSE < 3.0, MAE < 2.5
- 🟡 Yellow: RMSE 3.0-4.0, MAE 2.5-3.5  
- 🔴 Red: RMSE > 4.0, MAE > 3.5

### Data Drift Metrics
Critical for detecting when training data differs from production data:
- **Dataset Drift Detected**: Binary flag (0=no drift, 1=drift detected)
- **Drift Score**: Overall drift metric (0.0-1.0, higher means more drift)
- **Number of Drifted Columns**: Count of features showing significant drift
- **Share of Drifted Columns**: Percentage of features with drift

**Threshold Alerts**:
- 🟢 Green: Drift score < 0.3, no columns drifted
- 🟡 Yellow: Drift score 0.3-0.6, < 30% columns drifted
- 🔴 Red: Drift score > 0.6, > 30% columns drifted

### Pipeline Health Metrics
Monitor the health of the MLOps pipeline:
- **Data Files Count**: Should be > 0 (indicates data ingestion working)
- **Feature Files Count**: Should be > 0 (indicates feature engineering working)
- **Model Files Count**: Should be > 0 (indicates model training working)
- **Last Data Update**: Timestamp of most recent data processing

### API Performance Metrics
Monitor production API health:
- **Request Rate**: Requests per second (should be stable)
- **Response Time p95**: 95th percentile latency (should be < 1s for good UX)
- **Error Rate**: 4xx/5xx responses (should be < 1%)

## 🔧 Current Configuration

### Prometheus Scrape Configs
Located in: `k8s-minimal/05-prometheus-minimal.yaml`

```yaml
scrape_configs:
  - job_name: 'prometheus' # Self-monitoring
  - job_name: 'mlops-api' # Main API service
  - job_name: 'mlops-model-v2' # Model serving
  - job_name: 'mlops-router' # Traffic routing
  - job_name: 'mlflow' # Experiment tracking (404 currently)
  - job_name: 'mlops-metrics-exporter' # Custom metrics (pending)
  - job_name: 'kubernetes-pods' # Generic pod discovery
```

### Grafana Dashboards
Located in: `k8s-minimal/11-grafana-dashboards.yaml`

Dashboard includes:
- 13 panels covering model performance, drift, and pipeline health
- Auto-refresh every 30 seconds
- Color-coded thresholds for easy status visualization
- Time range: Last 1 hour (adjustable)

## 📝 Next Steps for Complete Monitoring

### 1. Add Custom Metrics Exporter (Optional)
The `k8s-minimal/10-mlops-metrics-exporter.yaml` file contains a Python-based metrics exporter that:
- Fetches model metrics from MLflow API
- Reads drift reports from S3
- Exposes custom `mlops_*` metrics for Prometheus

**Status**: Configured but needs AWS credentials setup in pod

**Alternative**: Add metrics directly to existing FastAPI services (recommended)

### 2. Enable MLflow Metrics
MLflow doesn't natively expose Prometheus metrics. Options:
- Use MLflow metrics API and poll from exporter (implemented)
- Display metrics in Streamlit UI (already working ✅)
- Add Prometheus exporter sidecar to MLflow pod

### 3. Set Up Alerts
Create Prometheus AlertManager rules for:
- High model error (RMSE > 4.0)
- Data drift detected
- Pipeline failures (no new data in 24h)
- API errors > 1%
- High latency > 5s p95

### 4. Add Historical Metrics Storage
Current setup uses emptyDir (ephemeral storage). For production:
- Add PersistentVolume for Prometheus data retention
- Configure longer retention period (currently 7 days)
- Set up Thanos or Cortex for long-term storage

## 🚀 How to Use Grafana

### Access Grafana
1. Open: http://13.127.21.160:30300
2. Login: admin / admin123
3. Navigate to: Dashboards → MLOps folder → MLOps Overview

### Viewing Metrics
- **Real-time**: Dashboard auto-refreshes every 30s
- **Historical**: Use time picker (top right) to select time range
- **Custom queries**: Click panel title → Edit to modify queries

### Creating Custom Panels
1. Click "+ Add Panel" button
2. Select Prometheus as data source
3. Enter PromQL query, e.g.:
   ```promql
   mlops_model_rmse
   mlops_drift_score
   rate(http_requests_total[5m])
   ```
4. Configure visualization (graph, gauge, stat, etc.)
5. Set thresholds for color-coding
6. Save dashboard

## 🔍 Useful PromQL Queries

### Model Metrics
```promql
# Current RMSE
mlops_model_rmse

# RMSE change over last hour
delta(mlops_model_rmse[1h])

# Average MAE over 5 minutes
avg_over_time(mlops_model_mae[5m])
```

### Drift Detection
```promql
# Drift status (0 or 1)
mlops_drift_detected

# Drift percentage
mlops_drift_columns_share * 100

# Alert if drift detected
mlops_drift_detected > 0
```

### API Performance
```promql
# Request rate per minute
rate(http_requests_total[1m])

# 95th percentile latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Error rate percentage
(rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m])) * 100
```

### Pipeline Health
```promql
# Files processed in last hour
increase(mlops_data_files_count[1h])

# Time since last data update (seconds)
time() - mlops_last_data_update_timestamp
```

## 🎬 Monitoring Workflow

### Daily Monitoring
1. **Check Grafana Dashboard** (morning)
   - Model performance trending
   - Any drift detected?
   - Pipeline running smoothly?
   - API performance acceptable?

2. **Review Alerts** (if configured)
   - Acknowledge/investigate any alerts
   - Check Streamlit UI for detailed drift reports

3. **Trend Analysis** (weekly)
   - Is model performance degrading?
   - Seasonal patterns in drift?
   - API traffic patterns

### When Alerts Fire

#### High Model Error Alert
1. Check recent drift reports
2. Review recent data quality
3. Check if training data is stale
4. Consider retraining model

#### Drift Detected Alert
1. Open Streamlit UI → Monitoring tab → Data Drift section
2. Review drifted columns
3. Determine if drift is:
   - Expected (seasonal, new user behavior)
   - Concerning (data quality issue, system bug)
4. If concerning: retrain model with new data

#### Pipeline Failure Alert
1. Check Airflow UI: http://13.127.21.160:30080
2. Review DAG run logs
3. Check Kubernetes pod logs
4. Verify S3 connectivity and data availability

#### API Performance Alert
1. Check Grafana for request volume spike
2. Review slow endpoints
3. Check pod resource usage (CPU/memory)
4. Consider horizontal scaling if needed

## 💾 Data Sources

### Metrics Flow
```
S3 (Drift Reports) ────┐
                       ├──→ Metrics Exporter ──→ Prometheus ──→ Grafana
MLflow (Model Metrics) ─┤                              ↓
                        │                          Alerting
FastAPI Apps ───────────┘
```

### Current Data in System
- **Model Metrics**: RMSE=3.36, MAE=3.19 (in MLflow)
- **Drift Reports**: 10+ JSON files in S3 `reports/drift/`
- **API Metrics**: Request count, latency from FastAPI `/metrics`

## 🛠️ Troubleshooting

### Grafana shows "No Data"
1. Check Prometheus is scraping: http://13.127.21.160:30900/targets
2. Verify metric name in PromQL query
3. Check time range (may be outside data retention)
4. Verify Prometheus data source configured in Grafana

### Metrics not updating
1. Check Prometheus scrape interval (15-30s)
2. Verify target pods are running: `kubectl get pods -n mlops`
3. Check Prometheus logs: `kubectl logs -n mlops prometheus-xxx`
4. Restart Prometheus: `kubectl rollout restart deployment/prometheus -n mlops`

### Dashboard not loading
1. Check Grafana pod status: `kubectl get pods -n mlops -l app=grafana`
2. Check logs: `kubectl logs -n mlops grafana-xxx`
3. Verify ConfigMaps: `kubectl get configmap -n mlops | grep grafana`
4. Restart Grafana: `kubectl rollout restart deployment/grafana -n mlops`

### Custom metrics missing
1. Check metrics exporter pod: `kubectl get pods -n mlops -l app=mlops-metrics-exporter`
2. Test metrics endpoint: `kubectl exec -n mlops <pod> -- curl localhost:8000/metrics`
3. Verify AWS credentials if using S3 access
4. Check exporter logs for errors

## 📚 Additional Resources

### Prometheus
- Query language: https://prometheus.io/docs/prometheus/latest/querying/basics/
- Best practices: https://prometheus.io/docs/practices/naming/

### Grafana
- Dashboard guide: https://grafana.com/docs/grafana/latest/dashboards/
- Panel types: https://grafana.com/docs/grafana/latest/panels/

### MLOps Monitoring
- Model monitoring best practices
- Data drift detection techniques
- A/B testing and canary deployments

## 🎯 Success Criteria

Your monitoring setup is working correctly when:
- ✅ Grafana dashboard loads and shows data
- ✅ Model metrics (RMSE, MAE) are visible
- ✅ Drift detection status is displayed
- ✅ API performance metrics show request rate and latency
- ✅ Pipeline health indicators are green
- ✅ Streamlit UI shows metrics and drift reports
- ✅ Prometheus targets are "up" (except MLflow)

---

**Last Updated**: February 17, 2026
**Status**: Prometheus and Grafana deployed and operational ✅
**Next Step**: Verify dashboards in Grafana UI and add alerting rules
