# MLOps Model Performance & Data Drift Monitoring Dashboard

## ✅ Dashboard Successfully Created!

Your comprehensive MLOps monitoring dashboard for **Model Performance** and **Data Drift Detection** is now available.

### 📊 Dashboard Access

**Primary Dashboard: Model Performance & Data Drift**
- URL: http://13.127.21.160:30300/d/deef0ca3-9cc7-4a0a-9b25-225b899968d8/mlops-model-performance-and-data-drift
- Username: `admin`
- Password: `admin123`

**Additional Dashboards**:
- General Monitoring: http://13.127.21.160:30300/d/e25178a0-e76c-4db8-8978-56fc926bba8c/mlops-monitoring
- All Dashboards: http://13.127.21.160:30300/dashboards

---

## 📈 Dashboard Panels Overview

### Section 1: Model Performance Monitoring

#### 1. **Model RMSE (Root Mean Squared Error)**
- **Type**: Time Series Graph
- **Metric**: `model_rmse`
- **Description**: Tracks model prediction error over time
- **Thresholds**:
  - 🟢 Green: < 0.8 (Good performance)
  - 🟡 Yellow: 0.8 - 1.2 (Warning)
  - 🔴 Red: > 1.2 (Poor performance)
- **Use**: Monitor if model accuracy is degrading

#### 2. **Model MAE (Mean Absolute Error)**
- **Type**: Time Series Graph
- **Metric**: `model_mae`
- **Description**: Average absolute difference between predictions and actuals
- **Thresholds**:
  - 🟢 Green: < 0.6
  - 🟡 Yellow: 0.6 - 0.9
  - 🔴 Red: > 0.9
- **Use**: Track prediction quality trends

#### 3. **Total Predictions Made**
- **Type**: Stat Panel
- **Metric**: `sum(model_predictions_total)`
- **Description**: Cumulative count of all predictions
- **Use**: Monitor prediction volume and service utilization

#### 4. **Prediction Rate (per second)**
- **Type**: Stat Panel
- **Metric**: `rate(model_predictions_total[5m])`
- **Description**: Current prediction throughput
- **Use**: Capacity planning and load monitoring

#### 5. **Prediction Latency (p50, p95, p99)**
- **Type**: Time Series Graph
- **Metrics**: Histogram quantiles of `model_prediction_latency_seconds_bucket`
- **Description**: Response time percentiles
- **Thresholds**:
  - 🟢 Green: < 500ms
  - 🟡 Yellow: 500ms - 1s
  - 🔴 Red: > 1s
- **Use**: Detect performance degradation and latency spikes

---

### Section 2: Data Drift Monitoring

#### 6. **Data Drift Score by Feature**
- **Type**: Time Series Graph
- **Metric**: `drift_score{feature="..."}`
- **Description**: Statistical drift detection score for each feature
- **Thresholds**:
  - 🟢 Green: < 0.3 (No significant drift)
  - 🟡 Yellow: 0.3 - 0.5 (Warning - monitor)
  - 🔴 Red: > 0.5 (Critical - retraining recommended)
- **Use**: Identify which features are drifting from training distribution
- **Alert**: Configured to alert when score > 0.5

#### 7. **Drift Detection Status**
- **Type**: Stat Panel
- **Metric**: `drift_detected`
- **Values**:
  - 🟢 "✓ No Drift" (value=0)
  - 🔴 "⚠ Drift Detected" (value=1)
- **Use**: Quick visual indicator of drift status

#### 8. **Drift Severity**
- **Type**: Gauge
- **Metric**: `drift_severity`
- **Levels**:
  - 🟢 0 = None
  - 🟡 1 = Warning
  - 🔴 2 = Critical
- **Use**: Understand drift urgency for retraining decisions

#### 9. **Drift Checks Performed**
- **Type**: Stat Panel
- **Metric**: `sum(drift_checks_total)`
- **Description**: Total number of drift detection runs
- **Use**: Verify drift monitoring is running

#### 10. **Sample Sizes (Baseline vs Current)**
- **Type**: Time Series Graph
- **Metrics**: 
  - `drift_baseline_samples` - Training data sample size
  - `drift_current_samples` - Production data sample size
- **Use**: Ensure sufficient data for valid drift detection

---

### Section 3: System Health Monitoring

#### 11. **API Error Rate**
- **Type**: Time Series Graph
- **Metric**: `rate(model_api_errors_total[5m])`
- **Description**: Errors per second by type
- **Thresholds**:
  - 🟢 Green: < 0.01 (< 1% error rate)
  - 🟡 Yellow: 0.01 - 0.05 (1-5% error rate)
  - 🔴 Red: > 0.05 (> 5% error rate)
- **Use**: Detect service issues and prediction failures

#### 12. **Active Requests**
- **Type**: Time Series Graph
- **Metric**: `model_api_active_requests`
- **Description**: Current in-flight requests
- **Use**: Monitor concurrent load and potential bottlenecks

---

## 🔍 Key Metrics Explained

### Model Performance Metrics

| Metric | Description | Source | Importance |
|--------|-------------|--------|------------|
| `model_rmse` | Root Mean Squared Error | Model evaluation | Primary accuracy metric |
| `model_mae` | Mean Absolute Error | Model evaluation | Alternative accuracy metric |
| `model_accuracy` | Model R² score | Model evaluation | Overall performance |
| `model_predictions_total` | Total prediction count | API service | Usage tracking |
| `model_prediction_latency_seconds` | Prediction response time | API service | Performance monitoring |

### Data Drift Metrics

| Metric | Description | Calculation | Action Threshold |
|--------|-------------|-------------|------------------|
| `drift_score` | Statistical distance between distributions | KL Divergence / PSI | > 0.5 |
| `drift_detected` | Binary drift flag | Threshold-based | = 1 |
| `drift_severity` | Drift urgency level | Multi-threshold | = 2 (Critical) |
| `drift_checks_total` | Monitoring runs performed | Counter | N/A |
| `drift_baseline_samples` | Training data size | Configuration | > 1000 recommended |
| `drift_current_samples` | Production data size | Windowed | > 100 for valid test |

---

## 🎯 MLOps Best Practices

### Model Performance Monitoring

1. **Set Baselines**:
   - Record RMSE/MAE from initial model training
   - Set alert thresholds at 10-20% degradation
   - Review metrics weekly

2. **Track Trends**:
   - Look for gradual performance degradation over time
   - Sudden drops may indicate data quality issues
   - Compare to business metrics (e.g., user satisfaction)

3. **Latency Monitoring**:
   - p95 < 500ms is good for recommendation systems
   - p99 should be < 1s for acceptable user experience
   - High latency may indicate resource constraints

### Data Drift Detection

1. **Regular Monitoring**:
   - Run drift detection daily or weekly
   - Focus on features with business significance
   - Investigate any feature drift > 0.3

2. **Root Cause Analysis**:
   - Check for data collection changes
   - Verify upstream data pipelines
   - Compare to expected seasonal patterns

3. **Retraining Triggers**:
   - **Critical Drift** (severity=2): Retrain immediately
   - **Warning Drift** (severity=1): Schedule retraining within 1-2 weeks
   - **Multiple Features Drifting**: High priority for retraining

4. **Validation**:
   - Ensure baseline has sufficient samples (> 1000)
   - Use sliding window for current data (e.g., last 7 days)
   - Consider different drift metrics for different feature types

---

## 🚨 Alerting Recommendations

### Critical Alerts (Immediate Action)

1. **Model Performance Degradation**:
   ```promql
   model_rmse > 1.2
   ```
   Action: Investigate immediately, prepare rollback

2. **Critical Data Drift**:
   ```promql
   drift_severity == 2
   ```
   Action: Initiate emergency retraining

3. **High Error Rate**:
   ```promql
   rate(model_api_errors_total[5m]) > 0.05
   ```
   Action: Check service health, review logs

### Warning Alerts (Monitor Closely)

1. **Performance Warning**:
   ```promql
   model_rmse > 0.8 and model_rmse < 1.2
   ```
   Action: Schedule retraining, monitor daily

2. **Drift Warning**:
   ```promql
   drift_severity == 1
   ```
   Action: Investigate causes, plan retraining

3. **High Latency**:
   ```promql
   histogram_quantile(0.95, rate(model_prediction_latency_seconds_bucket[5m])) > 0.5
   ```
   Action: Check resource utilization, optimize if needed

---

## 🔧 Troubleshooting

### No Data in Panels

1. **Check Prometheus is scraping**:
   ```bash
   curl http://13.127.21.160:30900/api/v1/query?query=up
   ```

2. **Verify metrics endpoint**:
   ```bash
   curl http://13.127.21.160:30000/metrics | grep model_
   ```

3. **Generate predictions** to populate metrics:
   ```bash
   curl -X POST http://13.127.21.160:30000/predict \
     -H "Content-Type: application/json" \
     -d '{
       "user_id": 1,
       "movie_id": 50,
       "user_avg_rating": 3.5,
       "user_rating_count": 100,
       "movie_popularity": 0.8,
       "movie_avg_rating": 4.2,
       "day_of_week": 5,
       "month": 2
     }'
   ```

4. **Check time range**: Ensure dashboard time range includes recent data

### Metrics Not Updating

1. **Restart API service**:
   ```bash
   kubectl rollout restart deployment/mlops-api -n mlops
   ```

2. **Check pod logs**:
   ```bash
   kubectl logs -n mlops deployment/mlops-api --tail=50
   ```

3. **Verify Prometheus targets**:
   - Go to http://13.127.21.160:30900/targets
   - Ensure mlops-api target is "UP"

### Drift Metrics Missing

Drift metrics require:
1. Baseline data in S3: `s3://mlops-poc-data-<account>/baseline/`
2. Drift detection DAG running in Airflow
3. Drift detector pushing metrics to Prometheus

To trigger drift detection:
```bash
# Via Airflow UI
# Go to http://13.127.21.160:30080
# Trigger "drift_detection_dag" manually
```

---

## 📚 Additional Resources

### Prometheus Queries

Access Prometheus at: http://13.127.21.160:30900

**Useful queries**:

1. **Average RMSE over 1 hour**:
   ```promql
   avg_over_time(model_rmse[1h])
   ```

2. **Prediction rate by model type**:
   ```promql
   rate(model_predictions_total[5m])
   ```

3. **Features with drift > 0.3**:
   ```promql
   drift_score > 0.3
   ```

4. **Error rate percentage**:
   ```promql
   rate(model_api_errors_total[5m]) / rate(model_api_requests_total[5m]) * 100
   ```

### Dashboard Customization

To customize the dashboard:
1. Click the gear icon ⚙️ in Grafana
2. Edit panels, thresholds, or queries
3. Save changes
4. Export JSON for version control

### Integration with MLflow

The metrics are automatically populated from MLflow:
- Model RMSE/MAE: Read from MLflow run metrics
- Model version: Tracked from MLflow registry
- Updates on model reload or service restart

---

## 🎬 Quick Start Guide

1. **Access Dashboard**:
   ```
   http://13.127.21.160:30300
   Login: admin / admin123
   Navigate to: "MLOps - Model Performance & Data Drift"
   ```

2. **Generate Sample Data**:
   ```bash
   python3 scripts/initialize_monitoring_metrics.py
   ```

3. **Wait for Metrics** (30-60 seconds for Prometheus scrape)

4. **Verify Data**:
   - Check "Total Predictions" panel shows > 0
   - Verify RMSE/MAE panels have values
   - Confirm time range is "Last 15 minutes" or "Last 1 hour"

5. **Set Up Alerts** (Optional):
   - Click panel title → "Edit"
   - Go to "Alert" tab
   - Configure alert rules
   - Add notification channels

---

## 📞 Support

For issues or questions:
- Check [GRAFANA_DASHBOARD_GUIDE.md](GRAFANA_DASHBOARD_GUIDE.md) for general monitoring
- Review Prometheus logs: `kubectl logs -n mlops deployment/prometheus --tail=100`
- Review API service logs: `kubectl logs -n mlops deployment/mlops-api --tail=100`
- Check drift detector logs: `kubectl logs -n mlops -l app=drift-detector --tail=100`

---

**Last Updated**: February 17, 2026
**Dashboard Version**: 1.0
**Prometheus Version**: 2.48.0
**Grafana Version**: 10.2.0
