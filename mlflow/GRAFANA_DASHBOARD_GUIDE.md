# Grafana Dashboard Access Guide

## ✅ Dashboard Created Successfully!

Your MLOps monitoring dashboards are now available in Grafana.

### 📊 Available Dashboards

1. **MLOps Monitoring Dashboard** (Main)
   - URL: http://13.127.21.160:30300/d/e25178a0-e76c-4db8-8978-56fc926bba8c/mlops-monitoring
   - Panels:
     - Active Services (total count)
     - API Service Uptime status
     - Model V2 Service Uptime status
     - Service Status Table (all services)
     - HTTP Request Rate chart
     - HTTP Request Duration (p95) chart
     - Process Memory Usage chart
     - CPU Usage chart

2. **MLOps Test Dashboard** (Simple)
   - URL: http://13.127.21.160:30300/d/e4fdc1bb-87dd-4803-916b-823ad4324378/mlops-test
   - Simple dashboard with Services Up metric

### 🔐 Access Information

- **Grafana URL**: http://13.127.21.160:30300
- **Username**: admin
- **Password**: admin123

### 📈 Current Metrics Status

**Active Services** (from Prometheus):
- ✅ mlops-api: UP (8000/metrics)
- ✅ mlops-model-v2: UP (8080/metrics)
- ✅ prometheus: UP (self-monitoring)
- ❌ mlflow: DOWN (404 - doesn't expose /metrics endpoint)
- ❌ mlops-router: DOWN (connection refused)
- ❌ mlops-metrics-exporter: DOWN (not deployed)

**Available Metrics**:
- `up` - Service availability (1=up, 0=down)
- `process_resident_memory_bytes` - Memory usage per service
- `process_cpu_seconds_total` - CPU time per service
- `http_requests_total` - Total HTTP requests (if instrumented)
- `http_request_duration_seconds_bucket` - Request latency histogram

### 🎯 Using Prometheus for Queries

Access Prometheus directly at: http://13.127.21.160:30900

**Useful PromQL Queries**:

1. **Check which services are up**:
   ```promql
   up
   ```

2. **Count active services**:
   ```promql
   sum(up)
   ```

3. **Memory usage by service**:
   ```promql
   process_resident_memory_bytes
   ```

4. **HTTP request rate (5 min average)**:
   ```promql
   rate(http_requests_total[5m])
   ```

5. **95th percentile request duration**:
   ```promql
   histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
   ```

### 🔍 Troubleshooting

#### Dashboard shows "No data"

1. **Check Prometheus is collecting metrics**:
   ```bash
   curl -s http://13.127.21.160:30900/api/v1/query?query=up | jq '.data.result'
   ```

2. **Verify Grafana datasource**:
   - Go to Configuration → Data Sources
   - Check Prometheus connection is successful

3. **Generate some traffic** to populate HTTP metrics:
   ```bash
   for i in {1..20}; do
     curl -s http://13.127.21.160:30000/health
     curl -s http://13.127.21.160:30001/health
     sleep 1
   done
   ```

4. **Check time range** in Grafana:
   - Make sure time range includes recent data
   - Try "Last 15 minutes" or "Last 1 hour"

#### Metrics missing

Some metrics like `http_requests_total` may not be available if services don't expose them. You can:

1. **Check what metrics are available**:
   ```bash
   curl -s http://13.127.21.160:30000/metrics
   curl -s http://13.127.21.160:30001/metrics
   ```

2. **Add custom metrics** to your Python services using prometheus_client:
   ```python
   from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
   
   REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint'])
   REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')
   ```

### 🚀 Next Steps

1. **Add more services to monitoring**:
   - Fix mlops-router deployment
   - Add MLflow metrics exporter (optional)
   - Add Airflow metrics

2. **Create custom dashboards** for:
   - Model Performance Monitoring (RMSE, MAE, R²)
   - Data Drift Detection (drift scores, feature distributions)
   - Pipeline Health (DAG success/failure rates)
   - Training Job Metrics (training time, epochs, loss curves)

3. **Set up alerting**:
   - Create alert rules in Prometheus
   - Configure notification channels in Grafana (email, Slack, etc.)
   - Example alerts:
     - Service down for > 5 minutes
     - High error rate (> 5%)
     - High memory usage (> 80%)
     - Model drift detected

4. **Enhanced MLOps metrics** (requires custom exporter):
   - Model accuracy trends
   - Prediction drift scores
   - Feature importance changes
   - Training data quality metrics

### 📝 Dashboard Maintenance

**To update dashboards**:
1. Edit in Grafana UI
2. Save changes
3. Export JSON from dashboard settings
4. Store in version control

**To create new dashboards**:
```bash
curl -X POST -u admin:admin123 \
  http://13.127.21.160:30300/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @my-dashboard.json
```

### 🎨 Dashboard Customization

In Grafana UI you can:
- Add/remove panels
- Change visualization types (graph, stat, gauge, table, etc.)
- Set custom thresholds and colors
- Create variables for dynamic filtering
- Set up panel links and drill-downs
- Configure auto-refresh intervals

---

## Quick Access Links

- **Main Dashboard**: http://13.127.21.160:30300/d/e25178a0-e76c-4db8-8978-56fc926bba8c/mlops-monitoring
- **Prometheus**: http://13.127.21.160:30900
- **Grafana Home**: http://13.127.21.160:30300

**Note**: If you don't see data immediately, wait 30-60 seconds for Prometheus to scrape metrics, or generate some API traffic to populate the metrics.
