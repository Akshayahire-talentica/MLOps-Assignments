# ML Feedback Loop - Deployment Guide

## Overview

This guide explains how to deploy and operate the complete ML feedback loop system.

## Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                    DEPLOYED SERVICES                         │
└─────────────────────────────────────────────────────────────┘

1. API Service (Port 8000)          - Recommendations + predictions
2. Event Collector (Port 8002)      - Feedback event collection
3. MLflow (Port 5000)               - Model registry
4. PostgreSQL (Port 5432)           - Event storage
5. Airflow (Port 8080)              - Retraining orchestration
6. Prometheus (Port 9091)           - Metrics collection
7. Grafana (Port 3000)              - Monitoring dashboards
```

## Step-by-Step Deployment

### 1. Initialize Database Schema

```bash
# Run schema creation
docker exec -i mlops-postgres psql -U mlops -d mlops_db < src/feedback/schema.sql

# Verify tables created
docker exec mlops-postgres psql -U mlops -d mlops_db -c "\dt"
```

Expected output:
```
            List of relations
 Schema |         Name          | Type  | Owner 
--------+-----------------------+-------+-------
 public | recommendation_logs   | table | mlops
 public | user_events           | table | mlops
 public | training_labels       | table | mlops
 public | model_performance     | table | mlops
 public | retraining_triggers   | table | mlops
 public | ab_test_results       | table | mlops
```

### 2. Add Event Collector to Docker Compose

Add this service to `docker-compose.yml`:

```yaml
  event-collector:
    build:
      context: .
      dockerfile: Dockerfile.event_collector
    image: mlops-event-collector:local
    container_name: mlops-event-collector
    restart: unless-stopped
    ports:
      - "8002:8002"
    environment:
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DB: mlops_db
      POSTGRES_USER: mlops
      POSTGRES_PASSWORD: mlops123
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - mlops-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8002/health"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
```

### 3. Rebuild and Restart Services

```bash
# Stop services
docker compose down

# Build new images
docker compose build api-service event-collector

# Start all services
docker compose up -d

# Verify all healthy
docker compose ps

# Check event collector
curl http://localhost:8002/health
```

### 4. Test the Feedback Loop

```bash
# Test recommendation endpoint
curl "http://localhost:8000/recommend?user_id=123&top_k=5"

# Test event collection
curl -X POST http://localhost:8002/events/click \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 123,
    "item_id": 456,
    "position": 0,
    "is_synthetic": false
  }'

# Check events in database
docker exec mlops-postgres psql -U mlops -d mlops_db \
  -c "SELECT event_type, COUNT(*) FROM user_events GROUP BY event_type;"
```

### 5. Run User Behavior Simulation

```bash
# Install dependencies (if not already installed)
pip install numpy requests

# Run simulator (start small)
python scripts/simulate_user_behavior_v2.py \
  --users 50 \
  --sessions 100 \
  --delay 100

# Run larger simulation
python scripts/simulate_user_behavior_v2.py \
  --users 500 \
  --sessions 10000 \
  --delay 50
```

Expected output:
```
================================================================================
SIMULATION COMPLETE
================================================================================
Duration: 120.5s
Sessions: 10000
Rate: 83.0 sessions/s

Event Statistics:
  Impressions: 10000
  Clicks: 1850
  Watches: 925
  Feedback: 278
  Errors: 0

Click-Through Rate: 18.50%
Watch Rate: 50.00%
Feedback Rate: 30.05%

All events tagged as synthetic (is_synthetic=true)
================================================================================
```

### 6. Trigger Retraining DAG

```bash
# Via Airflow UI
open http://localhost:8080
# Login: admin / admin123
# Find DAG: feedback_loop_retraining
# Click "Trigger DAG"

# Or via CLI
docker exec mlops-airflow-webserver \
  airflow dags trigger feedback_loop_retraining
```

### 7. Monitor the System

```bash
# View event collector stats
curl http://localhost:8002/stats | jq

# View Prometheus metrics
open http://localhost:9091

# View Grafana dashboards
open http://localhost:3000
# Login: admin / admin123
```

## Monitoring Setup

### Key Metrics to Track

#### 1. Business Metrics

**Click-Through Rate (CTR)**
```sql
SELECT 
    DATE(r.created_at) as date,
    COUNT(DISTINCT r.recommendation_id) as impressions,
    COUNT(DISTINCT e.recommendation_id) as clicks,
    ROUND(COUNT(DISTINCT e.recommendation_id)::NUMERIC / 
          COUNT(DISTINCT r.recommendation_id) * 100, 2) as ctr_percent
FROM recommendation_logs r
LEFT JOIN user_events e ON r.recommendation_id = e.recommendation_id 
    AND e.event_type = 'click'
WHERE r.created_at >= CURRENT_DATE - 7
GROUP BY DATE(r.created_at);
```

**Watch Rate**
```sql
SELECT 
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN event_id END) as clicks,
    COUNT(DISTINCT CASE WHEN event_type = 'watch' THEN event_id END) as watches,
    ROUND(COUNT(DISTINCT CASE WHEN event_type = 'watch' THEN event_id END)::NUMERIC /
          COUNT(DISTINCT CASE WHEN event_type = 'click' THEN event_id END) * 100, 2) as watch_rate
FROM user_events
WHERE created_at >= CURRENT_DATE - 7;
```

**Average Engagement Score**
```sql
SELECT 
    AVG(label) as avg_engagement,
    MIN(label) as min_engagement,
    MAX(label) as max_engagement
FROM training_labels
WHERE label_date >= CURRENT_DATE - 7;
```

#### 2. Model Metrics

Query MLflow for latest model metrics:

```python
import mlflow

client = mlflow.tracking.MlflowClient(tracking_uri="http://localhost:5000")
model_versions = client.get_latest_versions("nmf_recommendation_production", stages=["Production"])

if model_versions:
    run = client.get_run(model_versions[0].run_id)
    print(f"RMSE: {run.data.metrics.get('rmse')}")
    print(f"MAE: {run.data.metrics.get('mae')}")
```

#### 3. System Metrics

**Prometheus Queries** (access at http://localhost:9091):

```promql
# Request rate
rate(events_collected_total[5m])

# Event collection latency
histogram_quantile(0.95, rate(event_collection_latency_seconds_bucket[5m]))

# Error rate
rate(event_collector_db_errors_total[5m])

# API latency
histogram_quantile(0.95, rate(model_prediction_latency_seconds_bucket[5m]))
```

### Alert Rules

Create `monitoring/prometheus/alert_rules.yml`:

```yaml
groups:
  - name: ml_feedback_loop
    interval: 1m
    rules:
      # CTR drops below 15%
      - alert: LowCTR
        expr: |
          (
            sum(rate(events_collected_total{event_type="click"}[1h]))
            / 
            sum(rate(events_collected_total[event_type="impression"}[1h]))
          ) < 0.15
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Click-through rate is low"
          description: "CTR has dropped below 15% for 15 minutes"
      
      # High error rate
      - alert: HighErrorRate
        expr: rate(event_collector_db_errors_total[5m]) > 10
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate in event collection"
      
      # Model latency spike
      - alert: HighLatency
        expr: |
          histogram_quantile(0.95,
            rate(model_prediction_latency_seconds_bucket[5m])
          ) > 0.2
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Model prediction latency is high"
```

### Grafana Dashboard

Import this dashboard JSON at http://localhost:3000:

Key panels to include:
1. **CTR Over Time** (line chart)
2. **Watch Rate** (line chart)
3. **Event Volume** (stacked area chart - impressions, clicks, watches)
4. **Model Performance** (gauge - RMSE, MAE)
5. **Latency Heatmap** (heatmap)
6. **Error Rate** (line chart)
7. **Synthetic vs Real Events** (pie chart)
8. **Top Recommended Items** (bar chart)

## Retraining Workflow

### Automatic Retraining Triggers

The system automatically triggers retraining when:

1. **Scheduled**: Every 7 days (cron: `0 2 * * 0`)
2. **Event threshold**: > 10,000 new events
3. **Performance drop**: CTR drops > 10%
4. **Manual**: Triggered via Airflow UI

### Manual Retraining

```bash
# Trigger via Airflow
docker exec mlops-airflow-webserver \
  airflow dags trigger feedback_loop_retraining

# Or create manual trigger in DB
docker exec mlops-postgres psql -U mlops -d mlops_db -c \
  "INSERT INTO retraining_triggers (trigger_type, trigger_reason, training_status) 
   VALUES ('manual', 'Manual trigger from ops team', 'pending');"
```

### Canary Deployment

After a new model passes evaluation gates:

1. **Promote to Staging**:
   ```python
   import mlflow
   
   client = mlflow.tracking.MlflowClient()
   client.transition_model_version_stage(
       name="nmf_recommendation_production",
       version="5",
       stage="Staging"
   )
   ```

2. **Route 10% Traffic**: Update router service
3. **Monitor for 24 hours**: Check CTR, errors, latency
4. **Promote or Rollback**:
   - If successful → promote to Production
   - If issues → rollback to previous version

### Rollback Procedure

```python
import mlflow

client = mlflow.tracking.MlflowClient()

# Rollback to previous version
client.transition_model_version_stage(
    name="nmf_recommendation_production",
    version="4",  # previous version
    stage="Production",
    archive_existing_versions=True
)

# Restart services to load new model
# docker compose restart api-service
```

## Data Management

### Separate Synthetic vs Real Data

```sql
-- Query only real user data
SELECT * FROM user_events WHERE is_synthetic = false;

-- Training on mixed data, evaluate on real data
SELECT * FROM training_labels 
WHERE label_date >= CURRENT_DATE - 30
AND is_synthetic = false;  -- for validation set
```

### Data Retention

Archive old events to keep database performant:

```sql
-- Archive events older than 90 days
INSERT INTO user_events_archive 
SELECT * FROM user_events 
WHERE created_at < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM user_events 
WHERE created_at < CURRENT_DATE - INTERVAL '90 days';
```

Run via cron monthly or Airflow DAG.

## Troubleshooting

### Event Collector Not Working

```bash
# Check service health
curl http://localhost:8002/health

# Check logs
docker logs mlops-event-collector --tail 100

# Check database connection
docker exec mlops-postgres psql -U mlops -d mlops_db -c "SELECT 1;"
```

### No Events Being Collected

```bash
# Test endpoint manually
curl -X POST http://localhost:8002/events/impression \
  -H "Content-Type: application/json" \
  -d '{
    "recommendation_id": "test-123",
    "user_id": 1,
    "item_ids": [1, 2, 3],
    "model_name": "test_model",
    "model_version": "v1",
    "is_synthetic": true
  }'

# Check database
docker exec mlops-postgres psql -U mlops -d mlops_db \
  -c "SELECT COUNT(*) FROM user_events;"
```

### Retraining Not Triggering

```bash
# Check trigger conditions
docker exec mlops-postgres psql -U mlops -d mlops_db -c \
  "SELECT COUNT(*) FROM user_events WHERE created_at >= CURRENT_DATE - 7;"

# Manually trigger
docker exec mlops-airflow-webserver \
  airflow dags trigger feedback_loop_retraining

# Check DAG logs
docker logs mlops-airflow-scheduler --tail 100
```

## Production Checklist

- [ ] Database schema created
- [ ] Event collector deployed and healthy
- [ ] Recommendation endpoint returns data
- [ ] Events being collected (check database)
- [ ] Synthetic data properly tagged
- [ ] Airflow DAG deployed
- [ ] Prometheus scraping metrics
- [ ] Grafana dashboards configured
- [ ] Alert rules configured
- [ ] Retraining tested manually
- [ ] Evaluation gates validated
- [ ] Canary deployment tested
- [ ] Rollback procedure documented
- [ ] Data retention policy set

## Next Steps

1. **Run baseline simulation**: Generate initial dataset
2. **Train initial model**: Use feedback data
3. **Set up monitoring**: Configure alerts
4. **Test canary deployment**: Validate workflow
5. **Document runbooks**: For on-call engineers

## Resources

- Architecture: `FEEDBACK_LOOP_ARCHITECTURE.md`
- Database schema: `src/feedback/schema.sql`
- Event collector: `src/feedback/event_collector.py`
- Simulator: `scripts/simulate_user_behavior_v2.py`
- Retraining DAG: `airflow/dags/feedback_loop_dag.py`
- Evaluation gates: `src/feedback/evaluation_gate.py`
