# ML Feedback Loop Architecture

## Overview
Production-ready ML feedback loop for continuous model improvement using real and synthetic user behavior.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FEEDBACK LOOP PIPELINE                        │
└─────────────────────────────────────────────────────────────────────┘

1. USER INTERACTION
   ┌──────────────┐
   │ User Request │──► GET /recommend?user_id=123
   └──────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Recommendation Service (API)     │
   │  - Load user history              │
   │  - Generate recommendations       │
   │  - Log impression event           │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Return: [movie_ids, scores]      │
   │  + recommendation_id (tracking)   │
   └──────────────────────────────────┘

2. EVENT COLLECTION
   ┌──────────────────────────────────┐
   │  User Behavior (Real/Synthetic)   │
   │  - Clicks item                    │
   │  - Watches (duration)             │
   │  - Likes/Dislikes                 │
   │  - Skips                          │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Event Collector API              │
   │  POST /events                     │
   │  - Validates schema               │
   │  - Adds timestamps                │
   │  - Tags synthetic data            │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Postgres Event Store             │
   │  Tables:                          │
   │  - user_events                    │
   │  - recommendation_logs            │
   │  - model_performance              │
   └──────────────────────────────────┘

3. LABEL GENERATION (Daily)
   ┌──────────────────────────────────┐
   │  Airflow DAG: build_training_data │
   │  - Join impressions + feedback    │
   │  - Calculate engagement scores    │
   │  - Generate labels (0-5)          │
   │  - Create train/val split         │
   │  - Separate synthetic flag        │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  data/feedback/                   │
   │  - training_YYYYMMDD.parquet      │
   │  - validation_YYYYMMDD.parquet    │
   └──────────────────────────────────┘

4. RETRAINING TRIGGER (Weekly)
   ┌──────────────────────────────────┐
   │  Airflow DAG: retrain_model       │
   │  Triggers if:                     │
   │  - >= 10k new interactions        │
   │  - Performance drops > 5%         │
   │  - Drift detected                 │
   │  - Manual trigger                 │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Model Training                   │
   │  - Load feedback data             │
   │  - Train NMF model                │
   │  - Log to MLflow                  │
   │  - Create candidate model         │
   └──────────────────────────────────┘

5. EVALUATION GATE
   ┌──────────────────────────────────┐
   │  Model Validation                 │
   │  ✓ RMSE < current + 10%           │
   │  ✓ MAE < current + 10%            │
   │  ✓ CTR > baseline                 │
   │  ✓ Watch rate > baseline          │
   │  ✓ No NaN predictions             │
   │  ✓ Latency < 200ms (p95)          │
   └──────────────────────────────────┘
           │
           ├─ PASS ──┐
           │         ▼
           │    ┌──────────────────────┐
           │    │ Promote to Staging   │
           │    └──────────────────────┘
           │         │
           │         ▼
           │    ┌──────────────────────┐
           │    │ A/B Test (Canary)    │
           │    │ 10% traffic → new    │
           │    │ 90% traffic → old    │
           │    └──────────────────────┘
           │         │
           │         ▼
           │    ┌──────────────────────┐
           │    │ Monitor for 24h      │
           │    └──────────────────────┘
           │         │
           │         ├─ SUCCESS ──► Promote to Production
           │         └─ FAIL ─────► Rollback
           │
           └─ FAIL ──► Alert + Block Deployment

6. MONITORING
   ┌──────────────────────────────────┐
   │  Real-time Metrics (Prometheus)   │
   │  - CTR (click-through rate)       │
   │  - Watch rate (completion)        │
   │  - Avg watch time                 │
   │  - Prediction latency             │
   │  - Error rate                     │
   └──────────────────────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │  Grafana Dashboards               │
   │  - Model performance              │
   │  - Data drift alerts              │
   │  - User engagement trends         │
   └──────────────────────────────────┘
```

## Data Flow

### Event Types & Schemas

```json
{
  "impression": {
    "event_type": "impression",
    "recommendation_id": "uuid",
    "user_id": 123,
    "item_ids": [1, 2, 3, 4, 5],
    "model_version": "nmf_v1.2",
    "timestamp": "2026-02-23T10:30:00Z",
    "is_synthetic": false
  },
  
  "click": {
    "event_type": "click",
    "recommendation_id": "uuid",
    "user_id": 123,
    "item_id": 2,
    "position": 1,
    "timestamp": "2026-02-23T10:30:05Z",
    "is_synthetic": false
  },
  
  "watch": {
    "event_type": "watch",
    "user_id": 123,
    "item_id": 2,
    "watch_duration_seconds": 3600,
    "total_duration_seconds": 7200,
    "completion_rate": 0.5,
    "timestamp": "2026-02-23T11:30:00Z",
    "is_synthetic": false
  },
  
  "feedback": {
    "event_type": "feedback",
    "user_id": 123,
    "item_id": 2,
    "feedback_type": "like",  // like, dislike, not_interested
    "rating": 4.5,  // optional explicit rating
    "timestamp": "2026-02-23T11:35:00Z",
    "is_synthetic": false
  }
}
```

## Label Generation Logic

```python
# Compute engagement score from events
def generate_label(events):
    score = 0.0
    
    # Impression only: 0
    # Click: +1.0
    # Watch > 25%: +1.0
    # Watch > 75%: +1.5
    # Like: +2.0
    # Not interested: -2.0
    
    if has_click:
        score += 1.0
    if completion_rate > 0.75:
        score += 2.5
    elif completion_rate > 0.25:
        score += 1.0
    if feedback == "like":
        score += 2.0
    elif feedback == "not_interested":
        score = max(0, score - 2.0)
    
    # Normalize to 0-5 scale
    return min(5.0, max(0.0, score))
```

## Separating Synthetic vs Real Data

1. **Tag at Collection**: `is_synthetic` flag in every event
2. **Separate Storage**: Optional separate tables for isolation
3. **Training Strategy**:
   - Train on BOTH synthetic + real (more data)
   - Evaluate ONLY on real data
   - Monitor distribution shift
4. **Sampling**: Downsample synthetic if ratio > 80%

## Stack Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| API Service | FastAPI | Recommendations + predictions |
| Event Collector | FastAPI | Event ingestion |
| Database | PostgreSQL | Event storage |
| Model Registry | MLflow | Model versioning |
| Orchestration | Airflow | Scheduled pipelines |
| Monitoring | Prometheus + Grafana | Metrics & alerts |
| Simulation | Python script | Synthetic behavior |

## Retraining Schedule

**Trigger Conditions** (OR logic):
- ✅ Every 7 days (cron: `0 2 * * 0`)
- ✅ > 10,000 new events since last train
- ✅ CTR drops > 10% (7-day avg)
- ✅ Model RMSE increases > 5%
- ✅ Manual trigger via Airflow UI

## Evaluation Gates (Prevent Bad Models)

```python
class ModelGate:
    def validate(self, candidate_model, current_model, val_data):
        checks = {
            'rmse': candidate.rmse < current.rmse * 1.1,
            'mae': candidate.mae < current.mae * 1.1,
            'no_errors': all(~np.isnan(candidate.predict(val_data))),
            'latency': p95_latency < 200,  # ms
            'ctr': candidate_ctr > baseline_ctr * 0.95,
        }
        return all(checks.values()), checks
```

## Canary Deployment Flow

1. **Staging**: Promote candidate to "Staging" stage in MLflow
2. **Canary**: Route 10% traffic to new model
3. **Monitoring**: Track for 24 hours
   - If CTR stable + no errors → promote
   - If degradation → auto-rollback
4. **Production**: Promote to "Production" stage
5. **Archive**: Archive old model

## Monitoring Metrics

### Business Metrics
- **CTR (Click-Through Rate)**: clicks / impressions
- **Watch Rate**: watches / clicks
- **Engagement Score**: avg(watch_completion * 5)
- **User Return Rate**: daily active users

### Model Metrics
- **RMSE**: Root mean squared error
- **MAE**: Mean absolute error
- **Coverage**: % users getting recommendations
- **Diversity**: Unique items in top-100

### System Metrics
- **Latency**: p50, p95, p99 response time
- **Error Rate**: 5xx errors / total requests
- **Throughput**: requests per second
- **Data Freshness**: hours since last event

## Implementation Checklist

- [ ] Event collector API
- [ ] Database schema (events, recommendations, feedback)
- [ ] User behavior simulator
- [ ] Label generation DAG
- [ ] Retraining DAG
- [ ] Evaluation gate logic
- [ ] Monitoring dashboards
- [ ] Alerting rules
- [ ] Rollback procedure
- [ ] Documentation

## Next Steps

See implementation files:
- `src/feedback/event_collector.py` - Event API
- `src/feedback/schema.sql` - Database schema  
- `scripts/simulate_user_behavior_v2.py` - Behavior simulator
- `airflow/dags/feedback_loop_dag.py` - Retraining pipeline
- `src/feedback/evaluation_gate.py` - Model validation
