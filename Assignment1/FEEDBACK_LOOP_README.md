# ML Feedback Loop - Quick Reference

## What is this?

A production-ready ML feedback loop that continuously improves your movie recommendation model using real user behavior data.

## 🎯 Key Features

✅ **Event Collection** - Captures impressions, clicks, watches, and feedback  
✅ **Synthetic Data Simulation** - Realistic user behavior for testing  
✅ **Automatic Retraining** - Triggers based on data volume or performance  
✅ **Evaluation Gates** - Prevents bad models from deploying  
✅ **Canary Deployments** - Gradual rollout with A/B testing  
✅ **Monitoring** - Real-time metrics and alerts  
✅ **Local Deployment** - No AWS/cloud dependencies  

## 🚀 Quick Start (5 minutes)

```bash
# 1. Initialize everything
./scripts/setup_feedback_loop.sh

# 2. Generate synthetic user behavior
python3 scripts/simulate_user_behavior_v2.py --users 100 --sessions 1000

# 3. Open UIs
open http://localhost:8501  # Streamlit
open http://localhost:8002  # Event Collector
open http://localhost:8080  # Airflow (admin/admin123)
```

## 📊 Architecture

```
User → /recommend → Recommendation → Events → Postgres → Labels → 
  Model Training → Evaluation → Canary → Production
```

## 🔧 Components

| Service | Port | Purpose |
|---------|------|---------|
| API Service | 8000 | Serves recommendations |
| Event Collector | 8002 | Collects user events |
| MLflow | 5000 | Model registry |
| Airflow | 8080 | Orchestration |
| Grafana | 3000 | Monitoring |
| Prometheus | 9091 | Metrics |

## 📝 Event Schema

### Impression (Recommendation shown)
```json
{
  "recommendation_id": "uuid",
  "user_id": 123,
  "item_ids": [1, 2, 3],
  "model_version": "nmf_v1",
  "is_synthetic": false
}
```

### Click (User clicked item)
```json
{
  "user_id": 123,
  "item_id": 2,
  "position": 1,
  "recommendation_id": "uuid",
  "is_synthetic": false
}
```

### Watch (User watched content)
```json
{
  "user_id": 123,
  "item_id": 2,
  "watch_duration_seconds": 3600,
  "total_duration_seconds": 7200,
  "is_synthetic": false
}
```

### Feedback (Like/dislike)
```json
{
  "user_id": 123,
  "item_id": 2,
  "feedback_type": "like",
  "rating": 4.5,
  "is_synthetic": false
}
```

## 🎬 Usage Examples

### Get Recommendations
```bash
curl "http://localhost:8000/recommend?user_id=123&top_k=10"
```

### Log Click Event
```bash
curl -X POST http://localhost:8002/events/click \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 123,
    "item_id": 456,
    "position": 0,
    "is_synthetic": false
  }'
```

### Check Event Stats
```bash
curl http://localhost:8002/stats | jq
```

### Trigger Retraining
```bash
docker exec mlops-airflow-webserver \
  airflow dags trigger feedback_loop_retraining
```

## 📈 Monitoring Queries

### Click-Through Rate
```sql
SELECT 
    DATE(created_at) as date,
    ROUND(COUNT(DISTINCT CASE WHEN event_type='click' THEN recommendation_id END)::NUMERIC /
          COUNT(DISTINCT CASE WHEN event_type='impression' THEN recommendation_id END) * 100, 2) as ctr
FROM user_events
WHERE created_at >= CURRENT_DATE - 7
GROUP BY DATE(created_at);
```

### Watch Rate
```sql
SELECT 
    ROUND(COUNT(DISTINCT CASE WHEN event_type='watch' THEN event_id END)::NUMERIC /
          COUNT(DISTINCT CASE WHEN event_type='click' THEN event_id END) * 100, 2) as watch_rate
FROM user_events
WHERE created_at >= CURRENT_DATE - 7;
```

## 🔄 Retraining Workflow

1. **Trigger** - Automatic based on:
   - ≥ 10k new events
   - CTR drops > 10%
   - Weekly schedule
   - Manual trigger

2. **Label Generation** - Converts events to training labels
   ```
   Label = click(1.0) + watch_completion(0-3.0) + feedback(±2.0)
   ```

3. **Training** - Trains new NMF model

4. **Evaluation** - Must pass gates:
   - RMSE < baseline + 10%
   - MAE < baseline + 10%
   - CTR > baseline * 95%
   - Latency < 200ms (P95)

5. **Canary** - 10% traffic for 24h

6. **Promote or Rollback**

## 🛡️ Evaluation Gates

The system prevents bad model deployments:

```python
# All gates must pass
✓ Performance: RMSE/MAE not significantly worse
✓ Business: CTR/watch rate maintained
✓ System: Latency and error rate acceptable
✓ Statistical: Significant improvement (p < 0.05)
```

## 🔧 Troubleshooting

### No events being collected
```bash
# Check event collector
curl http://localhost:8002/health

# Check database
docker exec mlops-postgres psql -U mlops -d mlops_db \
  -c "SELECT COUNT(*) FROM user_events;"
```

### Retraining not triggering
```bash
# Check conditions
docker exec mlops-postgres psql -U mlops -d mlops_db -c \
  "SELECT COUNT(*) FROM user_events 
   WHERE created_at >= CURRENT_DATE - 7;"

# Manual trigger
docker exec mlops-airflow-webserver \
  airflow dags trigger feedback_loop_retraining
```

### Check logs
```bash
docker logs mlops-event-collector --tail 100
docker logs mlops-airflow-scheduler --tail 100
```

## 📚 Documentation

- **Architecture**: `FEEDBACK_LOOP_ARCHITECTURE.md`
- **Deployment**: `FEEDBACK_LOOP_DEPLOYMENT.md`
- **Database Schema**: `src/feedback/schema.sql`
- **Event Collector**: `src/feedback/event_collector.py`
- **Simulator**: `scripts/simulate_user_behavior_v2.py`
- **Retraining DAG**: `airflow/dags/feedback_loop_dag.py`
- **Evaluation Gates**: `src/feedback/evaluation_gate.py`

## 🎯 Production Checklist

- [ ] Run `./scripts/setup_feedback_loop.sh`
- [ ] Verify all services healthy
- [ ] Generate initial synthetic data
- [ ] Check events in database
- [ ] Configure Grafana dashboards
- [ ] Set up alert rules
- [ ] Test retraining manually
- [ ] Validate evaluation gates
- [ ] Test rollback procedure
- [ ] Document runbooks

## 💡 Best Practices

### Synthetic vs Real Data

- ✅ **Tag everything** with `is_synthetic` flag
- ✅ **Train on both** for more data
- ✅ **Evaluate on real only** for accurate metrics
- ✅ **Monitor ratio** (keep synthetic < 80%)

### Retraining Frequency

- **Daily labels**: Generate training labels daily
- **Weekly training**: Retrain weekly (or when triggered)
- **Continuous monitoring**: Track CTR, errors, latency
- **Fast rollback**: Keep previous model ready

### Data Quality

- **Validate events** before storage
- **Archive old data** (> 90 days)
- **Monitor drift** in user behavior
- **Sample synthetics** if ratio too high

## 🚨 Alerts

Recommended alert thresholds:

- CTR < 15% for 15min → Warning
- Error rate > 1% for 5min → Critical
- P95 latency > 200ms for 10min → Warning
- Database connection failures → Critical

## 🌟 What Makes This Production-Ready?

1. **Complete event tracking** - All user actions logged
2. **Automatic triggers** - Retrains when needed
3. **Safety checks** - Multiple evaluation gates
4. **Gradual rollout** - Canary deployments
5. **Monitoring** - Real-time metrics and alerts
6. **Easy rollback** - One command to revert
7. **Separates synthetic data** - Clear testing isolation
8. **Database-backed** - Scalable event storage
9. **Documented** - Full runbooks and guides
10. **Tested** - Includes realistic simulator

## 📞 Support

For questions or issues:
1. Check `TROUBLESHOOTING.md`
2. Review logs: `docker logs <service-name>`
3. Query database for event stats
4. Check Grafana dashboards

---

**Built for production. Ready for scale. Simple to operate.**
