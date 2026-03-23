#!/bin/bash
# ============================================================
# ML Feedback Loop - Interactive Verification Script
# ============================================================

set -e

BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     ML FEEDBACK LOOP - USE CASE VERIFIER                     ║"
echo "╔══════════════════════════════════════════════════════════════╗"
echo ""

# ============================================================
# Function to run test and show result
# ============================================================
run_check() {
    local name="$1"
    local command="$2"
    
    echo -e "${BLUE}▶ Testing: ${name}${NC}"
    
    if eval "$command" > /tmp/verify_output.txt 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC}"
        if [ -s /tmp/verify_output.txt ]; then
            head -10 /tmp/verify_output.txt
        fi
    else
        echo -e "${RED}✗ FAILED${NC}"
        cat /tmp/verify_output.txt
    fi
    echo ""
}

# ============================================================
# 1. SERVICES HEALTH
# ============================================================
echo -e "${YELLOW}═══ 1. SERVICES HEALTH ═══${NC}"
echo ""

run_check "API Service" "curl -sf http://localhost:8000/health"
run_check "Event Collector" "curl -sf http://localhost:8002/health"
run_check "MLflow" "curl -sf http://localhost:5000/health"
run_check "Airflow" "curl -sf http://localhost:8080/health"

# ============================================================
# 2. DATA COLLECTION
# ============================================================
echo -e "${YELLOW}═══ 2. DATA COLLECTION ═══${NC}"
echo ""

run_check "Event Counts" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"SELECT event_type, COUNT(*) FROM user_events GROUP BY event_type;\""

run_check "Recommendation Logs" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"SELECT COUNT(*) as total_recommendations FROM recommendation_logs;\""

# ============================================================
# 3. ENGAGEMENT METRICS
# ============================================================
echo -e "${YELLOW}═══ 3. ENGAGEMENT METRICS ═══${NC}"
echo ""

run_check "Click-Through Rate" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"
WITH metrics AS (
  SELECT 
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN recommendation_id END)::float as clicks,
    COUNT(DISTINCT recommendation_id)::float as impressions
  FROM user_events WHERE recommendation_id IS NOT NULL
)
SELECT 
  'CTR: ' || ROUND((100.0 * clicks / NULLIF(impressions, 0))::numeric, 2) || '%' as metric
FROM metrics;\""

run_check "Watch Rate" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"
WITH metrics AS (
  SELECT 
    COUNT(DISTINCT CASE WHEN event_type = 'watch' THEN recommendation_id END)::float as watches,
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN recommendation_id END)::float as clicks
  FROM user_events WHERE recommendation_id IS NOT NULL
)
SELECT 
  'Watch Rate: ' || ROUND((100.0 * watches / NULLIF(clicks, 0))::numeric, 2) || '%' as metric
FROM metrics;\""

# ============================================================
# 4. TRAINING LABELS
# ============================================================
echo -e "${YELLOW}═══ 4. TRAINING LABELS ═══${NC}"
echo ""

run_check "Label Statistics" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"
SELECT 
  'Total: ' || COUNT(*) || ' | Avg: ' || ROUND(AVG(label)::numeric, 2) || 
  ' | Min: ' || MIN(label) || ' | Max: ' || MAX(label) as stats
FROM training_labels;\""

run_check "Label Distribution" "docker exec mlops-postgres psql -U mlops -d mlops_db -c \"
SELECT 
  ROUND(label::numeric, 1) as score,
  COUNT(*) as count,
  LPAD('█', (COUNT(*) * 50 / MAX(COUNT(*)) OVER())::int, '█') as bar
FROM training_labels 
GROUP BY ROUND(label::numeric, 1)
ORDER BY score DESC;\""

# ============================================================
# 5. MODEL VERSIONS
# ============================================================
echo -e "${YELLOW}═══ 5. MODEL VERSIONS ═══${NC}"
echo ""

run_check "Active Models" "docker exec mlops-postgres psql -U mlops -d mlops_db -c \"
SELECT 
  model_version,
  COUNT(*) as recommendation_count
FROM recommendation_logs 
GROUP BY model_version
ORDER BY recommendation_count DESC;\""

# ============================================================
# 6. RETRAINING STATUS
# ============================================================
echo -e "${YELLOW}═══ 6. RETRAINING STATUS ═══${NC}"
echo ""

run_check "Airflow DAG Status" "curl -s -u 'admin:admin123' http://localhost:8080/api/v1/dags/feedback_loop_retraining | python3 -c \"import sys, json; d=json.load(sys.stdin); print(f'Active: {d[\\\"is_active\\\"]} | Paused: {d[\\\"is_paused\\\"]}')\""

run_check "Recent DAG Runs" "curl -s -u 'admin:admin123' http://localhost:8080/api/v1/dags/feedback_loop_retraining/dagRuns?limit=3 | python3 -c \"import sys, json; runs=json.load(sys.stdin).get('dag_runs', []); [print(f'{r[\\\"execution_date\\\"]}: {r[\\\"state\\\"]}') for r in runs]\""

# ============================================================
# 7. DATA QUALITY
# ============================================================
echo -e "${YELLOW}═══ 7. DATA QUALITY ═══${NC}"
echo ""

run_check "Orphaned Events Check" "docker exec mlops-postgres psql -U mlops -d mlops_db -t -c \"
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN '✓ No orphaned events'
    ELSE '⚠ Found ' || COUNT(*) || ' orphaned events'
  END as status
FROM user_events e
LEFT JOIN recommendation_logs r ON e.recommendation_id = r.recommendation_id
WHERE e.recommendation_id IS NOT NULL AND r.recommendation_id IS NULL;\""

run_check "Synthetic Data Flag" "docker exec mlops-postgres psql -U mlops -d mlops_db -c \"
SELECT 
  is_synthetic,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM user_events
GROUP BY is_synthetic;\""

# ============================================================
# 8. RECENT ACTIVITY
# ============================================================
echo -e "${YELLOW}═══ 8. RECENT ACTIVITY (Last Hour) ═══${NC}"
echo ""

run_check "Recent Events" "docker exec mlops-postgres psql -U mlops -d mlops_db -c \"
SELECT 
  event_type,
  COUNT(*) as last_hour_count
FROM user_events 
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY event_type;\""

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo -e "║ ${GREEN}VERIFICATION COMPLETE${NC}                                        ║"
echo "╔══════════════════════════════════════════════════════════════╗"
echo ""

# Generate summary
docker exec mlops-postgres psql -U mlops -d mlops_db -t -c "
SELECT 
  '✓ ' || COUNT(*) || ' recommendations logged' as summary
FROM recommendation_logs
UNION ALL
SELECT '✓ ' || COUNT(*) || ' user events collected' FROM user_events
UNION ALL
SELECT '✓ ' || COUNT(*) || ' training labels generated' FROM training_labels;
"

echo ""
echo "To run specific checks, see: FEEDBACK_LOOP_README.md"
echo ""
