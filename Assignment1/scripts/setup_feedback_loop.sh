#!/bin/bash

# ============================================================
# Feedback Loop Quickstart Script
# ============================================================
# Initializes the complete ML feedback loop system
# ============================================================

set -e  # Exit on error

echo "============================================================"
echo "ML FEEDBACK LOOP - QUICKSTART"
echo "============================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "→ $1"
}

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

print_success "Docker is running"

# Step 1: Build and start services
echo ""
echo "Step 1: Building and starting services..."
echo "------------------------------------------------------------"

docker compose down 2>/dev/null || true

print_info "Building images (this may take a few minutes)..."
docker compose build api-service event-collector streamlit-ui >/dev/null 2>&1 &
BUILD_PID=$!

# Show spinner while building
spin='-\|/'
i=0
while kill -0 $BUILD_PID 2>/dev/null; do
    i=$(( (i+1) %4 ))
    printf "\r${spin:$i:1} Building..."
    sleep .1
done
wait $BUILD_PID
echo ""

print_success "Images built"

print_info "Starting services..."
docker compose up -d

# Wait for services to be healthy
print_info "Waiting for services to be healthy..."
sleep 15

# Check service health
check_service() {
    local service=$1
    local url=$2
    local name=$3
    
    if curl -f -s "$url" > /dev/null; then
        print_success "$name is healthy"
        return 0
    else
        print_warning "$name is not responding (this is okay if optional)"
        return 1
    fi
}

check_service "postgres" "http://localhost:5432" "PostgreSQL" || true
check_service "mlflow" "http://localhost:5000/health" "MLflow"
check_service "api" "http://localhost:8000/health" "API Service"
check_service "collector" "http://localhost:8002/health" "Event Collector"
check_service "streamlit" "http://localhost:8501/_stcore/health" "Streamlit UI"

# Step 2: Initialize database schema
echo ""
echo "Step 2: Initializing database schema..."
echo "------------------------------------------------------------"

print_info "Creating feedback loop tables..."
docker exec -i mlops-postgres psql -U mlops -d mlops_db < src/feedback/schema.sql 2>&1 | grep -v "NOTICE" || true

# Verify tables
TABLES=$(docker exec mlops-postgres psql -U mlops -d mlops_db -t -c "\dt" 2>/dev/null | grep -c "table" || echo "0")

if [ "$TABLES" -ge 6 ]; then
    print_success "Database schema initialized ($TABLES tables)"
else
    print_warning "Some tables may not have been created. Check logs."
fi

# Step 3: Test the system
echo ""
echo "Step 3: Testing the system..."
echo "------------------------------------------------------------"

# Test recommendation endpoint
print_info "Testing recommendation endpoint..."
RECOMMENDATION=$(curl -s "http://localhost:8000/recommend?user_id=123&top_k=5")

if echo "$RECOMMENDATION" | grep -q "recommendation_id"; then
    print_success "Recommendation endpoint working"
    REC_ID=$(echo "$RECOMMENDATION" | python3 -c "import sys, json; print(json.load(sys.stdin)['recommendation_id'])" 2>/dev/null || echo "")
else
    print_error "Recommendation endpoint not working"
    REC_ID=""
fi

# Test event collection
if [ -n "$REC_ID" ]; then
    print_info "Testing event collection..."
    
    EVENT_RESPONSE=$(curl -s -X POST http://localhost:8002/events/click \
        -H "Content-Type: application/json" \
        -d "{
            \"user_id\": 123,
            \"item_id\": 456,
            \"position\": 0,
            \"recommendation_id\": \"$REC_ID\",
            \"is_synthetic\": true
        }")
    
    if echo "$EVENT_RESPONSE" | grep -q "success"; then
        print_success "Event collection working"
    else
        print_warning "Event collection may have issues"
    fi
fi

# Check events in database
EVENT_COUNT=$(docker exec mlops-postgres psql -U mlops -d mlops_db -t -c "SELECT COUNT(*) FROM user_events;" 2>/dev/null | tr -d ' ' || echo "0")
print_info "Events in database: $EVENT_COUNT"

# Step 4: Run small simulation
echo ""
echo "Step 4: Running small simulation..."
echo "------------------------------------------------------------"

print_info "Simulating 50 user sessions..."

# Check if numpy is installed
if python3 -c "import numpy" 2>/dev/null; then
    python3 scripts/simulate_user_behavior_v2.py \
        --users 25 \
        --sessions 50 \
        --delay 50 2>&1 | tail -20
    
    print_success "Simulation completed"
else
    print_warning "NumPy not installed. Skipping simulation."
    print_info "Install with: pip3 install numpy requests"
fi

# Final status
echo ""
echo "============================================================"
echo "SETUP COMPLETE!"
echo "============================================================"
echo ""
echo "Services available at:"
echo "  • Streamlit UI:       http://localhost:8501"
echo "  • API Service:        http://localhost:8000"
echo "  • Event Collector:    http://localhost:8002"
echo "  • MLflow:             http://localhost:5000"
echo "  • Airflow:            http://localhost:8080 (admin/admin123)"
echo "  • Grafana:            http://localhost:3000 (admin/admin123)"
echo "  • Prometheus:         http://localhost:9091"
echo ""
echo "Next steps:"
echo "  1. Open Streamlit UI: http://localhost:8501"
echo "  2. Run full simulation:"
echo "     python3 scripts/simulate_user_behavior_v2.py --users 100 --sessions 1000"
echo "  3. Trigger retraining via Airflow UI"
echo "  4. Monitor at http://localhost:3000"
echo ""
echo "Documentation:"
echo "  • Architecture:  FEEDBACK_LOOP_ARCHITECTURE.md"
echo "  • Deployment:    FEEDBACK_LOOP_DEPLOYMENT.md"
echo ""
echo "============================================================"

# Check database for final stats
FINAL_EVENTS=$(docker exec mlops-postgres psql -U mlops -d mlops_db -t -c "SELECT COUNT(*) FROM user_events;" 2>/dev/null | tr -d ' ' || echo "0")

if [ "$FINAL_EVENTS" -gt 0 ]; then
    echo ""
    echo "Event Statistics:"
    docker exec mlops-postgres psql -U mlops -d mlops_db -c \
        "SELECT event_type, COUNT(*) as count, 
         SUM(CASE WHEN is_synthetic THEN 1 ELSE 0 END) as synthetic_count
         FROM user_events 
         GROUP BY event_type 
         ORDER BY COUNT(*) DESC;"
fi

echo ""
print_success "Feedback loop is ready!"
