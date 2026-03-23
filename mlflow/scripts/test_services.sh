#!/bin/bash

# Quick Service Test Script
# Tests all services via port-forward

echo "========================================="
echo "Testing MLOps Services"
echo "========================================="
echo ""

# Start port-forwards in background
echo "Starting port-forwards..."
kubectl port-forward -n mlops svc/mlops-api 8000:8000 > /dev/null 2>&1 &
API_PID=$!
kubectl port-forward -n mlops svc/mlops-model-v2 8080:8080 > /dev/null 2>&1 &
MODEL_PID=$!
kubectl port-forward -n mlops svc/mlops-router 8888:80 > /dev/null 2>&1 &
ROUTER_PID=$!
kubectl port-forward -n mlops svc/mlflow 5000:5000 > /dev/null 2>&1 &
MLFLOW_PID=$!
kubectl port-forward -n mlops svc/prometheus 9090:9090 > /dev/null 2>&1 &
PROM_PID=$!
kubectl port-forward -n mlops svc/grafana 3000:3000 > /dev/null 2>&1 &
GRAFANA_PID=$!
kubectl port-forward -n mlops svc/airflow-web 8081:8080 > /dev/null 2>&1 &
AIRFLOW_PID=$!
kubectl port-forward -n mlops svc/streamlit-ui 8501:8501 > /dev/null 2>&1 &
STREAMLIT_PID=$!

# Store all PIDs for cleanup
PIDS="$API_PID $MODEL_PID $ROUTER_PID $MLFLOW_PID $PROM_PID $GRAFANA_PID $AIRFLOW_PID $STREAMLIT_PID"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up port-forwards..."
    kill $PIDS 2>/dev/null
    pkill -f "kubectl port-forward" 2>/dev/null
    exit 0
}

trap cleanup EXIT INT TERM

echo "Waiting for port-forwards to establish (15 seconds)..."
sleep 15

echo ""
echo "=== Testing Services ==="
echo ""

# Test API Service
echo "1. API Service (8000):"
if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "   ✅ API Service is accessible"
    curl -s http://localhost:8000/health | head -n 2
else
    echo "   ❌ API Service is NOT accessible"
fi
echo ""

# Test Model v2
echo "2. Model v2 Service (8080):"
if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    echo "   ✅ Model v2 is accessible"
    curl -s http://localhost:8080/health | head -n 2
else
    echo "   ❌ Model v2 is NOT accessible"
fi
echo ""

# Test Router
echo "3. Router Service (8888):"
if curl -sf http://localhost:8888/health > /dev/null 2>&1; then
    echo "   ✅ Router is accessible"
    curl -s http://localhost:8888/health | head -n 2
else
    echo "   ❌ Router is NOT accessible"
fi
echo ""

# Test MLflow
echo "4. MLflow (5000):"
if curl -sf http://localhost:5000/health > /dev/null 2>&1; then
    echo "   ✅ MLflow is accessible"
else
    echo "   ❌ MLflow is NOT accessible"
fi
echo ""

# Test Prometheus
echo "5. Prometheus (9090):"
if curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1; then
    echo "   ✅ Prometheus is accessible"
else
    echo "   ❌ Prometheus is NOT accessible"
fi
echo ""

# Test Grafana
echo "6. Grafana (3000):"
if curl -sf http://localhost:3000/api/health > /dev/null 2>&1; then
    echo "   ✅ Grafana is accessible (admin/admin123)"
else
    echo "   ❌ Grafana is NOT accessible"
fi
echo ""

# Test Airflow
echo "7. Airflow UI (8081):"
if curl -sf http://localhost:8081/health > /dev/null 2>&1; then
    echo "   ✅ Airflow is accessible (admin/admin123)"
else
    echo "   ⚠️  Airflow may still be initializing..."
fi
echo ""

# Test Streamlit
echo "8. Streamlit UI (8501):"
if curl -sf http://localhost:8501/_stcore/health > /dev/null 2>&1; then
    echo "   ✅ Streamlit is accessible"
else
    echo "   ❌ Streamlit is NOT accessible"
fi
echo ""

echo "========================================="
echo "Service URLs:"
echo "========================================="
echo "  MLflow:       http://localhost:5000"
echo "  Prometheus:   http://localhost:9090"
echo "  Grafana:      http://localhost:3000 (admin/admin123)"
echo "  Airflow:      http://localhost:8081 (admin/admin123)"
echo "  API:          http://localhost:8000"
echo "  Model v2:     http://localhost:8080"
echo "  Router:       http://localhost:8888"
echo "  Streamlit:    http://localhost:8501"
echo "========================================="
echo ""
echo "Press Ctrl+C to stop port-forwards"

# Keep running
while true; do
    sleep 10
done
