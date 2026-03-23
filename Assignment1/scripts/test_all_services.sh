#!/bin/bash

echo "========================================="
echo "Testing All MLOps Services"
echo "========================================="

NAMESPACE="mlops"
SUCCESS=0
FAILED=0

test_service() {
    local service=$1
    local port=$2
    local endpoint=$3
    local wait_time=${4:-5}
    
    echo ""
    echo "Testing $service..."
    
    # Start port-forward in background
    kubectl port-forward -n $NAMESPACE svc/$service $port:$port > /dev/null 2>&1 &
    PF_PID=$!
    
    # Wait for port-forward
    sleep $wait_time
    
    # Test endpoint
    if curl -s --max-time 5 http://localhost:$port$endpoint > /dev/null 2>&1; then
        echo "✅ $service is healthy"
        ((SUCCESS++))
    else
        echo "❌ $service health check failed"
        ((FAILED++))
    fi
    
    # Cleanup
    kill $PF_PID 2>/dev/null
    sleep 1
}

# Test each service
test_service "mlops-api" "8000" "/health" 3
test_service "mlops-model-v2" "8080" "/health" 3
test_service "mlops-router" "8888" "/health" 3
test_service "mlflow" "5000" "/" 5
test_service "prometheus" "9090" "/-/healthy" 3
test_service "grafana" "3000" "/api/health" 3
test_service "streamlit-ui" "8501" "/_stcore/health" 3
test_service "airflow-web" "8080" "/health" 8

echo ""
echo "========================================="
echo "Test Summary: $SUCCESS passed, $FAILED failed"
echo "========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi
