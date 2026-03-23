#!/bin/bash

# Comprehensive MLOps Services Test Script
# Tests all services including Airflow UI accessibility

set -e

echo "=========================================="
echo "MLOps Platform - Complete Service Test"
echo "=========================================="
echo ""

# Get the external IP
EXTERNAL_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null || echo "")

if [ -z "$EXTERNAL_IP" ]; then
    echo "⚠️  Warning: Could not detect external IP"
    EXTERNAL_IP="<NODE_EXTERNAL_IP>"
fi

echo "📍 Cluster External IP: $EXTERNAL_IP"
echo ""

# Function to test endpoint
test_endpoint() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}
    
    echo -n "Testing $name... "
    
    response=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$url" 2>/dev/null || echo "000")
    
    if [ "$response" -eq "$expected_code" ] || [ "$response" -eq 200 ]; then
        echo "✅ OK (HTTP $response)"
        return 0
    else
        echo "❌ FAILED (HTTP $response)"
        return 1
    fi
}

# Function to test JSON endpoint
test_json_endpoint() {
    local name=$1
    local url=$2
    
    echo -n "Testing $name... "
    
    response=$(curl -s --max-time 10 "$url" 2>/dev/null || echo "")
    
    if echo "$response" | grep -q "{"; then
        echo "✅ OK (JSON response)"
        return 0
    else
        echo "❌ FAILED (No valid JSON)"
        return 1
    fi
}

echo "=========================================="
echo "1. Testing Kubernetes Cluster"
echo "=========================================="
echo ""

# Check cluster connectivity
echo -n "Checking cluster access... "
if kubectl cluster-info &>/dev/null; then
    echo "✅ OK"
else
    echo "❌ FAILED - Cannot connect to cluster"
    exit 1
fi

# Check namespace
echo -n "Checking mlops namespace... "
if kubectl get namespace mlops &>/dev/null; then
    echo "✅ OK"
else
    echo "❌ FAILED - Namespace not found"
    exit 1
fi

# Check all pods
echo ""
echo "Pod Status:"
kubectl get pods -n mlops --no-headers | while read line; do
    name=$(echo $line | awk '{print $1}')
    status=$(echo $line | awk '{print $3}')
    ready=$(echo $line | awk '{print $2}')
    
    if [ "$status" = "Running" ] && [[ "$ready" == "1/1" ]]; then
        echo "  ✅ $name: $status ($ready)"
    else
        echo "  ⚠️  $name: $status ($ready)"
    fi
done

echo ""
echo "=========================================="
echo "2. Testing Internal Services (via kubectl)"
echo "=========================================="
echo ""

# Test MLflow
test_json_endpoint "MLflow Health" "http://$(kubectl get svc mlflow -n mlops -o jsonpath='{.spec.clusterIP}'):5000/health"

# Test Prometheus
test_endpoint "Prometheus" "http://$(kubectl get svc prometheus -n mlops -o jsonpath='{.spec.clusterIP}'):9090/-/healthy"

# Test Grafana
test_endpoint "Grafana" "http://$(kubectl get svc grafana -n mlops -o jsonpath='{.spec.clusterIP}'):3000/api/health"

# Test API Service
test_json_endpoint "API Service Health" "http://$(kubectl get svc mlops-api -n mlops -o jsonpath='{.spec.clusterIP}'):8000/health"

# Test Model v2 Service
test_json_endpoint "Model v2 Health" "http://$(kubectl get svc mlops-model-v2 -n mlops -o jsonpath='{.spec.clusterIP}'):8080/health"

# Test Router Service
test_json_endpoint "Router Health" "http://$(kubectl get svc mlops-router -n mlops -o jsonpath='{.spec.clusterIP}'):80/health"

# Test Airflow
test_json_endpoint "Airflow Health" "http://$(kubectl get svc airflow-web -n mlops -o jsonpath='{.spec.clusterIP}'):8080/health"

echo ""
echo "=========================================="
echo "3. Testing External Access (NodePort)"
echo "=========================================="
echo ""

if [ "$EXTERNAL_IP" != "<NODE_EXTERNAL_IP>" ]; then
    # Test Airflow via NodePort
    AIRFLOW_PORT=$(kubectl get svc airflow-web -n mlops -o jsonpath='{.spec.ports[0].nodePort}')
    test_json_endpoint "Airflow UI (NodePort)" "http://$EXTERNAL_IP:$AIRFLOW_PORT/health"
    echo "   📱 Access: http://$EXTERNAL_IP:$AIRFLOW_PORT"
    echo "   👤 Credentials: admin / admin123"
    
    # Test Router via NodePort
    ROUTER_PORT=$(kubectl get svc mlops-router -n mlops -o jsonpath='{.spec.ports[0].nodePort}')
    test_json_endpoint "Router (NodePort)" "http://$EXTERNAL_IP:$ROUTER_PORT/health"
    echo "   📱 Access: http://$EXTERNAL_IP:$ROUTER_PORT"
    
    # Test Streamlit via NodePort
    STREAMLIT_PORT=$(kubectl get svc streamlit-ui -n mlops -o jsonpath='{.spec.ports[0].nodePort}')
    test_endpoint "Streamlit UI (NodePort)" "http://$EXTERNAL_IP:$STREAMLIT_PORT/"
    echo "   📱 Access: http://$EXTERNAL_IP:$STREAMLIT_PORT"
else
    echo "⚠️  Skipping external tests - no external IP detected"
fi

echo ""
echo "=========================================="
echo "4. Testing Prediction Endpoints"
echo "=========================================="
echo ""

# Test prediction via internal service
echo -n "Testing API prediction... "
API_IP=$(kubectl get svc mlops-api -n mlops -o jsonpath='{.spec.clusterIP}')
pred_response=$(curl -s -X POST "http://$API_IP:8000/predict" \
    -H "Content-Type: application/json" \
    -d '{"user_id": 123, "movie_id": 456}' 2>/dev/null || echo "")

if echo "$pred_response" | grep -q "prediction"; then
    echo "✅ OK"
    echo "   Response: $(echo $pred_response | head -c 100)..."
else
    echo "❌ FAILED"
fi

# Test prediction via router
echo -n "Testing Router prediction... "
ROUTER_IP=$(kubectl get svc mlops-router -n mlops -o jsonpath='{.spec.clusterIP}')
router_response=$(curl -s -X POST "http://$ROUTER_IP:80/predict" \
    -H "Content-Type: application/json" \
    -d '{"user_id": 123, "movie_id": 456}' 2>/dev/null || echo "")

if echo "$router_response" | grep -q "prediction"; then
    echo "✅ OK"
    echo "   Response: $(echo $router_response | head -c 100)..."
else
    echo "❌ FAILED"
fi

echo ""
echo "=========================================="
echo "5. Airflow Configuration Check"
echo "=========================================="
echo ""

echo -n "Checking Airflow database connection... "
AIRFLOW_POD=$(kubectl get pods -n mlops -l app=airflow-web -o jsonpath='{.items[0].metadata.name}')
if kubectl exec -n mlops $AIRFLOW_POD -- airflow db check &>/dev/null; then
    echo "✅ OK"
else
    echo "⚠️  Database check failed (may be normal)"
fi

echo -n "Checking Airflow scheduler status... "
SCHEDULER_POD=$(kubectl get pods -n mlops -l app=airflow-scheduler -o jsonpath='{.items[0].metadata.name}')
if kubectl get pods -n mlops $SCHEDULER_POD | grep -q "Running"; then
    echo "✅ OK"
else
    echo "❌ FAILED"
fi

echo ""
echo "=========================================="
echo "📊 Summary"
echo "=========================================="
echo ""

# Count pods
TOTAL_PODS=$(kubectl get pods -n mlops --no-headers | wc -l | xargs)
RUNNING_PODS=$(kubectl get pods -n mlops --no-headers | grep "Running" | grep "1/1" | wc -l | xargs)

echo "Pods:        $RUNNING_PODS/$TOTAL_PODS running"
echo ""

if [ "$EXTERNAL_IP" != "<NODE_EXTERNAL_IP>" ]; then
    echo "🌐 External Access URLs:"
    echo ""
    echo "  Airflow UI:    http://$EXTERNAL_IP:$AIRFLOW_PORT"
    echo "  Streamlit UI:  http://$EXTERNAL_IP:$STREAMLIT_PORT"
    echo "  Router API:    http://$EXTERNAL_IP:$ROUTER_PORT"
    echo ""
    echo "  Airflow Credentials: admin / admin123"
    echo ""
fi

echo "📝 Port-Forward Access:"
echo ""
echo "  Run: ./scripts/port_forward_services.sh"
echo ""
echo "  Then access:"
echo "    • MLflow:      http://localhost:5000"
echo "    • Prometheus:  http://localhost:9090"
echo "    • Grafana:     http://localhost:3000"
echo "    • Airflow:     http://localhost:8081"
echo "    • API:         http://localhost:8000"
echo "    • Router:      http://localhost:8888"
echo "    • Streamlit:   http://localhost:8501"
echo ""

echo "=========================================="
echo "✅ Test Complete!"
echo "=========================================="
echo ""

# Set external IP note
if [ "$EXTERNAL_IP" = "<NODE_EXTERNAL_IP>" ]; then
    echo "💡 Tip: Run this script inside the cluster or configure kubectl"
    echo "   to detect external IPs for NodePort testing."
fi
