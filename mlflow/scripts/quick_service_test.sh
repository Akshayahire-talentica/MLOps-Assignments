#!/bin/bash

# Quick Service Test - Verifies all MLOps services are accessible
# Tests both internal (ClusterIP) and external (NodePort) access

echo "=========================================="
echo "Quick MLOps Services Test"
echo "=========================================="
echo ""

# Get external IP
EXTERNAL_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')

echo "📍 External IP: $EXTERNAL_IP"
echo ""

echo "=========================================="
echo "Testing Pod Status"
echo "=========================================="
kubectl get pods -n mlops
echo ""

echo "=========================================="
echo "Testing External NodePort Services"
echo "=========================================="
echo ""

# Test Airflow
echo "🔹 Airflow UI (Port 30080):"
AIRFLOW_RESPONSE=$(curl -s -m 5 http://$EXTERNAL_IP:30080/health)
if echo "$AIRFLOW_RESPONSE" | grep -q "healthy"; then
    echo "   ✅ WORKING - http://$EXTERNAL_IP:30080"
    echo "   👤 Login: admin / admin123"
else
    echo "   ❌ NOT ACCESSIBLE"
fi
echo ""

# Test Router
echo "🔹 Router API (Port 31233):"
ROUTER_RESPONSE=$(curl -s -m 5 http://$EXTERNAL_IP:31233/health)
if echo "$ROUTER_RESPONSE" | grep -q "ok"; then
    echo "   ✅ WORKING - http://$EXTERNAL_IP:31233"
    echo "   Response: $ROUTER_RESPONSE"
else
    echo "   ❌ NOT ACCESSIBLE"
fi
echo ""

# Test Streamlit
echo "🔹 Streamlit UI (Port 30501):"
STREAMLIT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -m 5 http://$EXTERNAL_IP:30501/)
if [ "$STREAMLIT_CODE" = "200" ]; then
    echo "   ✅ WORKING - http://$EXTERNAL_IP:30501"
else
    echo "   ❌ NOT ACCESSIBLE (HTTP $STREAMLIT_CODE)"
fi
echo ""

echo "=========================================="
echo "Testing Internal Services"
echo "=========================================="
echo ""

echo "🔹 MLflow:"
kubectl get svc mlflow -n mlops -o wide
echo ""

echo "🔹 Prometheus:"
kubectl get svc prometheus -n mlops -o wide
echo ""

echo "🔹 Grafana:"
kubectl get svc grafana -n mlops -o wide
echo ""

echo "=========================================="
echo "Service Endpoints Summary"
echo "=========================================="
echo ""
kubectl get svc -n mlops
echo ""

echo "=========================================="
echo "✅ Access Your Services"
echo "=========================================="
echo ""
echo "🌐 EXTERNAL ACCESS (Direct from browser):"
echo "   • Airflow:    http://$EXTERNAL_IP:30080 (admin/admin123)"
echo "   • Streamlit:  http://$EXTERNAL_IP:30501"
echo "   • Router API: http://$EXTERNAL_IP:31233"
echo ""
echo "🔧 PORT-FORWARD ACCESS (Run ./scripts/port_forward_services.sh):"
echo "   • MLflow:     http://localhost:5000"
echo "   • Prometheus: http://localhost:9090"
echo "   • Grafana:    http://localhost:3000 (admin/admin123)"
echo "   • Airflow:    http://localhost:8081 (admin/admin123)"
echo "   • API:        http://localhost:8000"
echo "   • Streamlit:  http://localhost:8501"
echo ""
echo "=========================================="
