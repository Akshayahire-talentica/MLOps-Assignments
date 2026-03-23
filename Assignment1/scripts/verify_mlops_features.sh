#!/bin/bash
# Verify All MLOps Features Implementation
# Quick check to ensure all 4 spreadsheet tasks are implemented

echo "========================================"
echo "MLOps POC - Feature Verification"
echo "========================================"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}[OK]${NC} $2"
        return 0
    else
        echo -e "${RED}[MISSING]${NC} $2"
        return 1
    fi
}

check_directory() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}[OK]${NC} $2"
        return 0
    else
        echo -e "${RED}[MISSING]${NC} $2"
        return 1
    fi
}

echo "1. Drift Detection (Owner: Vikas)"
echo "-----------------------------------"
check_file "src/monitoring/drift_detector.py" "Drift detector module"
check_file "config/drift_config.yaml" "Drift configuration"
check_directory "reports/drift" "Drift reports directory" 2>/dev/null || mkdir -p reports/drift
echo ""

echo "2. Drift Alerts (Owner: Vikas)"
echo "-----------------------------------"
check_file "monitoring/prometheus/alerts.yml" "Prometheus alert rules"
check_file "monitoring/prometheus/prometheus.yml" "Prometheus configuration"
echo ""

echo "3. Rollback Automation (Owner: Aksay)"
echo "-----------------------------------"
check_file "scripts/rollback_model.py" "Rollback automation script"
echo ""

echo "4. Registry Audit Trail (Owner: Aksay)"
echo "-----------------------------------"
check_file "src/serving/audit_logger.py" "Audit logger module"
check_directory "reports/audit" "Audit reports directory" 2>/dev/null || mkdir -p reports/audit
echo ""

echo "5. Documentation"
echo "-----------------------------------"
check_file "CICD_IMPLEMENTATION.md" "Consolidated CI/CD guide"
check_file "IMPLEMENTATION_SUMMARY.md" "Implementation summary"
check_file "MLOPS_DEMO_GUIDE.md" "Demo guide"
check_file "DEMO_QUICK_REFERENCE.md" "Quick reference"
echo ""

echo "6. Pipeline Integration"
echo "-----------------------------------"
check_file ".github/workflows/production-pipeline.yml" "Production pipeline"

# Check if drift detection is integrated
if grep -q "src/monitoring/drift_detector.py" .github/workflows/production-pipeline.yml; then
    echo -e "${GREEN}[OK]${NC} Drift detection integrated in pipeline"
else
    echo -e "${RED}[MISSING]${NC} Drift detection not integrated"
fi

# Check if audit logging is integrated
if grep -q "audit_logger" .github/workflows/production-pipeline.yml; then
    echo -e "${GREEN}[OK]${NC} Audit logging integrated in pipeline"
else
    echo -e "${RED}[MISSING]${NC} Audit logging not integrated"
fi

echo ""
echo "========================================"
echo "Feature Test Commands"
echo "========================================"
echo ""
echo "Test Drift Detection:"
echo "  python3 src/monitoring/drift_detector.py"
echo ""
echo "Test Rollback:"
echo "  python3 scripts/rollback_model.py --history"
echo ""
echo "Test Audit Trail:"
echo "  python3 src/serving/audit_logger.py"
echo ""
echo "========================================"
echo "Verification Complete!"
echo "========================================"
