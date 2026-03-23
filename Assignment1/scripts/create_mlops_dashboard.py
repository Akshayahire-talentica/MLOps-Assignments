#!/usr/bin/env python3
"""
Create MLOps Model Performance & Data Drift Dashboard in Grafana
"""

import requests
import json

GRAFANA_URL = "http://13.127.21.160:30300"
USERNAME = "admin"
PASSWORD = "admin123"

dashboard = {
    "dashboard": {
        "title": "MLOps - Model Performance & Data Drift",
        "tags": ["mlops", "model-monitoring", "drift-detection"],
        "timezone": "browser",
        "schemaVersion": 27,
        "refresh": "30s",
        "panels": [
            # Row 1: Model Performance Metrics
            {
                "id": 1,
                "title": "Model RMSE (Root Mean Squared Error)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "targets": [{
                    "expr": "model_rmse",
                    "refId": "A",
                    "legendFormat": "{{model_name}} v{{model_version}}"
                }],
                "fieldConfig": {
                    "defaults": {
                        "unit": "none",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.8, "color": "yellow"},
                                {"value": 1.2, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "id": 2,
                "title": "Model MAE (Mean Absolute Error)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                "targets": [{
                    "expr": "model_mae",
                    "refId": "A",
                    "legendFormat": "{{model_name}} v{{model_version}}"
                }],
                "fieldConfig": {
                    "defaults": {
                        "unit": "none",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.6, "color": "yellow"},
                                {"value": 0.9, "color": "red"}
                            ]
                        }
                    }
                }
            },
            
            # Row 2: Predictions & Latency
            {
                "id": 3,
                "title": "Total Predictions Made",
                "type": "stat",
                "gridPos": {"h": 4, "w": 6, "x": 0, "y": 8},
                "targets": [{
                    "expr": "sum(model_predictions_total)",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {
                        "unit": "short",
                        "color": {"mode": "value"},
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"value": None, "color": "blue"}]
                        }
                    }
                }
            },
            {
                "id": 4,
                "title": "Prediction Rate (per second)",
                "type": "stat",
                "gridPos": {"h": 4, "w": 6, "x": 6, "y": 8},
                "targets": [{
                    "expr": "rate(model_predictions_total[5m])",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {"unit": "reqps"}
                }
            },
            {
                "id": 5,
                "title": "Prediction Latency (p50, p95, p99)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                "targets": [
                    {
                        "expr": "histogram_quantile(0.50, rate(model_prediction_latency_seconds_bucket[5m]))",
                        "refId": "A",
                        "legendFormat": "p50"
                    },
                    {
                        "expr": "histogram_quantile(0.95, rate(model_prediction_latency_seconds_bucket[5m]))",
                        "refId": "B",
                        "legendFormat": "p95"
                    },
                    {
                        "expr": "histogram_quantile(0.99, rate(model_prediction_latency_seconds_bucket[5m]))",
                        "refId": "C",
                        "legendFormat": "p99"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "s",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.5, "color": "yellow"},
                                {"value": 1.0, "color": "red"}
                            ]
                        }
                    }
                }
            },
            
            # Row 3: Data Drift Monitoring
            {
                "id": 6,
                "title": "Data Drift Score by Feature",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
                "targets": [{
                    "expr": "drift_score",
                    "refId": "A",
                    "legendFormat": "{{feature}}"
                }],
                "fieldConfig": {
                    "defaults": {
                        "unit": "none",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.3, "color": "yellow"},
                                {"value": 0.5, "color": "red"}
                            ]
                        }
                    }
                },
                "alert": {
                    "name": "High Drift Score",
                    "message": "Data drift score exceeds threshold",
                    "conditions": [{
                        "evaluator": {"params": [0.5], "type": "gt"},
                        "query": {"params": ["A", "5m", "now"]}
                    }]
                }
            },
            {
                "id": 7,
                "title": "Drift Detection Status",
                "type": "stat",
                "gridPos": {"h": 4, "w": 6, "x": 12, "y": 16},
                "targets": [{
                    "expr": "drift_detected",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {
                        "mappings": [
                            {
                                "type": "value",
                                "options": {
                                    "0": {"text": "✓ No Drift", "color": "green"},
                                    "1": {"text": "⚠ Drift Detected", "color": "red"}
                                }
                            }
                        ],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": 0, "color": "green"},
                                {"value": 1, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "id": 8,
                "title": "Drift Severity",
                "type": "gauge",
                "gridPos": {"h": 4, "w": 6, "x": 18, "y": 16},
                "targets": [{
                    "expr": "drift_severity",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {
                        "min": 0,
                        "max": 2,
                        "mappings": [
                            {"type": "value", "options": {
                                "0": {"text": "None"},
                                "1": {"text": "Warning"},
                                "2": {"text": "Critical"}
                            }}
                        ],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": 0, "color": "green"},
                                {"value": 1, "color": "yellow"},
                                {"value": 2, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "id": 9,
                "title": "Drift Checks Performed",
                "type": "stat",
                "gridPos": {"h": 4, "w": 6, "x": 12, "y": 20},
                "targets": [{
                    "expr": "sum(drift_checks_total)",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {"unit": "short", "color": {"mode": "value"}}
                }
            },
            {
                "id": 10,
                "title": "Sample Sizes",
                "type": "timeseries",
                "gridPos": {"h": 4, "w": 6, "x": 18, "y": 20},
                "targets": [
                    {"expr": "drift_baseline_samples", "refId": "A", "legendFormat": "Baseline"},
                    {"expr": "drift_current_samples", "refId": "B", "legendFormat": "Current"}
                ]
            },
            
            # Row 4: Errors & System Health
            {
                "id": 11,
                "title": "API Error Rate",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24},
                "targets": [{
                    "expr": "rate(model_api_errors_total[5m])",
                    "refId": "A",
                    "legendFormat": "{{error_type}}"
                }],
                "fieldConfig": {
                    "defaults": {
                        "unit": "reqps",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.01, "color": "yellow"},
                                {"value": 0.05, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "id": 12,
                "title": "Active Requests",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24},
                "targets": [{
                    "expr": "model_api_active_requests",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {"unit": "short"}
                }
            }
        ]
    },
    "folderId": 0,
    "overwrite": True
}

# Create dashboard
response = requests.post(
    f"{GRAFANA_URL}/api/dashboards/db",
    auth=(USERNAME, PASSWORD),
    headers={"Content-Type": "application/json"},
    json=dashboard
)

if response.status_code == 200:
    result = response.json()
    print(f"✅ Dashboard created successfully!")
    print(f"   URL: {GRAFANA_URL}{result.get('url')}")
    print(f"   UID: {result.get('uid')}")
    print(f"   Status: {result.get('status')}")
    print()
    print(f"🔗 Direct link: {GRAFANA_URL}{result.get('url')}")
else:
    print(f"❌ Failed to create dashboard: {response.status_code}")
    print(f"   Response: {response.text}")
