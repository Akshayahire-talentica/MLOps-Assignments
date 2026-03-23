#!/usr/bin/env bash
set -euo pipefail

# Basic smoke test for model_v2 service
curl -f http://localhost:8080/health
curl -sS -X POST http://localhost:8080/predict -H "Content-Type: application/json" -d '{"user_id":999,"k":3}' | jq -e '.recommended_item_ids | length>0' >/dev/null

echo "smoke ok"
