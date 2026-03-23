"""
Simple canary router that forwards /predict calls to v1 or v2 backends
based on an environment-controlled traffic split percentage.

Set `CANARY_PERCENT` env var to an integer 0-100 representing percent of
traffic to send to v2. Default 10 (10%).

This is intentionally simple for a PoC and can be replaced by Traefik
or an ingress controller later.
"""

import os
import random
import requests
import time
from flask import Flask, request, Response, jsonify
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

V1_URL = os.environ.get('V1_URL', 'http://mlops-api:8000')
V2_URL = os.environ.get('V2_URL', 'http://mlops-model-v2:8080')
CANARY_PERCENT = int(os.environ.get('CANARY_PERCENT', '10'))

# Prometheus metrics for router observability
ROUTED_TO_V1 = Counter('router_routed_to_v1_total', 'Total requests routed to v1')
ROUTED_TO_V2 = Counter('router_routed_to_v2_total', 'Total requests routed to v2')
ROUTER_LATENCY = Histogram('router_request_latency_seconds', 'Router request latency', ['backend'])
ROUTER_ERRORS = Counter('router_errors_total', 'Total routing errors', ['backend', 'error_type'])
CANARY_PERCENTAGE = Gauge('router_canary_percentage', 'Current canary percentage')
ACTIVE_REQUESTS = Gauge('router_active_requests', 'Currently active requests', ['backend'])

# Set initial canary percentage
CANARY_PERCENTAGE.set(CANARY_PERCENT)


def choose_backend():
    p = random.randint(1, 100)
    if p <= CANARY_PERCENT:
        return V2_URL
    return V1_URL


@app.route('/predict', methods=['POST'])
def predict():
    backend = choose_backend()
    backend_name = 'v2' if backend == V2_URL else 'v1'
    url = f"{backend}/predict"
    headers = {k: v for k, v in request.headers if k.lower() != 'host'}
    
    ACTIVE_REQUESTS.labels(backend=backend_name).inc()
    start_time = time.time()
    
    try:
        resp = requests.post(url, headers=headers, json=request.get_json(), timeout=10)
        resp_headers = dict(resp.headers)
        # Add a header to indicate which backend served the request (v1 or v2)
        resp_headers['X-Backend'] = backend_name
        resp_headers['X-Canary-Percent'] = str(CANARY_PERCENT)
        
        # Update prometheus counters
        if backend == V2_URL:
            ROUTED_TO_V2.inc()
        else:
            ROUTED_TO_V1.inc()
        
        # Record latency
        duration = time.time() - start_time
        ROUTER_LATENCY.labels(backend=backend_name).observe(duration)
        
        return Response(resp.content, status=resp.status_code, headers=resp_headers)
    
    except requests.exceptions.Timeout:
        ROUTER_ERRORS.labels(backend=backend_name, error_type='timeout').inc()
        return jsonify({'error': 'backend timeout', 'backend': backend_name}), 504
    except requests.exceptions.ConnectionError:
        ROUTER_ERRORS.labels(backend=backend_name, error_type='connection_error').inc()
        return jsonify({'error': 'backend unreachable', 'backend': backend_name}), 503
    except Exception as e:
        ROUTER_ERRORS.labels(backend=backend_name, error_type='unknown').inc()
        return jsonify({'error': str(e), 'backend': backend_name}), 500
    finally:
        ACTIVE_REQUESTS.labels(backend=backend_name).dec()


@app.route('/metrics')
def metrics():
    data = generate_latest()
    return Response(data, mimetype=CONTENT_TYPE_LATEST)


@app.route('/admin/set_canary', methods=['POST'])
def set_canary():
    global CANARY_PERCENT
    payload = request.get_json() or {}
    pct = payload.get('percent')
    if pct is None:
        return jsonify({'error': 'percent is required'}), 400
    try:
        pct = int(pct)
        if not (0 <= pct <= 100):
            raise ValueError()
    except Exception:
        return jsonify({'error': 'percent must be integer 0-100'}), 400
    CANARY_PERCENT = pct
    CANARY_PERCENTAGE.set(CANARY_PERCENT)
    return jsonify({'canary_percent': CANARY_PERCENT})


@app.route('/health')
def health():
    return jsonify({
        'status': 'ok',
        'canary_percent': CANARY_PERCENT,
        'v1_url': V1_URL,
        'v2_url': V2_URL
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
