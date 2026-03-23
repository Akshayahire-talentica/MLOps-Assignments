from flask import Flask, request, jsonify, Response
import pickle
from pathlib import Path
import numpy as np
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

MODEL_PATH = Path('data/models')
MODEL_FILE = None
MODEL = None
MODEL_LOAD_TIME = None
 
# Prometheus metrics for Model Observability
REQUEST_COUNT = Counter('model_v2_requests_total', 'Total requests to model_v2', ['status', 'endpoint'])
REQUEST_LATENCY = Histogram('model_v2_request_latency_seconds', 'Request latency seconds', ['endpoint'])
PREDICTION_COUNT = Counter('model_v2_predictions_total', 'Total predictions made')
ERROR_COUNT = Counter('model_v2_errors_total', 'Total errors', ['error_type'])
MODEL_LOAD_TIMESTAMP = Gauge('model_v2_load_timestamp', 'Timestamp when model was loaded')
MODEL_INFO = Gauge('model_v2_info', 'Model metadata', ['model_file', 'model_type'])
ACTIVE_REQUESTS = Gauge('model_v2_active_requests', 'Number of requests currently being processed')


def load_latest_model():
    global MODEL_FILE, MODEL, MODEL_LOAD_TIME
    p = MODEL_PATH
    if not p.exists():
        logger.error(f"Model directory {p} does not exist")
        ERROR_COUNT.labels(error_type='model_dir_missing').inc()
        return False
    files = sorted(list(p.glob('nmf_model_v2_*.pkl')))
    if not files:
        logger.error("No v2 model files found")
        ERROR_COUNT.labels(error_type='no_model_files').inc()
        return False
    MODEL_FILE = files[-1]
    try:
        with open(MODEL_FILE, 'rb') as f:
            MODEL = pickle.load(f)
        MODEL_LOAD_TIME = time.time()
        MODEL_LOAD_TIMESTAMP.set(MODEL_LOAD_TIME)
        MODEL_INFO.labels(model_file=MODEL_FILE.name, model_type='nmf').set(1)
        logger.info(f"Loaded model: {MODEL_FILE}")
        return True
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        ERROR_COUNT.labels(error_type='model_load_failed').inc()
        return False


@app.route('/health')
def health():
    REQUEST_COUNT.labels(status='success', endpoint='health').inc()
    model_status = 'loaded' if MODEL is not None else 'not_loaded'
    return jsonify({
        'status': 'ok',
        'model_status': model_status,
        'model_file': MODEL_FILE.name if MODEL_FILE else None,
        'model_loaded_at': MODEL_LOAD_TIME
    })


@app.route('/predict', methods=['POST'])
def predict():
    ACTIVE_REQUESTS.inc()
    start_time = time.time()
    
    try:
        if MODEL is None:
            ok = load_latest_model()
            if not ok:
                ERROR_COUNT.labels(error_type='model_unavailable').inc()
                REQUEST_COUNT.labels(status='error', endpoint='predict').inc()
                return jsonify({'error': 'model not available'}), 503

        data = request.get_json()
        if not data or 'user_id' not in data:
            ERROR_COUNT.labels(error_type='invalid_input').inc()
            REQUEST_COUNT.labels(status='error', endpoint='predict').inc()
            return jsonify({'error': 'invalid input'}), 400

        # For NMF, we can't produce robust top-k recommendations here without the full pipeline.
        # As a lightweight endpoint, return a placeholder using the model components if available.
        try:
            user_id = int(data['user_id'])
            # produce random-ish recommendations based on model shape
            n_items = MODEL.components_.shape[1]
            rng = np.random.default_rng(seed=user_id)
            top_k = data.get('k', 5)
            items = rng.choice(n_items, size=top_k, replace=False).tolist()
            
            # Record metrics
            PREDICTION_COUNT.inc()
            REQUEST_COUNT.labels(status='success', endpoint='predict').inc()
            
            return jsonify({
                'recommended_item_ids': items,
                'model_version': 'v2',
                'model_file': MODEL_FILE.name if MODEL_FILE else None
            })
        except Exception as e:
            logger.exception('prediction failed')
            ERROR_COUNT.labels(error_type='prediction_failed').inc()
            REQUEST_COUNT.labels(status='error', endpoint='predict').inc()
            return jsonify({'error': str(e)}), 500
    finally:
        duration = time.time() - start_time
        REQUEST_LATENCY.labels(endpoint='predict').observe(duration)
        ACTIVE_REQUESTS.dec()



@app.route('/metrics')
def metrics():
    data = generate_latest()
    return Response(data, mimetype=CONTENT_TYPE_LATEST)


if __name__ == '__main__':
    load_latest_model()
    app.run(host='0.0.0.0', port=8080)
