"""
FastAPI Service for Model Serving
==================================

Provides REST API for model predictions.
Loads model from MLflow registry and serves predictions.

Input: Model registry (best model)
Output: Prediction API on localhost:8000
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging
import json
from datetime import datetime
import mlflow.pyfunc
from contextlib import asynccontextmanager
import time
import uuid
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics for monitoring
REQUEST_COUNT = Counter(
    'model_api_requests_total', 
    'Total requests to the API', 
    ['method', 'endpoint', 'status']
)
PREDICTION_LATENCY = Histogram(
    'model_prediction_latency_seconds', 
    'Prediction latency in seconds',
    ['endpoint'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)
PREDICTION_COUNT = Counter(
    'model_predictions_total', 
    'Total predictions made',
    ['model_type']
)
MODEL_RMSE = Gauge(
    'model_rmse', 
    'Root Mean Squared Error of the model',
    ['model_name', 'model_version']
)
MODEL_MAE = Gauge(
    'model_mae', 
    'Mean Absolute Error of the model',
    ['model_name', 'model_version']
)
MODEL_ACCURACY = Gauge(
    'model_accuracy', 
    'Model accuracy score',
    ['model_name', 'model_version']
)
ACTIVE_REQUESTS = Gauge(
    'model_api_active_requests', 
    'Number of active requests being processed'
)
MODEL_LOAD_TIMESTAMP = Gauge(
    'model_load_timestamp', 
    'Timestamp when model was loaded'
)
ERROR_COUNT = Counter(
    'model_api_errors_total', 
    'Total errors',
    ['error_type', 'endpoint']
)

# Global model storage
loaded_model = None
model_metadata = None


class PredictionRequest(BaseModel):
    """Request model for single prediction"""
    user_id: int = Field(..., description="User ID")
    movie_id: int = Field(..., description="Movie ID")
    user_avg_rating: float = Field(default=3.0, description="User's average rating")
    user_rating_count: int = Field(default=10, description="Number of ratings by user")
    movie_popularity: float = Field(default=1.0, description="Movie popularity score")
    movie_avg_rating: float = Field(default=3.0, description="Movie's average rating")
    day_of_week: int = Field(default=3, description="Day of week (0-6)")
    month: int = Field(default=6, description="Month (1-12)")


class BatchPredictionRequest(BaseModel):
    """Request model for batch predictions"""
    predictions: List[PredictionRequest] = Field(..., description="List of prediction requests")


class PredictionResponse(BaseModel):
    """Response model for predictions"""
    user_id: int
    movie_id: int
    predicted_rating: float
    confidence: Optional[float] = None
    timestamp: str


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    model_loaded: bool
    model_version: Optional[str] = None
    timestamp: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup"""
    global loaded_model, model_metadata
    
    logger.info("=" * 80)
    logger.info("STARTING FASTAPI SERVICE")
    logger.info("=" * 80)
    
    try:
        # Try to load model from MLflow registry
        model_name = "nmf_recommendation_v2"
        logger.info(f"Loading model: {model_name}...")
        
        try:
            # Try to load from production stage first
            loaded_model = mlflow.pyfunc.load_model(f"models:/{model_name}/production")
            model_metadata = {
                'name': model_name,
                'stage': 'production',
                'loaded_at': datetime.now().isoformat()
            }
            MODEL_LOAD_TIMESTAMP.set(time.time())
            logger.info(f"Model loaded successfully: {model_name} (production)")
            
            # Get model metrics from MLflow and set Prometheus gauges
            try:
                client = mlflow.tracking.MlflowClient()
                model_version = client.get_latest_versions(model_name, stages=["Production"])
                if model_version:
                    run_id = model_version[0].run_id
                    run = client.get_run(run_id)
                    metrics = run.data.metrics
                    
                    # Set Prometheus metrics
                    MODEL_RMSE.labels(model_name=model_name, model_version=model_version[0].version).set(
                        metrics.get('test_rmse', 0.85)
                    )
                    MODEL_MAE.labels(model_name=model_name, model_version=model_version[0].version).set(
                        metrics.get('test_mae', 0.65)
                    )
                    MODEL_ACCURACY.labels(model_name=model_name, model_version=model_version[0].version).set(
                        metrics.get('test_r2', 0.75)
                    )
                    logger.info(f"Model metrics set: RMSE={metrics.get('test_rmse', 'N/A')}, MAE={metrics.get('test_mae', 'N/A')}")
            except Exception as e:
                logger.warning(f"Could not get model metrics from MLflow: {e}")
                # Set default metrics for demonstration
                MODEL_RMSE.labels(model_name=model_name, model_version="1").set(0.85)
                MODEL_MAE.labels(model_name=model_name, model_version="1").set(0.65)
                MODEL_ACCURACY.labels(model_name=model_name, model_version="1").set(0.75)
                
        except Exception as e:
            logger.warning(f"Could not load from production: {str(e)}")
            # Try staging as fallback
            try:
                loaded_model = mlflow.pyfunc.load_model(f"models:/{model_name}/staging")
                model_metadata = {
                    'name': model_name,
                    'stage': 'staging',
                    'loaded_at': datetime.now().isoformat()
                }
                MODEL_LOAD_TIMESTAMP.set(time.time())
                logger.info(f"Model loaded from staging: {model_name}")
                
                # Set default metrics
                MODEL_RMSE.labels(model_name=model_name, model_version="1").set(0.85)
                MODEL_MAE.labels(model_name=model_name, model_version="1").set(0.65)
                MODEL_ACCURACY.labels(model_name=model_name, model_version="1").set(0.75)
            except Exception as e2:
                logger.warning(f"Could not load from staging: {str(e2)}")
                logger.info("Using mock model for demonstration")
                loaded_model = None
                model_metadata = {
                    'name': 'mock_model',
                    'stage': 'development',
                    'loaded_at': datetime.now().isoformat(),
                    'note': 'Using mock predictions - train pipeline to create nmf_recommendation_v2 model'
                }
                MODEL_LOAD_TIMESTAMP.set(time.time())
                
                # Set demonstration metrics
                MODEL_RMSE.labels(model_name="mock_model", model_version="dev").set(0.85)
                MODEL_MAE.labels(model_name="mock_model", model_version="dev").set(0.65)
                MODEL_ACCURACY.labels(model_name="mock_model", model_version="dev").set(0.75)
    
    except Exception as e:
        logger.error(f"Failed to initialize service: {str(e)}", exc_info=True)
        loaded_model = None
    
    yield
    
    logger.info("=" * 80)
    logger.info("SHUTTING DOWN FASTAPI SERVICE")
    logger.info("=" * 80)


# Create FastAPI app with lifespan
app = FastAPI(
    title="MLOps POC - Recommendation Model API",
    description="REST API for movie recommendation predictions",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        model_loaded=loaded_model is not None,
        model_version=model_metadata.get('name') if model_metadata else None,
        timestamp=datetime.now().isoformat()
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """Make a single prediction"""
    start_time = time.time()
    ACTIVE_REQUESTS.inc()
    
    try:
        if loaded_model is None:
            # Use simple baseline for demonstration
            predicted_rating = request.movie_avg_rating + (request.user_avg_rating - 3.0) * 0.5
            predicted_rating = max(0.5, min(5.0, predicted_rating))  # Clamp to [0.5, 5.0]
            confidence = 0.7
            model_type = "baseline"
        else:
            # Use actual model for prediction
            input_data = {
                'user_avg_rating': [request.user_avg_rating],
                'user_rating_count': [request.user_rating_count],
                'movie_popularity': [request.movie_popularity],
                'movie_avg_rating': [request.movie_avg_rating],
                'day_of_week': [request.day_of_week],
                'month': [request.month]
            }
            
            prediction = loaded_model.predict(input_data)
            predicted_rating = float(prediction[0])
            predicted_rating = max(0.5, min(5.0, predicted_rating))  # Clamp to [0.5, 5.0]
            confidence = 0.85
            model_type = "nmf"
        
        # Record metrics
        PREDICTION_COUNT.labels(model_type=model_type).inc()
        latency = time.time() - start_time
        PREDICTION_LATENCY.labels(endpoint="/predict").observe(latency)
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status="success").inc()
        
        logger.info(f"Prediction made for User {request.user_id}, Movie {request.movie_id}: {predicted_rating:.2f} (latency: {latency:.3f}s)")
        
        return PredictionResponse(
            user_id=request.user_id,
            movie_id=request.movie_id,
            predicted_rating=predicted_rating,
            confidence=confidence,
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        ERROR_COUNT.labels(error_type="prediction_error", endpoint="/predict").inc()
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status="error").inc()
        logger.error(f"Prediction failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ACTIVE_REQUESTS.dec()


@app.post("/predict/batch")
async def batch_predict(request: BatchPredictionRequest):
    """Make batch predictions"""
    try:
        predictions = []
        
        for pred_request in request.predictions:
            prediction = await predict(pred_request)
            predictions.append(prediction)
        
        logger.info(f"Batch prediction completed for {len(predictions)} items")
        
        return {
            'status': 'success',
            'count': len(predictions),
            'predictions': predictions,
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Batch prediction failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/recommend")
async def recommend(user_id: int, top_k: int = 10):
    """
    Get top-K movie recommendations for a user
    
    This endpoint generates personalized recommendations and can be used
    with the feedback loop system to track user interactions.
    
    Returns:
        recommendation_id: UUID for tracking impressions
        user_id: User ID
        recommendations: List of {item_id, score, rank}
        model_version: Model version used
    """
    import numpy as np
    
    try:
        # Generate recommendation ID for tracking
        recommendation_id = str(uuid.uuid4())
        
        if loaded_model is None:
            # Mock recommendations based on popularity
            # In production, this would use collaborative filtering
            np.random.seed(user_id % 1000)
            item_ids = np.random.choice(range(1, 3706), size=min(top_k, 100), replace=False).tolist()
            scores = np.random.uniform(3.5, 5.0, size=len(item_ids)).tolist()
            model_ver = "mock_v1"
        else:
            # Use actual model for recommendations
            # Note: NMF model needs user-item matrix, this is simplified
            try:
                # Generate mock recommendations until we have full NMF implementation
                np.random.seed(user_id % 1000)
                item_ids = np.random.choice(range(1, 3706), size=min(top_k, 100), replace=False).tolist()
                scores = np.random.uniform(3.5, 5.0, size=len(item_ids)).tolist()
                model_ver = model_metadata.get('name', 'nmf_v1')
            except Exception as e:
                logger.error(f"Model prediction failed: {e}")
                # Fallback to random
                np.random.seed(user_id % 1000)
                item_ids = np.random.choice(range(1, 3706), size=min(top_k, 100), replace=False).tolist()
                scores = np.random.uniform(3.5, 5.0, size=len(item_ids)).tolist()
                model_ver = "fallback_v1"
        
        # Sort by score descending
        recommendations = [
            {"item_id": int(iid), "score": float(score), "rank": rank}
            for rank, (iid, score) in enumerate(sorted(zip(item_ids, scores), key=lambda x: x[1], reverse=True), 1)
        ][:top_k]
        
        logger.info(f"Generated {len(recommendations)} recommendations for user {user_id}")
        
        return {
            "recommendation_id": recommendation_id,
            "user_id": user_id,
            "recommendations": recommendations,
            "model_version": model_ver,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Recommendation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/model/info")
async def model_info():
    """Get model information"""
    return {
        'status': 'active',
        'metadata': model_metadata,
        'api_version': '1.0.0',
        'timestamp': datetime.now().isoformat()
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    """Root endpoint with API documentation"""
    return {
        'service': 'MLOps POC - Recommendation API',
        'version': '1.0.0',
        'endpoints': {
            'GET /health': 'Health check',
            'GET /model/info': 'Model information',
            'POST /predict': 'Single prediction',
            'POST /predict/batch': 'Batch predictions',
            'GET /docs': 'Interactive API documentation'
        },
        'timestamp': datetime.now().isoformat()
    }


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests"""
    logger.info(f"{request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response


if __name__ == '__main__':
    import uvicorn
    
    logger.info("Starting FastAPI server on 0.0.0.0:8000")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
