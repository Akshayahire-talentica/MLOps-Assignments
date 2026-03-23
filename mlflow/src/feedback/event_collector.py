"""
Event Collector Service
=======================

Lightweight FastAPI service for collecting user feedback events.
Stores events in PostgreSQL for training data generation.

Endpoints:
- POST /events - Collect individual event
- POST /events/batch - Collect multiple events
- GET /health - Health check
- GET /metrics - Prometheus metrics
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import Response
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import logging
import json
import psycopg2
from psycopg2.extras import execute_values, Json
import os
import uuid
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'mlops_db'),
    'user': os.getenv('POSTGRES_USER', 'mlops'),
    'password': os.getenv('POSTGRES_PASSWORD', 'mlops123')
}

# ============================================================
# PROMETHEUS METRICS
# ============================================================

EVENTS_COLLECTED = Counter(
    'events_collected_total',
    'Total events collected',
    ['event_type', 'is_synthetic']
)

EVENT_LATENCY = Histogram(
    'event_collection_latency_seconds',
    'Event collection latency',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1]
)

BATCH_SIZE = Histogram(
    'event_batch_size',
    'Size of event batches',
    buckets=[1, 5, 10, 50, 100, 500]
)

DB_ERRORS = Counter(
    'event_collector_db_errors_total',
    'Database errors',
    ['operation']
)

# ============================================================
# ENUMS
# ============================================================

class EventType(str, Enum):
    IMPRESSION = "impression"
    CLICK = "click"
    WATCH = "watch"
    FEEDBACK = "feedback"

class FeedbackType(str, Enum):
    LIKE = "like"
    DISLIKE = "dislike"
    NOT_INTERESTED = "not_interested"
    RATING = "rating"

# ============================================================
# PYDANTIC MODELS
# ============================================================

class ImpressionEvent(BaseModel):
    """Recommendation shown to user"""
    event_type: EventType = EventType.IMPRESSION
    recommendation_id: str
    user_id: int
    item_ids: List[int] = Field(..., min_items=1)
    scores: Optional[List[float]] = None
    model_name: str
    model_version: str
    model_run_id: Optional[str] = None
    is_synthetic: bool = False
    context: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None

class ClickEvent(BaseModel):
    """User clicked on recommended item"""
    event_type: EventType = EventType.CLICK
    recommendation_id: Optional[str] = None
    user_id: int
    item_id: int
    position: int = Field(..., ge=0, description="Position in recommendation list (0-indexed)")
    is_synthetic: bool = False
    session_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None

class WatchEvent(BaseModel):
    """User watched content"""
    event_type: EventType = EventType.WATCH
    user_id: int
    item_id: int
    watch_duration_seconds: int = Field(..., ge=0)
    total_duration_seconds: int = Field(..., gt=0)
    recommendation_id: Optional[str] = None
    is_synthetic: bool = False
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    
    @validator('watch_duration_seconds')
    def validate_watch_duration(cls, v, values):
        if 'total_duration_seconds' in values and v > values['total_duration_seconds']:
            logger.warning(f"Watch duration {v} exceeds total duration {values['total_duration_seconds']}")
        return v

class FeedbackEvent(BaseModel):
    """User provided explicit feedback"""
    event_type: EventType = EventType.FEEDBACK
    user_id: int
    item_id: int
    feedback_type: FeedbackType
    rating: Optional[float] = Field(None, ge=0, le=5)
    recommendation_id: Optional[str] = None
    is_synthetic: bool = False
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None

class BatchEventRequest(BaseModel):
    """Batch of events"""
    events: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000)

class EventResponse(BaseModel):
    """Response after event collection"""
    status: str
    event_id: Optional[str] = None
    events_count: Optional[int] = None
    message: str
    timestamp: datetime

# ============================================================
# DATABASE
# ============================================================

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

def insert_recommendation_log(conn, event: ImpressionEvent):
    """Insert recommendation log"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO recommendation_logs 
            (recommendation_id, user_id, item_ids, scores, model_name, 
             model_version, model_run_id, is_synthetic, context, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (recommendation_id) DO NOTHING
        """, (
            event.recommendation_id,
            event.user_id,
            event.item_ids,
            event.scores,
            event.model_name,
            event.model_version,
            event.model_run_id,
            event.is_synthetic,
            Json(event.context) if event.context else None,
            event.timestamp or datetime.utcnow()
        ))
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        DB_ERRORS.labels(operation='insert_recommendation').inc()
        logger.error(f"Failed to insert recommendation log: {e}")
        raise

def insert_user_event(conn, event_data: dict):
    """Insert user event"""
    try:
        cursor = conn.cursor()
        
        # Build dynamic INSERT based on event type
        event_type = event_data.get('event_type')
        base_fields = ['event_type', 'user_id', 'item_id', 'recommendation_id', 
                      'is_synthetic', 'session_id', 'context', 'created_at']
        base_values = [
            event_type,
            event_data.get('user_id'),
            event_data.get('item_id'),
            event_data.get('recommendation_id'),
            event_data.get('is_synthetic', False),
            event_data.get('session_id'),
            Json(event_data.get('context')) if event_data.get('context') else None,
            event_data.get('timestamp') or datetime.utcnow()
        ]
        
        # Add event-specific fields
        if event_type == 'click':
            base_fields.append('position')
            base_values.append(event_data.get('position'))
        
        elif event_type == 'watch':
            base_fields.extend(['watch_duration_seconds', 'total_duration_seconds', 'completion_rate'])
            watch_dur = event_data.get('watch_duration_seconds', 0)
            total_dur = event_data.get('total_duration_seconds', 1)
            completion = watch_dur / total_dur if total_dur > 0 else 0
            base_values.extend([watch_dur, total_dur, completion])
        
        elif event_type == 'feedback':
            base_fields.extend(['feedback_type', 'rating'])
            base_values.extend([
                event_data.get('feedback_type'),
                event_data.get('rating')
            ])
        
        placeholders = ','.join(['%s'] * len(base_fields))
        query = f"""
            INSERT INTO user_events ({','.join(base_fields)})
            VALUES ({placeholders})
            RETURNING event_id
        """
        
        cursor.execute(query, base_values)
        event_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return str(event_id)
    
    except Exception as e:
        conn.rollback()
        DB_ERRORS.labels(operation='insert_event').inc()
        logger.error(f"Failed to insert user event: {e}", exc_info=True)
        raise

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(
    title="Event Collector Service",
    description="Collects user feedback events for ML model training",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.post("/events/impression", response_model=EventResponse)
async def collect_impression(event: ImpressionEvent):
    """Collect impression event"""
    with EVENT_LATENCY.time():
        try:
            conn = get_db_connection()
            insert_recommendation_log(conn, event)
            conn.close()
            
            EVENTS_COLLECTED.labels(
                event_type='impression',
                is_synthetic=str(event.is_synthetic)
            ).inc()
            
            logger.info(f"Impression logged: rec_id={event.recommendation_id}, user={event.user_id}, items={len(event.item_ids)}")
            
            return EventResponse(
                status="success",
                event_id=event.recommendation_id,
                message="Impression logged",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to log impression: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/click", response_model=EventResponse)
async def collect_click(event: ClickEvent):
    """Collect click event"""
    with EVENT_LATENCY.time():
        try:
            conn = get_db_connection()
            event_id = insert_user_event(conn, event.dict())
            conn.close()
            
            EVENTS_COLLECTED.labels(
                event_type='click',
                is_synthetic=str(event.is_synthetic)
            ).inc()
            
            logger.info(f"Click logged: user={event.user_id}, item={event.item_id}, position={event.position}")
            
            return EventResponse(
                status="success",
                event_id=event_id,
                message="Click logged",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to log click: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/watch", response_model=EventResponse)
async def collect_watch(event: WatchEvent):
    """Collect watch event"""
    with EVENT_LATENCY.time():
        try:
            conn = get_db_connection()
            event_id = insert_user_event(conn, event.dict())
            conn.close()
            
            EVENTS_COLLECTED.labels(
                event_type='watch',
                is_synthetic=str(event.is_synthetic)
            ).inc()
            
            completion_rate = event.watch_duration_seconds / event.total_duration_seconds
            logger.info(f"Watch logged: user={event.user_id}, item={event.item_id}, completion={completion_rate:.2%}")
            
            return EventResponse(
                status="success",
                event_id=event_id,
                message="Watch logged",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to log watch: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/feedback", response_model=EventResponse)
async def collect_feedback(event: FeedbackEvent):
    """Collect feedback event"""
    with EVENT_LATENCY.time():
        try:
            conn = get_db_connection()
            event_id = insert_user_event(conn, event.dict())
            conn.close()
            
            EVENTS_COLLECTED.labels(
                event_type='feedback',
                is_synthetic=str(event.is_synthetic)
            ).inc()
            
            logger.info(f"Feedback logged: user={event.user_id}, item={event.item_id}, type={event.feedback_type}")
            
            return EventResponse(
                status="success",
                event_id=event_id,
                message="Feedback logged",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to log feedback: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/batch", response_model=EventResponse)
async def collect_batch(request: BatchEventRequest):
    """Collect batch of events"""
    with EVENT_LATENCY.time():
        BATCH_SIZE.observe(len(request.events))
        
        try:
            conn = get_db_connection()
            event_ids = []
            
            for event_data in request.events:
                event_type = event_data.get('event_type')
                
                if event_type == 'impression':
                    # Handle as impression
                    insert_recommendation_log(conn, ImpressionEvent(**event_data))
                    event_ids.append(event_data.get('recommendation_id'))
                else:
                    # Handle as user event
                    event_id = insert_user_event(conn, event_data)
                    event_ids.append(event_id)
                
                EVENTS_COLLECTED.labels(
                    event_type=event_type,
                    is_synthetic=str(event_data.get('is_synthetic', False))
                ).inc()
            
            conn.close()
            
            logger.info(f"Batch logged: {len(request.events)} events")
            
            return EventResponse(
                status="success",
                events_count=len(event_ids),
                message=f"Batch of {len(event_ids)} events logged",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to log batch: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/stats")
async def get_stats():
    """Get event collection statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get today's stats
        cursor.execute("""
            SELECT 
                event_type,
                COUNT(*) as count,
                SUM(CASE WHEN is_synthetic THEN 1 ELSE 0 END) as synthetic_count
            FROM user_events
            WHERE created_at >= CURRENT_DATE
            GROUP BY event_type
        """)
        
        event_stats = {}
        for row in cursor.fetchall():
            event_stats[row[0]] = {
                'total': row[1],
                'synthetic': row[2],
                'real': row[1] - row[2]
            }
        
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "date": datetime.utcnow().date().isoformat(),
            "event_stats": event_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Event Collector",
        "version": "1.0.0",
        "endpoints": {
            "POST /events/impression": "Log impression event",
            "POST /events/click": "Log click event",
            "POST /events/watch": "Log watch event",
            "POST /events/feedback": "Log feedback event",
            "POST /events/batch": "Log batch of events",
            "GET /stats": "Get collection statistics",
            "GET /health": "Health check",
            "GET /metrics": "Prometheus metrics"
        }
    }

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting Event Collector Service on 0.0.0.0:8002")
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")
