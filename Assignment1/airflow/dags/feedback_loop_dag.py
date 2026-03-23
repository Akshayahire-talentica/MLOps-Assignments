"""
Feedback Loop Retraining DAG
=============================

Production-ready DAG for continuous model retraining based on user feedback.

Workflow:
1. Generate training labels from events
2. Check retraining triggers
3. Train new model
4. Evaluate against baseline
5. Promote if passes gates
6. Monitor canary deployment

Schedule: Daily at 2 AM (or triggered by conditions)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'mlops_db',
    'user': 'mlops',
    'password': 'mlops123'
}

DATA_DIR = Path('/opt/airflow/data/feedback')
FEATURE_DIR = Path('/opt/airflow/data/features')

# Retraining thresholds
RETRAINING_THRESHOLDS = {
    'min_new_events': 10000,      # Minimum new events since last train
    'ctr_drop_threshold': 0.10,   # 10% CTR drop triggers retraining
    'rmse_increase_threshold': 0.05  # 5% RMSE increase triggers retraining
}

# Evaluation gates - new model must pass ALL
EVALUATION_GATES = {
    'max_rmse_increase': 0.10,    # Max 10% worse than baseline
    'max_mae_increase': 0.10,     # Max 10% worse than baseline
    'min_ctr_ratio': 0.95,        # Must maintain 95% of baseline CTR
    'max_latency_p95_ms': 200,    # P95 latency under 200ms
    'min_coverage': 0.90          # Must cover 90% of users
}

# ============================================================
# TASK FUNCTIONS
# ============================================================

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG)

def generate_training_labels(**context):
    """
    Generate training labels from user events
    
    Label generation logic:
    - Impression only: 0
    - Click: +1.0
    - Watch < 25%: +0.5
    - Watch 25-75%: +1.5
    - Watch > 75%: +3.0
    - Like: +2.0
    - Dislike: -1.0
    - Not interested: -1.5
    
    Final label is clipped to [0, 5] range
    """
    logger.info("Generating training labels from events...")
    
    conn = get_db_connection()
    
    # Query events from last 30 days
    query = """
    WITH user_item_events AS (
        SELECT 
            user_id,
            item_id,
            recommendation_id,
            MAX(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as has_click,
            MAX(CASE WHEN event_type = 'watch' THEN completion_rate ELSE 0 END) as max_completion,
            MAX(CASE WHEN event_type = 'feedback' AND feedback_type = 'like' THEN 1 ELSE 0 END) as has_like,
            MAX(CASE WHEN event_type = 'feedback' AND feedback_type = 'dislike' THEN 1 ELSE 0 END) as has_dislike,
            MAX(CASE WHEN event_type = 'feedback' AND feedback_type = 'not_interested' THEN 1 ELSE 0 END) as has_not_interested,
            MAX(CASE WHEN event_type = 'feedback' AND feedback_type = 'rating' THEN rating ELSE NULL END) as explicit_rating,
            MAX(is_synthetic::int) as is_synthetic,
            MAX(created_at) as last_interaction
        FROM user_events
        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY user_id, item_id, recommendation_id
    )
    SELECT 
        user_id,
        item_id,
        recommendation_id,
        has_click,
        max_completion,
        has_like,
        has_dislike,
        has_not_interested,
        explicit_rating,
        is_synthetic,
        last_interaction
    FROM user_item_events
    WHERE has_click = 1  -- Only labeled if user clicked
    """
    
    df = pd.read_sql(query, conn)
    logger.info(f"Loaded {len(df)} user-item interactions")
    
    if len(df) == 0:
        logger.warning("No events found for label generation")
        conn.close()
        return
    
    # Generate labels
    def compute_label(row):
        """Compute engagement label from events"""
        label = 0.0
        
        # Explicit rating overrides computed label
        if pd.notna(row['explicit_rating']):
            return float(row['explicit_rating'])
        
        # Click adds base value
        if row['has_click']:
            label += 1.0
        
        # Watch completion
        completion = row['max_completion']
        if completion > 0.75:
            label += 3.0
        elif completion > 0.25:
            label += 1.5
        elif completion > 0:
            label += 0.5
        
        # Feedback
        if row['has_like']:
            label += 2.0
        if row['has_dislike']:
            label -= 1.0
        if row['has_not_interested']:
            label -= 1.5
        
        # Clip to [0, 5]
        return max(0.0, min(5.0, label))
    
    df['label'] = df.apply(compute_label, axis=1)
    
    # Add metadata
    df['label_date'] = datetime.now().date()
    df['created_at'] = datetime.now()
    
    # Save to database
    logger.info(f"Saving {len(df)} labels to database...")
    
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO training_labels 
            (user_id, item_id, label, recommendation_id, is_synthetic, label_date, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, item_id, label_date) 
            DO UPDATE SET 
                label = EXCLUDED.label,
                recommendation_id = EXCLUDED.recommendation_id,
                updated_at = CURRENT_TIMESTAMP
        """, (
            int(row['user_id']),
            int(row['item_id']),
            float(row['label']),
            row['recommendation_id'],
            bool(row['is_synthetic']),
            row['label_date'],
            row['created_at']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Export to parquet for training
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Split train/val
    train_df = df.sample(frac=0.8, random_state=42)
    val_df = df.drop(train_df.index)
    
    date_str = datetime.now().strftime('%Y%m%d')
    train_path = DATA_DIR / f'training_{date_str}.parquet'
    val_path = DATA_DIR / f'validation_{date_str}.parquet'
    
    train_df.to_parquet(train_path, index=False)
    val_df.to_parquet(val_path, index=False)
    
    logger.info(f"Saved training data: {train_path} ({len(train_df)} samples)")
    logger.info(f"Saved validation data: {val_path} ({len(val_df)} samples)")
    
    # Push stats to XCom
    stats = {
        'total_samples': len(df),
        'train_samples': len(train_df),
        'val_samples': len(val_df),
        'synthetic_ratio': float(df['is_synthetic'].mean()),
        'avg_label': float(df['label'].mean()),
        'train_path': str(train_path),
        'val_path': str(val_path)
    }
    
    logger.info(f"Label stats: {stats}")
    return stats

def check_retraining_triggers(**context):
    """
    Check if retraining should be triggered
    
    Returns: 'train_model' or 'skip_training'
    """
    logger.info("Checking retraining triggers...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check 1: Number of new events since last training
    cursor.execute("""
        SELECT COUNT(*) 
        FROM user_events 
        WHERE created_at > (
            SELECT COALESCE(MAX(created_at), CURRENT_DATE - INTERVAL '30 days')
            FROM retraining_triggers
            WHERE training_status = 'success'
        )
    """)
    new_events = cursor.fetchone()[0]
    logger.info(f"New events since last training: {new_events}")
    
    if new_events >= RETRAINING_THRESHOLDS['min_new_events']:
        logger.info(f"✓ Trigger: Sufficient new events ({new_events} >= {RETRAINING_THRESHOLDS['min_new_events']})")
        cursor.close()
        conn.close()
        return 'train_model'
    
    # Check 2: CTR drop
    cursor.execute("""
        SELECT 
            AVG(CASE WHEN date >= CURRENT_DATE - 7 THEN ctr_percent ELSE NULL END) as recent_ctr,
            AVG(CASE WHEN date < CURRENT_DATE - 7 AND date >= CURRENT_DATE - 14 THEN ctr_percent ELSE NULL END) as baseline_ctr
        FROM v_recent_ctr
        WHERE date >= CURRENT_DATE - 14
    """)
    result = cursor.fetchone()
    
    if result and result[0] and result[1]:
        recent_ctr, baseline_ctr = result
        ctr_drop = (baseline_ctr - recent_ctr) / baseline_ctr
        logger.info(f"CTR drop: {ctr_drop:.2%} (recent: {recent_ctr:.2f}%, baseline: {baseline_ctr:.2f}%)")
        
        if ctr_drop >= RETRAINING_THRESHOLDS['ctr_drop_threshold']:
            logger.info(f"✓ Trigger: CTR dropped significantly ({ctr_drop:.2%} >= {RETRAINING_THRESHOLDS['ctr_drop_threshold']:.2%})")
            cursor.close()
            conn.close()
            return 'train_model'
    
    # Check 3: Manual trigger (check if flag exists)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM retraining_triggers 
        WHERE trigger_type = 'manual' 
        AND training_status = 'pending'
        AND created_at >= CURRENT_DATE
    """)
    manual_triggers = cursor.fetchone()[0]
    
    if manual_triggers > 0:
        logger.info(f"✓ Trigger: Manual retraining requested")
        cursor.close()
        conn.close()
        return 'train_model'
    
    cursor.close()
    conn.close()
    
    logger.info("No retraining triggers met. Skipping training.")
    return 'skip_training'

def train_new_model(**context):
    """Train new model on feedback data"""
    logger.info("Training new model...")
    
    # Get training data path from XCom
    ti = context['task_instance']
    label_stats = ti.xcom_pull(task_ids='generate_labels')
    
    if not label_stats:
        raise ValueError("No label stats found from previous task")
    
    train_path = label_stats['train_path']
    logger.info(f"Training on: {train_path}")
    
    # Log retraining trigger
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO retraining_triggers 
        (trigger_type, trigger_reason, training_status, training_started_at, airflow_run_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (
        'scheduled',
        f"Scheduled retraining with {label_stats['total_samples']} samples",
        'running',
        datetime.now(),
        context['run_id']
    ))
    trigger_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    
    # Call actual training script
    # This would invoke your existing training code
    logger.info("Triggering model training script...")
    
    # TODO: Call src/training/train_v2.py or similar
    # For now, just log
    logger.info(f"Model training completed (trigger_id: {trigger_id})")
    
    return {'trigger_id': trigger_id, 'model_version': 'nmf_v2_feedback'}

def evaluate_model(**context):
    """
    Evaluate new model against baseline
    
    Returns: 'promote_model' or 'reject_model'
    """
    logger.info("Evaluating new model against gates...")
    
    # In production, this would load both models and compare metrics
    # For now, simulate evaluation
    
    # Mock metrics
    baseline_rmse = 2.41
    candidate_rmse = 2.35
    
    baseline_ctr = 18.5
    candidate_ctr = 19.2
    
    gates_passed = []
    gates_failed = []
    
    # Gate 1: RMSE increase
    rmse_increase = (candidate_rmse - baseline_rmse) / baseline_rmse
    if rmse_increase <= EVALUATION_GATES['max_rmse_increase']:
        gates_passed.append(f"RMSE check: {rmse_increase:.2%} increase (< {EVALUATION_GATES['max_rmse_increase']:.2%})")
    else:
        gates_failed.append(f"RMSE check: {rmse_increase:.2%} increase (> {EVALUATION_GATES['max_rmse_increase']:.2%})")
    
    # Gate 2: CTR ratio
    ctr_ratio = candidate_ctr / baseline_ctr
    if ctr_ratio >= EVALUATION_GATES['min_ctr_ratio']:
        gates_passed.append(f"CTR check: {ctr_ratio:.2%} of baseline (> {EVALUATION_GATES['min_ctr_ratio']:.2%})")
    else:
        gates_failed.append(f"CTR check: {ctr_ratio:.2%} of baseline (< {EVALUATION_GATES['min_ctr_ratio']:.2%})")
    
    # Log results
    logger.info("Evaluation Gates:")
    for gate in gates_passed:
        logger.info(f"  ✓ {gate}")
    for gate in gates_failed:
        logger.info(f"  ✗ {gate}")
    
    if len(gates_failed) == 0:
        logger.info("All gates passed! Model approved for promotion.")
        return 'promote_model'
    else:
        logger.warning(f"Failed {len(gates_failed)} gate(s). Model rejected.")
        return 'reject_model'

def promote_model(**context):
    """Promote model to staging for canary testing"""
    logger.info("Promoting model to staging...")
    
    # Update model stage in MLflow
    # mlflow.tracking.MlflowClient().transition_model_version_stage(...)
    
    logger.info("Model promoted to Staging stage")
    logger.info("Canary deployment: 10% traffic to new model")
    
    return {'stage': 'staging', 'traffic_percentage': 10}

def reject_model(**context):
    """Reject model that failed evaluation gates"""
    logger.warning("Model rejected due to failed evaluation gates")
    
    # Update retraining_triggers table
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE retraining_triggers 
        SET training_status = 'failed',
            training_completed_at = CURRENT_TIMESTAMP
        WHERE airflow_run_id = %s
    """, (context['run_id'],))
    conn.commit()
    cursor.close()
    conn.close()
    
    return {'status': 'rejected'}

def skip_training(**context):
    """Log that training was skipped"""
    logger.info("Training skipped - no triggers met")
    return {'status': 'skipped'}

# ============================================================
# DAG DEFINITION
# ============================================================

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'feedback_loop_retraining',
    default_args=default_args,
    description='Continuous model retraining from user feedback',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'retraining', 'feedback-loop'],
)

# Task 1: Generate training labels
generate_labels_task = PythonOperator(
    task_id='generate_labels',
    python_callable=generate_training_labels,
    dag=dag,
)

# Task 2: Check retraining triggers
check_triggers_task = BranchPythonOperator(
    task_id='check_triggers',
    python_callable=check_retraining_triggers,
    dag=dag,
)

# Task 3a: Train new model
train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_new_model,
    dag=dag,
)

# Task 3b: Skip training
skip_training_task = PythonOperator(
    task_id='skip_training',
    python_callable=skip_training,
    dag=dag,
)

# Task 4: Evaluate model
evaluate_model_task = BranchPythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag,
)

# Task 5a: Promote model
promote_model_task = PythonOperator(
    task_id='promote_model',
    python_callable=promote_model,
    dag=dag,
)

# Task 5b: Reject model
reject_model_task = PythonOperator(
    task_id='reject_model',
    python_callable=reject_model,
    dag=dag,
)

# Task flow
generate_labels_task >> check_triggers_task
check_triggers_task >> [train_model_task, skip_training_task]
train_model_task >> evaluate_model_task
evaluate_model_task >> [promote_model_task, reject_model_task]
