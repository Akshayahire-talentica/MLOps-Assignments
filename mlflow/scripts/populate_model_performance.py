#!/usr/bin/env python3
"""
Populate Model Performance Table
Fetches metrics from MLflow and calculates business metrics from user events
"""

import os
import sys
import psycopg2
from datetime import datetime, date
import mlflow
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

def connect_db():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB', 'mlops_db'),
        user=os.getenv('POSTGRES_USER', 'mlops'),
        password=os.getenv('POSTGRES_PASSWORD', 'mlops123')
    )

def get_mlflow_metrics():
    """Fetch model metrics from MLflow"""
    mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
    mlflow.set_tracking_uri(mlflow_uri)
    client = mlflow.tracking.MlflowClient()
    
    models_data = []
    
    try:
        # Get all experiments
        experiments = client.search_experiments()
        
        for exp in experiments:
            # Get runs from this experiment
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                order_by=["start_time DESC"],
                max_results=10
            )
            
            for run in runs:
                metrics = run.data.metrics
                params = run.data.params
                
                # Extract model info
                model_data = {
                    'model_name': params.get('model_name', 'unknown'),
                    'model_version': f"v{run.info.run_id[:8]}",
                    'model_run_id': run.info.run_id,
                    'stage': params.get('stage', 'staging'),
                    'rmse': metrics.get('rmse'),
                    'mae': metrics.get('mae'),
                    'mse': metrics.get('mse'),
                    'r2_score': metrics.get('r2_score'),
                    'training_samples_count': int(params.get('train_samples', 0)) if params.get('train_samples') else None,
                    'evaluation_date': date.today(),
                    'created_at': datetime.fromtimestamp(run.info.start_time / 1000)
                }
                
                models_data.append(model_data)
        
        print(f"✅ Found {len(models_data)} model runs in MLflow")
        
    except Exception as e:
        print(f"⚠️ Error fetching MLflow data: {e}")
    
    return models_data

def calculate_business_metrics(conn, model_version=None):
    """Calculate business metrics from user events"""
    cursor = conn.cursor()
    
    # Calculate CTR and engagement metrics
    query = """
        WITH model_recs AS (
            SELECT 
                r.model_version,
                r.recommendation_id,
                r.created_at
            FROM recommendation_logs r
            {where_clause}
        ),
        events AS (
            SELECT 
                mr.model_version,
                COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.event_id END) as clicks,
                COUNT(DISTINCT CASE WHEN e.event_type = 'watch' THEN e.event_id END) as watches,
                COUNT(DISTINCT CASE WHEN e.event_type = 'feedback' THEN e.event_id END) as feedback,
                COUNT(DISTINCT mr.recommendation_id) as impressions,
                AVG(CASE WHEN e.event_type = 'watch' THEN e.completion_rate END) as avg_watch_completion,
                AVG(tl.label) as avg_engagement_score
            FROM model_recs mr
            LEFT JOIN user_events e ON e.recommendation_id = mr.recommendation_id
            LEFT JOIN training_labels tl ON tl.user_id = e.user_id AND tl.item_id = e.item_id
            GROUP BY mr.model_version
        )
        SELECT 
            model_version,
            impressions,
            clicks,
            watches,
            feedback,
            CASE WHEN impressions > 0 THEN (clicks::float / impressions * 100) ELSE 0 END as ctr,
            CASE WHEN clicks > 0 THEN (watches::float / clicks * 100) ELSE 0 END as watch_rate,
            avg_watch_completion * 100 as avg_watch_completion_pct,
            avg_engagement_score
        FROM events
    """
    
    where_clause = f"WHERE r.model_version = '{model_version}'" if model_version else ""
    query = query.format(where_clause=where_clause)
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    metrics = {}
    for row in results:
        metrics[row[0]] = {
            'ctr': row[5],
            'watch_rate': row[6],
            'avg_watch_completion': row[7],
            'avg_engagement_score': row[8]
        }
    
    cursor.close()
    return metrics

def insert_performance_data(conn, model_data, business_metrics):
    """Insert model performance data into database"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO model_performance (
            model_name, model_version, model_run_id, stage,
            rmse, mae, mse, r2_score,
            ctr, watch_rate, avg_watch_completion, avg_engagement_score,
            training_samples_count,
            evaluation_date, is_production,
            created_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s,
            %s
        )
        ON CONFLICT DO NOTHING
    """
    
    inserted = 0
    for model in model_data:
        model_version = model['model_version']
        biz_metrics = business_metrics.get(model_version, {})
        
        values = (
            model['model_name'],
            model['model_version'],
            model['model_run_id'],
            model['stage'],
            model.get('rmse'),
            model.get('mae'),
            model.get('mse'),
            model.get('r2_score'),
            biz_metrics.get('ctr'),
            biz_metrics.get('watch_rate'),
            biz_metrics.get('avg_watch_completion'),
            biz_metrics.get('avg_engagement_score'),
            model.get('training_samples_count'),
            model['evaluation_date'],
            model['stage'] == 'production',
            model['created_at']
        )
        
        try:
            cursor.execute(insert_query, values)
            inserted += 1
        except Exception as e:
            print(f"⚠️ Error inserting {model_version}: {e}")
    
    conn.commit()
    cursor.close()
    
    return inserted

def populate_current_deployment():
    """Add entry for currently deployed model based on recommendation logs"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get the currently deployed model version
    cursor.execute("""
        SELECT model_version, MAX(created_at) as last_seen
        FROM recommendation_logs 
        GROUP BY model_version
        ORDER BY last_seen DESC 
        LIMIT 1
    """)
    result = cursor.fetchone()
    
    if result:
        current_version = result[0]
        
        # Calculate business metrics for this version
        business_metrics = calculate_business_metrics(conn, current_version)
        
        if current_version in business_metrics:
            biz = business_metrics[current_version]
            
            # Insert as production model
            cursor.execute("""
                INSERT INTO model_performance (
                    model_name, model_version, stage,
                    ctr, watch_rate, avg_watch_completion, avg_engagement_score,
                    evaluation_date, is_production
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT DO NOTHING
            """, (
                'nmf_recommender',
                current_version,
                'production',
                biz.get('ctr'),
                biz.get('watch_rate'),
                biz.get('avg_watch_completion'),
                biz.get('avg_engagement_score'),
                date.today(),
                True
            ))
            
            conn.commit()
            print(f"✅ Added current deployment: {current_version}")
    
    cursor.close()
    conn.close()

def main():
    print("🔄 Populating model performance data...")
    
    # Connect to database
    conn = connect_db()
    print("✅ Connected to database")
    
    # Get MLflow metrics
    mlflow_data = get_mlflow_metrics()
    
    # Calculate business metrics
    print("📊 Calculating business metrics from user events...")
    business_metrics = calculate_business_metrics(conn)
    print(f"✅ Calculated metrics for {len(business_metrics)} model versions")
    
    # Insert data
    if mlflow_data:
        inserted = insert_performance_data(conn, mlflow_data, business_metrics)
        print(f"✅ Inserted {inserted} model performance records")
    
    # Also add current deployment
    conn.close()
    populate_current_deployment()
    
    print("\n✅ Done! Model performance table updated.")
    print("📊 View in Streamlit: http://localhost:8501 -> Model Comparison tab")

if __name__ == "__main__":
    main()
