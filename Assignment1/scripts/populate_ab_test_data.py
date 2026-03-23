#!/usr/bin/env python3
"""
Populate Sample A/B Test Data
Creates realistic A/B test scenarios for demo purposes
"""

import os
import sys
import psycopg2
from datetime import datetime, timedelta
import random

def connect_db():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB', 'mlops_db'),
        user=os.getenv('POSTGRES_USER', 'mlops'),
        password=os.getenv('POSTGRES_PASSWORD', 'mlops123')
    )

def create_ab_test_scenarios():
    """Create realistic A/B test scenarios"""
    
    scenarios = [
        {
            'experiment_name': 'NMF_Tuning_Experiment_v1',
            'control_model_version': 'mock_v1',
            'treatment_model_version': 'nmf_v2_30comp',
            'traffic_percentage_treatment': 10,
            'control_ctr': 89.5,
            'treatment_ctr': 94.2,
            'ctr_lift_percent': 5.2,
            'control_watch_rate': 55.6,
            'treatment_watch_rate': 61.3,
            'control_avg_engagement': 2.08,
            'treatment_avg_engagement': 2.45,
            'sample_size': 1500,
            'p_value': 0.032,
            'is_significant': True,
            'test_status': 'completed',
            'decision': 'promote',
            'decision_reason': 'Treatment shows 5.2% CTR lift with statistical significance (p=0.032). Watch rate improved by 5.7 points. Recommended for promotion to champion.',
            'test_start_date': datetime.now() - timedelta(days=7),
            'test_end_date': datetime.now() - timedelta(days=5)
        },
        {
            'experiment_name': 'Deep_Learning_Hybrid_Test',
            'control_model_version': 'nmf_v2_30comp',
            'treatment_model_version': 'hybrid_dl_v1',
            'traffic_percentage_treatment': 15,
            'control_ctr': 94.2,
            'treatment_ctr': 91.8,
            'ctr_lift_percent': -2.5,
            'control_watch_rate': 61.3,
            'treatment_watch_rate': 58.7,
            'control_avg_engagement': 2.45,
            'treatment_avg_engagement': 2.31,
            'sample_size': 2200,
            'p_value': 0.089,
            'is_significant': False,
            'test_status': 'completed',
            'decision': 'rollback',
            'decision_reason': 'Treatment underperformed champion by 2.5% CTR. Watch rate decreased. Not statistically significant but trend is negative. Rolled back to champion.',
            'test_start_date': datetime.now() - timedelta(days=4),
            'test_end_date': datetime.now() - timedelta(days=2)
        },
        {
            'experiment_name': 'SVD_vs_NMF_Showdown',
            'control_model_version': 'nmf_v2_30comp', 
            'treatment_model_version': 'svd_v1_50factors',
            'traffic_percentage_treatment': 20,
            'control_ctr': 94.2,
            'treatment_ctr': 95.1,
            'ctr_lift_percent': 1.0,
            'control_watch_rate': 61.3,
            'treatment_watch_rate': 62.8,
            'control_avg_engagement': 2.45,
            'treatment_avg_engagement': 2.52,
            'sample_size': 3100,
            'p_value': 0.156,
            'is_significant': False,
            'test_status': 'running',
            'decision': 'continue',
            'decision_reason': 'Early results show slight improvement but not statistically significant yet. Continuing test to collect more data. Current sample size: 3,100.',
            'test_start_date': datetime.now() - timedelta(days=1),
            'test_end_date': None
        },
        {
            'experiment_name': 'Content_Filtering_Enhancement',
            'control_model_version': 'nmf_v2_30comp',
            'treatment_model_version': 'nmf_v3_content_boost',
            'traffic_percentage_treatment': 25,
            'control_ctr': 94.2,
            'treatment_ctr': 96.8,
            'ctr_lift_percent': 2.8,
            'control_watch_rate': 61.3,
            'treatment_watch_rate': 63.9,
            'control_avg_engagement': 2.45, 
            'treatment_avg_engagement': 2.61,
            'sample_size': 4500,
            'p_value': 0.018,
            'is_significant': True,
            'test_status': 'scaling',
            'decision': 'promote',
            'decision_reason': 'Strong performance with 2.8% CTR lift and statistical significance. Scaling from 25% to 50% traffic before full promotion.',
            'test_start_date': datetime.now() - timedelta(hours=36),
            'test_end_date': None
        },
        {
            'experiment_name': 'Cold_Start_Optimization',
            'control_model_version': 'nmf_v2_30comp',
            'treatment_model_version': 'nmf_v2_cold_start',
            'traffic_percentage_treatment': 10,
            'control_ctr': 94.2,
            'treatment_ctr': 92.1,
            'ctr_lift_percent': -2.2,
            'control_watch_rate': 61.3,
            'treatment_watch_rate': 64.5,
            'control_avg_engagement': 2.45,
            'treatment_avg_engagement': 2.58,
            'sample_size': 850,
            'p_value': 0.245,
            'is_significant': False,
            'test_status': 'running',
            'decision': 'continue',
            'decision_reason': 'Mixed results: CTR slightly down but watch rate significantly up. May indicate better quality recommendations. Need more data to assess trade-off.',
            'test_start_date': datetime.now() - timedelta(hours=12),
            'test_end_date': None
        }
    ]
    
    return scenarios

def populate_ab_tests(conn):
    """Insert A/B test data into database"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO ab_test_results (
            experiment_name,
            control_model_version,
            treatment_model_version,
            traffic_percentage_treatment,
            control_ctr,
            treatment_ctr,
            ctr_lift_percent,
            control_watch_rate,
            treatment_watch_rate,
            control_avg_engagement,
            treatment_avg_engagement,
            sample_size,
            p_value,
            is_significant,
            test_status,
            decision,
            decision_reason,
            test_start_date,
            test_end_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
    """
    
    scenarios = create_ab_test_scenarios()
    inserted = 0
    
    for scenario in scenarios:
        try:
            cursor.execute(insert_query, (
                scenario['experiment_name'],
                scenario['control_model_version'],
                scenario['treatment_model_version'],
                scenario['traffic_percentage_treatment'],
                scenario['control_ctr'],
                scenario['treatment_ctr'],
                scenario['ctr_lift_percent'],
                scenario['control_watch_rate'],
                scenario['treatment_watch_rate'],
                scenario['control_avg_engagement'],
                scenario['treatment_avg_engagement'],
                scenario['sample_size'],
                scenario['p_value'],
                scenario['is_significant'],
                scenario['test_status'],
                scenario['decision'],
                scenario['decision_reason'],
                scenario['test_start_date'],
                scenario['test_end_date']
            ))
            inserted += 1
            print(f"✅ Added: {scenario['experiment_name']}")
        except Exception as e:
            print(f"⚠️ Error inserting {scenario['experiment_name']}: {e}")
    
    conn.commit()
    cursor.close()
    
    return inserted

def main():
    print("🔄 Populating A/B test data...")
    
    conn = connect_db()
    print("✅ Connected to database")
    
    inserted = populate_ab_tests(conn)
    print(f"\n✅ Inserted {inserted} A/B test scenarios")
    
    conn.close()
    
    print("\n📊 A/B Test Scenarios Created:")
    print("  1. ✅ NMF Tuning - PROMOTED (5.2% CTR lift)")
    print("  2. ❌ Deep Learning Hybrid - ROLLED BACK (performance degraded)")
    print("  3. ⏳ SVD vs NMF - RUNNING (collecting more data)")
    print("  4. 🚀 Content Filtering - SCALING (strong performance)")
    print("  5. ⏳ Cold Start Optimization - RUNNING (mixed results)")
    
    print("\n📊 View in Streamlit: http://localhost:8501 -> Model Comparison tab")

if __name__ == "__main__":
    main()
