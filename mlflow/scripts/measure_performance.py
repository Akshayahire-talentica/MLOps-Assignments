#!/usr/bin/env python3
"""
Performance Measurement & Comparison Tool
==========================================

Measures and compares:
- Model prediction metrics (RMSE, MAE, R2)
- Business metrics (CTR, Watch Rate, Engagement)
- System metrics (Latency, Throughput, Error Rate)
- A/B test results

Usage:
    python3 measure_performance.py --model-version v1
    python3 measure_performance.py --compare v1 v2
    python3 measure_performance.py --benchmark --duration 60
"""

import argparse
import psycopg2
import time
import requests
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# DATABASE CONNECTION
# ============================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mlops_db',
    'user': 'mlops',
    'password': 'mlops123'
}

API_URL = "http://localhost:8000"
EVENT_URL = "http://localhost:8002"

# ============================================================
# MODEL PERFORMANCE METRICS
# ============================================================

def get_model_metrics(model_version: str = None) -> List[Dict]:
    """Get ML model performance metrics from database"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        model_version,
        rmse,
        mae,
        r2_score,
        ctr,
        watch_rate,
        avg_engagement_score,
        p95_latency_ms,
        evaluation_date
    FROM model_performance
    """
    
    if model_version:
        query += f" WHERE model_version = '{model_version}'"
    
    query += " ORDER BY evaluation_date DESC LIMIT 10"
    
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return results


def get_business_metrics(model_version: str = None, days: int = 7) -> Dict:
    """Calculate business metrics from events"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    WITH events AS (
        SELECT 
            r.model_version,
            r.recommendation_id,
            COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.id END) as clicks,
            COUNT(DISTINCT CASE WHEN e.event_type = 'watch' THEN e.id END) as watches,
            COUNT(DISTINCT CASE WHEN e.event_type = 'feedback' AND e.feedback_type IN ('like', 'rating') THEN e.id END) as positive_feedback,
            AVG(CASE WHEN e.event_type = 'watch' THEN e.completion_rate END) as avg_completion
        FROM recommendation_logs r
        LEFT JOIN user_events e ON r.recommendation_id = e.recommendation_id
        WHERE r.created_at > NOW() - INTERVAL '%s days'
    """
    
    if model_version:
        query += f" AND r.model_version = '{model_version}'"
    
    query += """
        GROUP BY r.model_version, r.recommendation_id
    )
    SELECT 
        model_version,
        COUNT(*) as total_recommendations,
        SUM(CASE WHEN clicks > 0 THEN 1 ELSE 0 END) as clicked_recommendations,
        SUM(clicks) as total_clicks,
        SUM(watches) as total_watches,
        SUM(positive_feedback) as total_positive_feedback,
        ROUND(AVG(avg_completion)::numeric, 4) as avg_watch_completion,
        ROUND((SUM(CASE WHEN clicks > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) * 100)::numeric, 2) as ctr_percent,
        ROUND((SUM(watches)::float / NULLIF(SUM(clicks), 0) * 100)::numeric, 2) as watch_rate_percent
    FROM events
    GROUP BY model_version
    """
    
    cursor.execute(query % days)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return results[0] if results else {}


# ============================================================
# SYSTEM PERFORMANCE BENCHMARKING
# ============================================================

def benchmark_latency(num_requests: int = 100, concurrent: int = 10) -> Dict:
    """Benchmark API latency and throughput"""
    print(f"\n🔧 Benchmarking with {num_requests} requests ({concurrent} concurrent)...")
    
    latencies = []
    errors = 0
    
    def make_request(i):
        start = time.time()
        try:
            response = requests.get(f"{API_URL}/recommend?user_id={i % 1000}", timeout=5)
            latency = (time.time() - start) * 1000  # ms
            if response.status_code == 200:
                return latency, False
            else:
                return latency, True
        except Exception as e:
            return None, True
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=concurrent) as executor:
        futures = [executor.submit(make_request, i) for i in range(num_requests)]
        
        for future in as_completed(futures):
            latency, is_error = future.result()
            if latency:
                latencies.append(latency)
            if is_error:
                errors += 1
    
    duration = time.time() - start_time
    
    if not latencies:
        return {
            'error': 'All requests failed',
            'error_rate': 1.0
        }
    
    latencies.sort()
    
    return {
        'requests': num_requests,
        'duration_seconds': round(duration, 2),
        'throughput_rps': round(num_requests / duration, 2),
        'latency_min_ms': round(min(latencies), 2),
        'latency_max_ms': round(max(latencies), 2),
        'latency_mean_ms': round(statistics.mean(latencies), 2),
        'latency_median_ms': round(statistics.median(latencies), 2),
        'latency_p95_ms': round(latencies[int(len(latencies) * 0.95)], 2),
        'latency_p99_ms': round(latencies[int(len(latencies) * 0.99)], 2),
        'error_count': errors,
        'error_rate': round(errors / num_requests, 4)
    }


def benchmark_event_collector(num_events: int = 1000) -> Dict:
    """Benchmark event collector throughput"""
    print(f"\n🔧 Benchmarking Event Collector with {num_events} events...")
    
    latencies = []
    errors = 0
    
    start_time = time.time()
    
    for i in range(num_events):
        event = {
            "user_id": i % 100,
            "item_id": i % 500,
            "recommendation_id": f"bench-{i}",
            "position": i % 10,
            "is_synthetic": True
        }
        
        start = time.time()
        try:
            response = requests.post(f"{EVENT_URL}/events/click", json=event, timeout=5)
            latency = (time.time() - start) * 1000
            latencies.append(latency)
            if response.status_code != 200:
                errors += 1
        except:
            errors += 1
    
    duration = time.time() - start_time
    
    return {
        'events': num_events,
        'duration_seconds': round(duration, 2),
        'throughput_eps': round(num_events / duration, 2),
        'avg_latency_ms': round(statistics.mean(latencies), 2),
        'p95_latency_ms': round(sorted(latencies)[int(len(latencies) * 0.95)], 2),
        'error_count': errors,
        'error_rate': round(errors / num_events, 4)
    }


# ============================================================
# COMPARISON REPORTS
# ============================================================

def compare_models(model_a: str, model_b: str, days: int = 7):
    """Compare two model versions"""
    print(f"\n" + "="*70)
    print(f"📊 COMPARING MODELS: {model_a} vs {model_b}")
    print("="*70)
    
    # Business metrics comparison
    print(f"\n📈 Business Metrics (Last {days} days)")
    print("-" * 70)
    
    metrics_a = get_business_metrics(model_a, days)
    metrics_b = get_business_metrics(model_b, days)
    
    if not metrics_a or not metrics_b:
        print("⚠️  Insufficient data for comparison")
        return
    
    def compare(metric_name, format_str="{:.2f}"):
        val_a = metrics_a.get(metric_name, 0)
        val_b = metrics_b.get(metric_name, 0)
        
        if val_a == 0:
            diff = 0
            pct = 0
        else:
            diff = val_b - val_a
            pct = (diff / val_a) * 100
        
        symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
        color = "🟢" if diff > 0 else "🔴" if diff < 0 else "⚪"
        
        print(f"{metric_name:30} | {model_a:12} | {model_b:12} | {symbol} {color}")
        print(f"{'':30} | {format_str.format(val_a):12} | {format_str.format(val_b):12} | {pct:+.1f}%")
    
    compare("total_recommendations", "{:.0f}")
    compare("ctr_percent", "{:.2f}")
    compare("watch_rate_percent", "{:.2f}")
    compare("avg_watch_completion", "{:.4f}")
    compare("total_positive_feedback", "{:.0f}")
    
    # Statistical significance test
    print(f"\n📊 Statistical Analysis")
    print("-" * 70)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT 
        r.model_version,
        COUNT(DISTINCT r.recommendation_id) as sample_size,
        COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN r.recommendation_id END)::float / 
            NULLIF(COUNT(DISTINCT r.recommendation_id), 0) as ctr
    FROM recommendation_logs r
    LEFT JOIN user_events e ON r.recommendation_id = e.recommendation_id
    WHERE r.model_version IN (%s, %s)
    AND r.created_at > NOW() - INTERVAL '%s days'
    GROUP BY r.model_version
    """, (model_a, model_b, days))
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if len(results) == 2:
        n1, ctr1 = results[0][1], results[0][2]
        n2, ctr2 = results[1][1], results[1][2]
        
        # Z-test for proportions
        p_pooled = (n1 * ctr1 + n2 * ctr2) / (n1 + n2)
        se = (p_pooled * (1 - p_pooled) * (1/n1 + 1/n2)) ** 0.5
        z_score = (ctr2 - ctr1) / se if se > 0 else 0
        
        print(f"Sample sizes: {model_a}={n1}, {model_b}={n2}")
        print(f"Z-score: {z_score:.3f}")
        
        if abs(z_score) > 1.96:
            print(f"✓ Statistically significant at 95% confidence (|z| > 1.96)")
        else:
            print(f"⚠️  Not statistically significant at 95% confidence")
    
    # Winner declaration
    print(f"\n🏆 WINNER")
    print("-" * 70)
    
    ctr_winner = model_b if metrics_b.get('ctr_percent', 0) > metrics_a.get('ctr_percent', 0) else model_a
    watch_winner = model_b if metrics_b.get('watch_rate_percent', 0) > metrics_a.get('watch_rate_percent', 0) else model_a
    
    print(f"Best CTR:        {ctr_winner}")
    print(f"Best Watch Rate: {watch_winner}")
    
    if ctr_winner == watch_winner:
        print(f"\n✓ Clear winner: {ctr_winner}")
    else:
        print(f"\n⚠️  Mixed results - consider business priorities")


def generate_performance_report():
    """Generate comprehensive performance report"""
    print("\n" + "="*70)
    print("📊 COMPREHENSIVE PERFORMANCE REPORT")
    print("="*70)
    
    # All models overview
    print(f"\n📈 All Models - Business Metrics")
    print("-" * 70)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT 
        r.model_version,
        COUNT(DISTINCT r.recommendation_id) as recommendations,
        COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.recommendation_id END) as clicks,
        ROUND((100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.recommendation_id END)::float / 
              NULLIF(COUNT(DISTINCT r.recommendation_id), 0))::numeric, 2) as ctr_percent
    FROM recommendation_logs r
    LEFT JOIN user_events e ON r.recommendation_id = e.recommendation_id
    GROUP BY r.model_version
    ORDER BY ctr_percent DESC
    """)
    
    print(f"{'Model Version':20} {'Recommendations':>15} {'Clicks':>10} {'CTR %':>10}")
    print("-" * 70)
    
    for row in cursor.fetchall():
        print(f"{row[0]:20} {row[1]:>15} {row[2]:>10} {row[3]:>10}")
    
    # Engagement funnel
    print(f"\n🔄 Engagement Funnel")
    print("-" * 70)
    
    cursor.execute("""
    SELECT 
        COUNT(DISTINCT recommendation_id) as impressions,
        COUNT(DISTINCT CASE WHEN event_type = 'click' THEN recommendation_id END) as clicks,
        COUNT(DISTINCT CASE WHEN event_type = 'watch' THEN recommendation_id END) as watches,
        COUNT(DISTINCT CASE WHEN event_type = 'feedback' THEN user_id END) as feedbacks
    FROM user_events
    """)
    
    row = cursor.fetchone()
    impressions, clicks, watches, feedbacks = row
    
    print(f"Impressions → Clicks → Watches → Feedback")
    print(f"{impressions:>11} → {clicks:>6} → {watches:>7} → {feedbacks:>8}")
    
    if impressions > 0:
        ctr = (clicks / impressions) * 100
        watch_rate = (watches / clicks) * 100 if clicks > 0 else 0
        feedback_rate = (feedbacks / watches) * 100 if watches > 0 else 0
        
        print(f"{'':>11}   {ctr:>5.1f}%   {watch_rate:>6.1f}%   {feedback_rate:>7.1f}%")
    
    # Training data quality
    print(f"\n📊 Training Data Quality")
    print("-" * 70)
    
    cursor.execute("""
    SELECT 
        COUNT(*) as total_labels,
        MIN(label) as min_label,
        MAX(label) as max_label,
        ROUND(AVG(label)::numeric, 2) as avg_label,
        ROUND(STDDEV(label)::numeric, 2) as std_label,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT item_id) as unique_items
    FROM training_labels
    """)
    
    row = cursor.fetchone()
    if row and row[0] > 0:
        print(f"Total labels: {row[0]}")
        print(f"Label range: {row[1]} - {row[2]} (avg: {row[3]}, std: {row[4]})")
        print(f"Coverage: {row[5]} users, {row[6]} items")
    
    cursor.close()
    conn.close()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Performance Measurement & Comparison')
    parser.add_argument('--model-version', help='Specific model version to analyze')
    parser.add_argument('--compare', nargs=2, metavar=('MODEL_A', 'MODEL_B'), 
                       help='Compare two model versions')
    parser.add_argument('--benchmark', action='store_true', 
                       help='Run system performance benchmark')
    parser.add_argument('--duration', type=int, default=60,
                       help='Benchmark duration in seconds (for load test)')
    parser.add_argument('--requests', type=int, default=100,
                       help='Number of requests for latency benchmark')
    parser.add_argument('--concurrent', type=int, default=10,
                       help='Concurrent requests for benchmark')
    parser.add_argument('--days', type=int, default=7,
                       help='Number of days to analyze')
    parser.add_argument('--report', action='store_true',
                       help='Generate comprehensive performance report')
    
    args = parser.parse_args()
    
    if args.report:
        generate_performance_report()
    
    elif args.compare:
        compare_models(args.compare[0], args.compare[1], args.days)
    
    elif args.model_version:
        print(f"\n📊 Performance for model: {args.model_version}")
        metrics = get_business_metrics(args.model_version, args.days)
        print(json.dumps(metrics, indent=2))
    
    elif args.benchmark:
        results = benchmark_latency(args.requests, args.concurrent)
        print(f"\n📊 API Latency Benchmark Results:")
        print(json.dumps(results, indent=2))
        
        # Optional: benchmark event collector
        # event_results = benchmark_event_collector(args.requests)
        # print(f"\n📊 Event Collector Benchmark Results:")
        # print(json.dumps(event_results, indent=2))
    
    else:
        generate_performance_report()


if __name__ == '__main__':
    main()
