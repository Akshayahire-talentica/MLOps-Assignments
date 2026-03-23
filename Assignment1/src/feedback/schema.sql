-- ============================================================
-- ML FEEDBACK LOOP DATABASE SCHEMA
-- ============================================================
-- Production-ready schema for collecting user feedback events
-- and building training datasets for continuous learning.
--
-- Database: PostgreSQL (mlops_db)
-- ============================================================

-- ============================================================
-- 1. RECOMMENDATION LOGS
-- ============================================================
-- Tracks every recommendation served to users

CREATE TABLE IF NOT EXISTS recommendation_logs (
    id SERIAL PRIMARY KEY,
    recommendation_id UUID NOT NULL UNIQUE,
    user_id INTEGER NOT NULL,
    item_ids INTEGER[] NOT NULL,  -- Array of recommended item IDs
    scores FLOAT[] ,              -- Prediction scores for each item
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_run_id VARCHAR(100),    -- MLflow run ID
    is_synthetic BOOLEAN DEFAULT FALSE,
    context JSONB,                -- Additional context (time of day, device, etc.)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rec_user_id ON recommendation_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_rec_created_at ON recommendation_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_rec_model_version ON recommendation_logs(model_version);
CREATE INDEX IF NOT EXISTS idx_rec_synthetic ON recommendation_logs(is_synthetic);

-- ============================================================
-- 2. USER EVENTS
-- ============================================================
-- Captures all user interaction events

CREATE TABLE IF NOT EXISTS user_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,  -- impression, click, watch, feedback
    user_id INTEGER NOT NULL,
    item_id INTEGER,
    recommendation_id UUID,           -- Links back to recommendation_logs
    
    -- Event-specific data
    position INTEGER,                 -- Position in recommendation list (for clicks)
    watch_duration_seconds INTEGER,   -- Actual watch time
    total_duration_seconds INTEGER,   -- Total movie/content duration
    completion_rate FLOAT,            -- watch_duration / total_duration
    
    feedback_type VARCHAR(50),        -- like, dislike, not_interested, rating
    rating FLOAT,                     -- Explicit rating (0-5)
    
    -- Metadata
    is_synthetic BOOLEAN DEFAULT FALSE,
    session_id UUID,
    device_type VARCHAR(50),
    context JSONB,                    -- Additional context
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_recommendation
        FOREIGN KEY (recommendation_id)
        REFERENCES recommendation_logs(recommendation_id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_event_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_event_item_id ON user_events(item_id);
CREATE INDEX IF NOT EXISTS idx_event_type ON user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_event_rec_id ON user_events(recommendation_id);
CREATE INDEX IF NOT EXISTS idx_event_created_at ON user_events(created_at);
CREATE INDEX IF NOT EXISTS idx_event_synthetic ON user_events(is_synthetic);

-- ============================================================
-- 3. TRAINING LABELS
-- ============================================================
-- Pre-computed labels from events for model training

CREATE TABLE IF NOT EXISTS training_labels (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    label FLOAT NOT NULL,             -- Engagement score (0-5)
    
    -- Features derived from events
    click_count INTEGER DEFAULT 0,
    watch_count INTEGER DEFAULT 0,
    avg_completion_rate FLOAT,
    like_count INTEGER DEFAULT 0,
    dislike_count INTEGER DEFAULT 0,
    
    -- Metadata
    recommendation_id UUID,
    model_version VARCHAR(50),        -- Model that made the recommendation
    is_synthetic BOOLEAN DEFAULT FALSE,
    label_date DATE NOT NULL,         -- When label was generated
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (user_id, item_id, label_date)
);

CREATE INDEX IF NOT EXISTS idx_label_user_id ON training_labels(user_id);
CREATE INDEX IF NOT EXISTS idx_label_item_id ON training_labels(item_id);
CREATE INDEX IF NOT EXISTS idx_label_date ON training_labels(label_date);
CREATE INDEX IF NOT EXISTS idx_label_synthetic ON training_labels(is_synthetic);

-- ============================================================
-- 4. MODEL PERFORMANCE
-- ============================================================
-- Tracks model performance metrics over time

CREATE TABLE IF NOT EXISTS model_performance (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_run_id VARCHAR(100),
    stage VARCHAR(50),                -- staging, production, archived
    
    -- Model metrics
    rmse FLOAT,
    mae FLOAT,
    mse FLOAT,
    r2_score FLOAT,
    
    -- Business metrics (computed from events)
    ctr FLOAT,                        -- Click-through rate
    watch_rate FLOAT,                 -- Watch after click rate
    avg_watch_completion FLOAT,       -- Average completion rate
    avg_engagement_score FLOAT,       -- Average label value
    
    -- System metrics
    p50_latency_ms FLOAT,
    p95_latency_ms FLOAT,
    p99_latency_ms FLOAT,
    error_rate FLOAT,
    
    -- Data stats
    training_samples_count INTEGER,
    validation_samples_count INTEGER,
    synthetic_ratio FLOAT,            -- % of synthetic data
    
    evaluation_date DATE NOT NULL,
    is_production BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_perf_model ON model_performance(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_perf_stage ON model_performance(stage);
CREATE INDEX IF NOT EXISTS idx_perf_date ON model_performance(evaluation_date);

-- ============================================================
-- 5. RETRAINING TRIGGERS
-- ============================================================
-- Logs when and why retraining was triggered

CREATE TABLE IF NOT EXISTS retraining_triggers (
    id SERIAL PRIMARY KEY,
    trigger_type VARCHAR(50) NOT NULL,  -- scheduled, performance_drop, drift, manual
    trigger_reason TEXT,
    
    -- Metrics at trigger time
    current_model_version VARCHAR(50),
    events_since_last_train INTEGER,
    ctr_current FLOAT,
    ctr_baseline FLOAT,
    rmse_current FLOAT,
    rmse_baseline FLOAT,
    
    -- Training job info
    airflow_dag_id VARCHAR(100),
    airflow_run_id VARCHAR(100),
    training_started_at TIMESTAMP,
    training_completed_at TIMESTAMP,
    training_status VARCHAR(50),      -- pending, running, success, failed
    
    new_model_version VARCHAR(50),
    new_model_run_id VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trigger_type ON retraining_triggers(trigger_type);
CREATE INDEX IF NOT EXISTS idx_trigger_status ON retraining_triggers(training_status);

-- ============================================================
-- 6. A/B TEST RESULTS
-- ============================================================
-- Tracks canary deployment performance

CREATE TABLE IF NOT EXISTS ab_test_results (
    id SERIAL PRIMARY KEY,
    experiment_name VARCHAR(100) NOT NULL,
    
    -- Model variants
    control_model_version VARCHAR(50) NOT NULL,
    treatment_model_version VARCHAR(50) NOT NULL,
    
    -- Traffic split
    traffic_percentage_treatment INTEGER DEFAULT 10,
    
    -- Performance comparison
    control_ctr FLOAT,
    treatment_ctr FLOAT,
    ctr_lift_percent FLOAT,
    
    control_watch_rate FLOAT,
    treatment_watch_rate FLOAT,
    
    control_avg_engagement FLOAT,
    treatment_avg_engagement FLOAT,
    
    -- Statistical significance
    sample_size INTEGER,
    p_value FLOAT,
    is_significant BOOLEAN,
    
    -- Decision
    test_status VARCHAR(50),          -- running, passed, failed, rolled_back
    decision VARCHAR(50),             -- promote, rollback, continue
    decision_reason TEXT,
    
    test_start_date TIMESTAMP NOT NULL,
    test_end_date TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ab_experiment ON ab_test_results(experiment_name);
CREATE INDEX IF NOT EXISTS idx_ab_status ON ab_test_results(test_status);

-- ============================================================
-- HELPER VIEWS
-- ============================================================

-- View: Recent CTR by model version
CREATE OR REPLACE VIEW v_recent_ctr AS
SELECT 
    r.model_version,
    DATE(r.created_at) as date,
    COUNT(DISTINCT r.recommendation_id) as impressions,
    COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.recommendation_id END) as clicks,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'click' THEN e.recommendation_id END)::NUMERIC / 
        NULLIF(COUNT(DISTINCT r.recommendation_id), 0) * 100, 
        2
    ) as ctr_percent
FROM recommendation_logs r
LEFT JOIN user_events e ON r.recommendation_id = e.recommendation_id 
    AND e.event_type = 'click'
WHERE r.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY r.model_version, DATE(r.created_at)
ORDER BY date DESC, model_version;

-- View: User engagement funnel
CREATE OR REPLACE VIEW v_engagement_funnel AS
SELECT 
    DATE(created_at) as date,
    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) as impressions,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN event_type = 'watch' THEN 1 ELSE 0 END) as watches,
    SUM(CASE WHEN event_type = 'feedback' AND feedback_type = 'like' THEN 1 ELSE 0 END) as likes,
    
    ROUND(
        SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0) * 100,
        2
    ) as ctr,
    
    ROUND(
        SUM(CASE WHEN event_type = 'watch' THEN 1 ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0) * 100,
        2
    ) as watch_rate
FROM user_events
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

-- Composite index for label generation queries
CREATE INDEX IF NOT EXISTS idx_events_label_generation 
ON user_events(user_id, item_id, event_type, created_at) 
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';

-- Index for time-series queries
CREATE INDEX IF NOT EXISTS idx_events_timeseries 
ON user_events(created_at DESC, event_type);

-- ============================================================
-- CLEANUP POLICIES (Optional)
-- ============================================================

-- Archive old events (keep recent 90 days hot, rest cold storage)
-- Run via cron/Airflow monthly
-- 
-- CREATE TABLE user_events_archive (LIKE user_events INCLUDING ALL);
-- 
-- INSERT INTO user_events_archive 
-- SELECT * FROM user_events 
-- WHERE created_at < CURRENT_DATE - INTERVAL '90 days';
-- 
-- DELETE FROM user_events 
-- WHERE created_at < CURRENT_DATE - INTERVAL '90 days';

COMMENT ON TABLE recommendation_logs IS 'Logs of all recommendations served to users';
COMMENT ON TABLE user_events IS 'All user interaction events (impressions, clicks, watches, feedback)';
COMMENT ON TABLE training_labels IS 'Pre-computed training labels derived from user events';
COMMENT ON TABLE model_performance IS 'Model evaluation metrics tracked over time';
COMMENT ON TABLE retraining_triggers IS 'Audit log of retraining triggers and outcomes';
COMMENT ON TABLE ab_test_results IS 'A/B test results for canary deployments';
