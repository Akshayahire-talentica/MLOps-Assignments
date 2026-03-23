#!/usr/bin/env python3
"""
Complete MLOps Pipeline Runner
================================

Pipeline Stages:
✅ Check S3 Raw Data
⚙️ PySpark ETL Processing (Pandas-based for simplicity)
🔍 Great Expectations Validation
🔧 Feature Engineering
🤖 Model Training (MLflow)
📊 Drift Detection (scipy KS-Test)
🔄 Auto-Retraining (if drift detected)

Usage:
    python run_complete_mlops_pipeline.py [--skip-s3-check] [--skip-drift]
"""

import os
import sys
import yaml
import logging
import argparse
import pandas as pd
import numpy as np
import boto3
from pathlib import Path
from datetime import datetime
from io import BytesIO
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLOpsPipeline:
    """Complete MLOps Pipeline Controller"""
    
    def __init__(self, config_path='config/data_ingestion_config.yaml'):
        """Initialize pipeline with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.s3_config = self.config.get('s3', {})
        self.bucket_name = self.s3_config.get('bucket_name')
        self.region = self.s3_config.get('region', 'ap-south-1')
        
        # Initialize S3 client if enabled
        if self.s3_config.get('enabled', False):
            try:
                self.s3_client = boto3.client('s3', region_name=self.region)
                logger.info(f"✅ S3 client initialized for bucket: {self.bucket_name}")
            except Exception as e:
                logger.warning(f"⚠️  Could not initialize S3 client: {e}")
                self.s3_client = None
        else:
            self.s3_client = None
            logger.info("S3 disabled in config, using local files only")
        
        # Create necessary directories
        self.data_dir = Path('data')
        self.processed_dir = self.data_dir / 'processed'
        self.features_dir = self.data_dir / 'features'
        self.models_dir = self.data_dir / 'models'
        self.reports_dir = Path('reports')
        
        for dir_path in [self.processed_dir, self.features_dir, self.models_dir, self.reports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.pipeline_stats = {}
    
    def print_stage_header(self, stage_name, icon):
        """Print formatted stage header"""
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"{icon} {stage_name}")
        logger.info("=" * 80)
    
    def stage_1_check_s3_raw_data(self):
        """Stage 1: Check if raw data exists in S3 or locally"""
        self.print_stage_header("STAGE 1: Check S3 Raw Data", "✅")
        
        raw_files = ['movies.dat', 'ratings.dat', 'users.dat']
        
        # Check local files first
        files_exist_locally = all((self.data_dir / 'raw' / f).exists() for f in raw_files)
        
        if files_exist_locally:
            logger.info("✅ All raw data files found locally:")
            for f in raw_files:
                file_path = self.data_dir / 'raw' / f
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"   ✓ {f} ({size_mb:.2f} MB)")
            return True
        
        # If not local, try S3
        if self.s3_client:
            logger.info(f"Checking S3 bucket: s3://{self.bucket_name}/raw/")
            try:
                for file_name in raw_files:
                    s3_key = f"raw/{file_name}"
                    response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                    size_mb = response['ContentLength'] / (1024 * 1024)
                    logger.info(f"   ✓ {file_name} ({size_mb:.2f} MB)")
                
                logger.info("\n🔄 Downloading raw data from S3...")
                for file_name in raw_files:
                    local_path = self.data_dir / 'raw' / file_name
                    s3_key = f"raw/{file_name}"
                    self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
                    logger.info(f"   ✓ Downloaded {file_name}")
                
                logger.info("✅ All raw data files downloaded successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to access S3 data: {e}")
                return False
        else:
            logger.error("❌ No local data found and S3 is not configured")
            return False
    
    def stage_2_run_etl_processing(self):
        """Stage 2: Run Pandas ETL Processing"""
        self.print_stage_header("STAGE 2: PySpark/Pandas ETL Processing", "⚙️")
        
        try:
            # Load raw data
            logger.info("Loading raw data files...")
            
            movies_df = pd.read_csv(
                self.data_dir / 'raw' / 'movies.dat',
                sep='::',
                header=None,
                names=['MovieID', 'Title', 'Genres'],
                encoding='latin1',
                engine='python'
            )
            
            ratings_df = pd.read_csv(
                self.data_dir / 'raw' / 'ratings.dat',
                sep='::',
                header=None,
                names=['UserID', 'MovieID', 'Rating', 'Timestamp'],
                encoding='latin1',
                engine='python'
            )
            
            users_df = pd.read_csv(
                self.data_dir / 'raw' / 'users.dat',
                sep='::',
                header=None,
                names=['UserID', 'Gender', 'Age', 'Occupation', 'ZipCode'],
                encoding='latin1',
                engine='python'
            )
            
            logger.info(f"✓ Loaded: {len(movies_df)} movies, {len(ratings_df):,} ratings, {len(users_df)} users")
            
            # Data quality transformations
            logger.info("Applying data quality transformations...")
            movies_df = movies_df.drop_duplicates(subset=['MovieID'])
            ratings_df = ratings_df[(ratings_df['Rating'] >= 1) & (ratings_df['Rating'] <= 5)]
            users_df = users_df.drop_duplicates(subset=['UserID'])
            
            # Add processing metadata
            processing_timestamp = datetime.now().isoformat()
            for df in [movies_df, ratings_df, users_df]:
                df['processing_timestamp'] = processing_timestamp
            
            # Save processed data as Parquet
            logger.info("Saving processed data as Parquet...")
            movies_df.to_parquet(self.processed_dir / 'movies.parquet', index=False)
            ratings_df.to_parquet(self.processed_dir / 'ratings.parquet', index=False)
            users_df.to_parquet(self.processed_dir / 'users.parquet', index=False)
            
            stats = {
                "movies_count": len(movies_df),
                "ratings_count": len(ratings_df),
                "users_count": len(users_df),
                "processing_timestamp": processing_timestamp
            }
            
            self.pipeline_stats['etl'] = stats
            logger.info(f"✅ ETL completed: {stats}")
            
            return True, stats
        
        except Exception as e:
            logger.error(f"❌ ETL failed: {e}")
            return False, {}
    
    def stage_3_great_expectations_validation(self):
        """Stage 3: Run Great Expectations Validation"""
        self.print_stage_header("STAGE 3: Great Expectations Validation", "🔍")
        
        try:
            # Load processed data
            logger.info("Loading processed data for validation...")
            movies_df = pd.read_parquet(self.processed_dir / 'movies.parquet')
            ratings_df = pd.read_parquet(self.processed_dir / 'ratings.parquet')
            users_df = pd.read_parquet(self.processed_dir / 'users.parquet')
            
            # Basic validation checks
            validations = []
            
            # Movies validations
            validations.append({
                'dataset': 'movies',
                'check': 'no_null_movie_ids',
                'passed': bool(movies_df['MovieID'].notna().all())
            })
            validations.append({
                'dataset': 'movies',
                'check': 'unique_movie_ids',
                'passed': bool(movies_df['MovieID'].is_unique)
            })
            
            # Ratings validations
            validations.append({
                'dataset': 'ratings',
                'check': 'ratings_in_range_1_5',
                'passed': bool(((ratings_df['Rating'] >= 1) & (ratings_df['Rating'] <= 5)).all())
            })
            validations.append({
                'dataset': 'ratings',
                'check': 'no_null_user_ids',
                'passed': bool(ratings_df['UserID'].notna().all())
            })
            
            # Users validations
            validations.append({
                'dataset': 'users',
                'check': 'no_null_user_ids',
                'passed': bool(users_df['UserID'].notna().all())
            })
            validations.append({
                'dataset': 'users',
                'check': 'unique_user_ids',
                'passed': bool(users_df['UserID'].is_unique)
            })
            
            # Check results
            passed = all(v['passed'] for v in validations)
            
            for validation in validations:
                status = "✓" if validation['passed'] else "✗"
                logger.info(f"   {status} {validation['dataset']}: {validation['check']}")
            
            # Save validation report
            validation_report = {
                'timestamp': datetime.now().isoformat(),
                'validations': validations,
                'all_passed': passed
            }
            
            report_path = self.reports_dir / f'validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(report_path, 'w') as f:
                json.dump(validation_report, f, indent=2)
            
            self.pipeline_stats['validation'] = validation_report
            
            if passed:
                logger.info(f"✅ All validations passed ({len(validations)}/{len(validations)})")
                return True
            else:
                failed_count = sum(1 for v in validations if not v['passed'])
                logger.error(f"❌ {failed_count} validations failed")
                return False
        
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            return False
    
    def stage_4_feature_engineering(self):
        """Stage 4: Run Feature Engineering"""
        self.print_stage_header("STAGE 4: Feature Engineering", "🔧")
        
        try:
            # Load processed data
            logger.info("Loading processed data for feature engineering...")
            movies = pd.read_parquet(self.processed_dir / 'movies.parquet')
            ratings = pd.read_parquet(self.processed_dir / 'ratings.parquet')
            users = pd.read_parquet(self.processed_dir / 'users.parquet')
            
            logger.info(f"✓ Loaded: {len(movies)} movies, {len(ratings):,} ratings, {len(users)} users")
            
            # Engineer user features
            logger.info("Engineering user features...")
            user_rating_stats = ratings.groupby('UserID').agg({
                'Rating': ['mean', 'std', 'count']
            }).reset_index()
            user_rating_stats.columns = ['UserID', 'avg_rating', 'rating_std', 'rating_count']
            user_features = users.merge(user_rating_stats, on='UserID', how='left')
            user_features['rating_std'] = user_features['rating_std'].fillna(0)
            
            # Engineer movie features
            logger.info("Engineering movie features...")
            movie_rating_stats = ratings.groupby('MovieID').agg({
                'Rating': ['mean', 'std', 'count']
            }).reset_index()
            movie_rating_stats.columns = ['MovieID', 'avg_rating', 'rating_std', 'rating_count']
            movie_features = movies.merge(movie_rating_stats, on='MovieID', how='left')
            movie_features['rating_std'] = movie_features['rating_std'].fillna(0)
            movie_features['rating_count'] = movie_features['rating_count'].fillna(0)
            
            # Engineer interaction features
            logger.info("Engineering interaction features...")
            interaction_features = ratings.merge(users[['UserID', 'Gender', 'Age']], on='UserID', how='left')
            interaction_features = interaction_features.merge(movies[['MovieID', 'Title', 'Genres']], on='MovieID', how='left')
            
            # Save features
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            user_features.to_parquet(self.features_dir / f'user_features_{timestamp}.parquet', index=False)
            movie_features.to_parquet(self.features_dir / f'movie_features_{timestamp}.parquet', index=False)
            interaction_features.to_parquet(self.features_dir / f'interaction_features_{timestamp}.parquet', index=False)
            
            # Also save as "latest" for easy access
            user_features.to_parquet(self.features_dir / 'user_features_latest.parquet', index=False)
            movie_features.to_parquet(self.features_dir / 'movie_features_latest.parquet', index=False)
            interaction_features.to_parquet(self.features_dir / 'interaction_features_latest.parquet', index=False)
            
            stats = {
                "user_features_shape": list(user_features.shape),
                "movie_features_shape": list(movie_features.shape),
                "interaction_features_shape": list(interaction_features.shape),
                "timestamp": timestamp
            }
            
            logger.info(f"✓ User features: {user_features.shape}")
            logger.info(f"✓ Movie features: {movie_features.shape}")
            logger.info(f"✓ Interaction features: {interaction_features.shape}")
            
            self.pipeline_stats['features'] = stats
            logger.info(f"✅ Feature engineering completed")
            
            return True, timestamp
        
        except Exception as e:
            logger.error(f"❌ Feature engineering failed: {e}")
            return False, None
    
    def stage_5_model_training(self, feature_timestamp):
        """Stage 5: Train ML Model with MLflow"""
        self.print_stage_header("STAGE 5: Model Training (MLflow)", "🤖")
        
        try:
            import mlflow
            import mlflow.sklearn
            from sklearn.model_selection import train_test_split
            from sklearn.decomposition import NMF
            from sklearn.metrics import mean_squared_error, mean_absolute_error
            
            # Set MLflow tracking URI
            mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
            mlflow.set_tracking_uri(mlflow_uri)
            mlflow.set_experiment("mlops-production-pipeline")
            
            logger.info(f"MLflow tracking URI: {mlflow_uri}")
            
            # Load interaction features
            logger.info("Loading interaction features for training...")
            interaction_features = pd.read_parquet(self.features_dir / 'interaction_features_latest.parquet')
            
            logger.info(f"✓ Loaded {len(interaction_features):,} interactions")
            
            # Create user-item matrix
            logger.info("Creating user-item matrix...")
            user_item_matrix = interaction_features.pivot_table(
                index='UserID',
                columns='MovieID',
                values='Rating',
                fill_value=0
            )
            
            logger.info(f"✓ Matrix shape: {user_item_matrix.shape}")
            
            # Train/test split
            train_interaction, test_interaction = train_test_split(
                interaction_features,
                test_size=0.2,
                random_state=42
            )
            
            logger.info(f"✓ Train: {len(train_interaction):,}, Test: {len(test_interaction):,}")
            
            # Start MLflow run
            with mlflow.start_run(run_name=f"production_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Train NMF model
                n_components = 30
                logger.info(f"Training NMF model with {n_components} components...")
                
                nmf_model = NMF(
                    n_components=n_components,
                    init='random',
                    random_state=42,
                    max_iter=200
                )
                
                W = nmf_model.fit_transform(user_item_matrix)
                H = nmf_model.components_
                
                logger.info("✓ Model training completed")
                
                # Evaluate on test set
                logger.info("Evaluating model...")
                test_matrix = test_interaction.pivot_table(
                    index='UserID',
                    columns='MovieID',
                    values='Rating',
                    fill_value=0
                )
                
                test_matrix_aligned = test_matrix.reindex(
                    index=user_item_matrix.index,
                    columns=user_item_matrix.columns,
                    fill_value=0
                )
                
                predictions = nmf_model.transform(test_matrix_aligned) @ H
                
                # Calculate metrics
                actual = []
                predicted = []
                for _, row in test_interaction.iterrows():
                    if row['UserID'] in user_item_matrix.index and row['MovieID'] in user_item_matrix.columns:
                        u_idx = user_item_matrix.index.get_loc(row['UserID'])
                        m_idx = user_item_matrix.columns.get_loc(row['MovieID'])
                        actual.append(row['Rating'])
                        predicted.append(predictions[u_idx, m_idx])
                
                rmse = np.sqrt(mean_squared_error(actual, predicted))
                mae = mean_absolute_error(actual, predicted)
                
                metrics = {
                    'rmse': float(rmse),
                    'mae': float(mae),
                    'n_components': n_components,
                    'reconstruction_error': float(nmf_model.reconstruction_err_)
                }
                
                logger.info(f"✓ RMSE: {rmse:.4f}")
                logger.info(f"✓ MAE: {mae:.4f}")
                logger.info(f"✓ Reconstruction Error: {nmf_model.reconstruction_err_:.4f}")
                
                # Log to MLflow
                mlflow.log_params({
                    'model_type': 'nmf',
                    'n_components': n_components,
                    'feature_timestamp': feature_timestamp,
                    'train_size': len(train_interaction),
                    'test_size': len(test_interaction)
                })
                
                mlflow.log_metrics(metrics)
                
                # Log model artifacts (without registry)
                try:
                    # Save model artifact
                    import tempfile
                    import joblib as jl
                    with tempfile.TemporaryDirectory() as tmpdir:
                        model_file = os.path.join(tmpdir, "nmf_model.pkl")
                        jl.dump(nmf_model, model_file)
                        mlflow.log_artifact(model_file, artifact_path="model")
                    logger.info("✓ Model logged to MLflow")
                except Exception as e:
                    logger.warning(f"⚠️  Could not log model to MLflow: {e}")
                
                # Save model locally
                import joblib
                model_path = self.models_dir / f'nmf_model_{feature_timestamp}.joblib'
                joblib.dump(nmf_model, model_path)
                logger.info(f"✓ Model saved to {model_path}")
                
                self.pipeline_stats['training'] = metrics
                logger.info(f"✅ Model training completed with RMSE: {rmse:.4f}")
                
                return True, metrics
        
        except Exception as e:
            logger.error(f"❌ Model training failed: {e}")
            import traceback
            traceback.print_exc()
            return False, {}
    
    def stage_6_drift_detection(self):
        """Stage 6: Detect Drift using scipy KS-Test"""
        self.print_stage_header("STAGE 6: Drift Detection (scipy KS-Test)", "📊")
        
        try:
            from scipy import stats as scipy_stats
            
            # Get all interaction feature files
            feature_files = sorted(list(self.features_dir.glob('interaction_features_*.parquet')))
            feature_files = [f for f in feature_files if 'latest' not in f.name]
            
            if len(feature_files) < 2:
                logger.warning("⚠️  Not enough feature versions for drift detection (need at least 2)")
                logger.info("Skipping drift detection...")
                return 'skip_retraining', None
            
            baseline_file = feature_files[0]
            current_file = feature_files[-1]
            
            logger.info(f"Baseline: {baseline_file.name}")
            logger.info(f"Current: {current_file.name}")
            
            # Load data
            baseline_data = pd.read_parquet(baseline_file)
            current_data = pd.read_parquet(current_file)
            
            logger.info(f"✓ Baseline shape: {baseline_data.shape}")
            logger.info(f"✓ Current shape: {current_data.shape}")
            
            # Select columns for drift detection
            numeric_cols = ['UserID', 'MovieID', 'Rating', 'Age']
            common_cols = [col for col in numeric_cols if col in baseline_data.columns and col in current_data.columns]
            
            logger.info(f"Testing drift on columns: {common_cols}")
            
            # Sample data for faster computation
            sample_size = min(10000, len(baseline_data), len(current_data))
            baseline_sample = baseline_data[common_cols].sample(sample_size, random_state=42)
            current_sample = current_data[common_cols].sample(sample_size, random_state=42)
            
            # Run KS-test on each column
            n_drifted = 0
            col_results = {}
            
            logger.info("\nDrift Test Results:")
            for col in common_cols:
                ks_stat, p_value = scipy_stats.ks_2samp(
                    baseline_sample[col].dropna(),
                    current_sample[col].dropna()
                )
                drifted = bool(p_value < 0.05)
                if drifted:
                    n_drifted += 1
                
                col_results[col] = {
                    'ks_stat': round(float(ks_stat), 4),
                    'p_value': round(float(p_value), 4),
                    'drifted': drifted
                }
                
                status = "🔴 DRIFT" if drifted else "🟢 OK"
                logger.info(f"   {col}: KS={ks_stat:.4f}, p={p_value:.4f} {status}")
            
            drift_share = n_drifted / len(common_cols) if common_cols else 0.0
            drift_detected = drift_share > 0.3  # Threshold: >30% columns drifted
            
            logger.info(f"\nDrift Summary:")
            logger.info(f"   Drifted columns: {n_drifted}/{len(common_cols)}")
            logger.info(f"   Drift share: {drift_share:.1%}")
            logger.info(f"   Threshold: 30%")
            
            drift_results = {
                'drift_detected': drift_detected,
                'drift_score': drift_share,
                'drifted_columns': n_drifted,
                'total_columns': len(common_cols),
                'column_results': col_results,
                'baseline_file': baseline_file.name,
                'current_file': current_file.name,
                'timestamp': datetime.now().isoformat()
            }
            
            # Save drift report
            report_path = self.reports_dir / f'drift_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(report_path, 'w') as f:
                json.dump(drift_results, f, indent=2)
            
            logger.info(f"✓ Drift report saved to {report_path}")
            
            self.pipeline_stats['drift'] = drift_results
            
            if drift_detected:
                logger.info(f"🔴 ✅ Drift detected! Retraining recommended.")
                return 'trigger_retraining', drift_results
            else:
                logger.info(f"🟢 ✅ No significant drift detected. Skipping retraining.")
                return 'skip_retraining', drift_results
        
        except Exception as e:
            logger.error(f"❌ Drift detection failed: {e}")
            import traceback
            traceback.print_exc()
            return 'skip_retraining', None
    
    def stage_7_auto_retraining(self, feature_timestamp):
        """Stage 7: Auto-Retraining if drift detected"""
        self.print_stage_header("STAGE 7: Auto-Retraining", "🔄")
        
        logger.info("Triggering model retraining due to drift...")
        success, metrics = self.stage_5_model_training(feature_timestamp)
        
        if success:
            logger.info("✅ Retraining completed successfully")
            return True
        else:
            logger.error("❌ Retraining failed")
            return False
    
    def run_complete_pipeline(self, skip_s3_check=False, skip_drift=False):
        """Execute complete MLOps pipeline"""
        logger.info("")
        logger.info("╔" + "═" * 78 + "╗")
        logger.info("║" + " " * 20 + "🚀 MLOps Complete Pipeline 🚀" + " " * 27 + "║")
        logger.info("╚" + "═" * 78 + "╝")
        logger.info("")
        
        start_time = datetime.now()
        
        try:
            # Stage 1: Check S3 Raw Data
            if not skip_s3_check:
                if not self.stage_1_check_s3_raw_data():
                    logger.error("❌ Pipeline failed at Stage 1: S3 data check")
                    return False
            else:
                logger.info("⏭️  Skipping Stage 1: S3 data check")
            
            # Stage 2: ETL Processing
            success, etl_stats = self.stage_2_run_etl_processing()
            if not success:
                logger.error("❌ Pipeline failed at Stage 2: ETL processing")
                return False
            
            # Stage 3: Great Expectations Validation
            if not self.stage_3_great_expectations_validation():
                logger.error("❌ Pipeline failed at Stage 3: Data validation")
                return False
            
            # Stage 4: Feature Engineering
            success, feature_timestamp = self.stage_4_feature_engineering()
            if not success:
                logger.error("❌ Pipeline failed at Stage 4: Feature engineering")
                return False
            
            # Stage 5: Model Training
            success, training_metrics = self.stage_5_model_training(feature_timestamp)
            if not success:
                logger.error("❌ Pipeline failed at Stage 5: Model training")
                return False
            
            # Stage 6: Drift Detection
            if not skip_drift:
                drift_decision, drift_results = self.stage_6_drift_detection()
                
                # Stage 7: Auto-Retraining (conditional)
                if drift_decision == 'trigger_retraining':
                    self.stage_7_auto_retraining(feature_timestamp)
            else:
                logger.info("⏭️  Skipping Stage 6 & 7: Drift detection and auto-retraining")
            
            # Pipeline completion
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.print_stage_header("PIPELINE COMPLETED SUCCESSFULLY", "🎉")
            logger.info(f"Total duration: {duration:.2f} seconds")
            logger.info("")
            logger.info("Pipeline Summary:")
            logger.info(f"   ✅ ETL: {etl_stats.get('ratings_count', 0):,} ratings processed")
            logger.info(f"   ✅ Validation: All checks passed")
            logger.info(f"   ✅ Features: {len(self.pipeline_stats.get('features', {}))} feature sets created")
            logger.info(f"   ✅ Training: RMSE = {training_metrics.get('rmse', 0):.4f}")
            if not skip_drift and drift_results:
                logger.info(f"   ✅ Drift: {drift_results.get('drift_score', 0):.1%} columns drifted")
            logger.info("")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Pipeline failed with exception: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run complete MLOps pipeline')
    parser.add_argument('--skip-s3-check', action='store_true', help='Skip S3 raw data check')
    parser.add_argument('--skip-drift', action='store_true', help='Skip drift detection and auto-retraining')
    args = parser.parse_args()
    
    pipeline = MLOpsPipeline()
    success = pipeline.run_complete_pipeline(
        skip_s3_check=args.skip_s3_check,
        skip_drift=args.skip_drift
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
