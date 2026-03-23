"""
Real MLOps Integration - Connects to Production Pipeline
Uses: S3 → DVC → PySpark → Great Expectations → Features → MLflow
"""

import os
import pandas as pd
import requests
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealMLOpsIntegration:
    """Integration with actual MLOps pipeline components"""
    
    def __init__(self):
        """Initialize with environment-aware service URLs"""
        
        # Detect if running in Docker
        self.in_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_ENV') == 'true'
        
        # Service URLs
        if self.in_docker:
            # Inside Kubernetes - use service names
            self.api_url = os.getenv("API_URL", "http://mlops-api:8000")
            self.mlflow_url = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
            self.airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-web:8080")
            self.data_dir = Path("/app/data")
        else:
            # Local development - use localhost or external NodePort
            self.api_url = os.getenv("API_URL", "http://localhost:8000")
            self.mlflow_url = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            # Allow external access via NodePort (e.g., http://13.127.21.160:30080)
            self.airflow_url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
            self.data_dir = Path("data")
        
        # S3/AWS Config
        self.s3_bucket = os.getenv("S3_BUCKET", "mlops-movielens-poc")
        self.aws_region = os.getenv("AWS_REGION", "ap-south-1")
        
        # Airflow auth - Updated to match actual Airflow configuration (admin/admin123)
        airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
        airflow_pass = os.getenv("AIRFLOW_PASSWORD", "admin123")
        self.airflow_auth = (airflow_user, airflow_pass)
        
        logger.info(f"MLOps Integration initialized:")
        logger.info(f"  Environment: {'Docker' if self.in_docker else 'Local'}")
        logger.info(f"  API: {self.api_url}")
        logger.info(f"  MLflow: {self.mlflow_url}")
        logger.info(f"  Airflow: {self.airflow_url}")
        logger.info(f"  Data dir: {self.data_dir}")
    
    # ==================== DATA LOADING ====================
    
    def load_processed_movies(self) -> pd.DataFrame:
        """Load processed movies - try local parquet first, then S3"""
        # ── 1. Local processed movies file (fast) ────────────────────────────
        try:
            processed_dir = self.data_dir / "processed"
            
            # Check for movies.parquet directly
            movies_parquet = processed_dir / "movies.parquet"
            if movies_parquet.exists():
                df = pd.read_parquet(movies_parquet)
                logger.info(f"Loaded {len(df)} movies from local: {movies_parquet}")
                return df
            
            # Check for timestamped parquet files
            parquet_files = sorted(processed_dir.glob("movies_*.parquet"), reverse=True)
            if parquet_files:
                df = pd.read_parquet(parquet_files[0])
                logger.info(f"Loaded {len(df)} movies from local: {parquet_files[0].name}")
                return df
                
        except Exception as local_err:
            logger.warning(f"Local movies load failed, falling back to S3: {local_err}")
        
        # ── 2. S3 fallback ────────────────────────────────────────────────────
        try:
            import boto3
            from io import BytesIO
            
            s3_client = boto3.client('s3', region_name=self.aws_region)
            
            # List files in S3
            response = s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='processed/movies/',
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                logger.warning(f"No movie files found in s3://{self.s3_bucket}/processed/movies/")
                return pd.DataFrame()
            
            # Get latest parquet file (sorted by LastModified)
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            parquet_files = [f for f in files if f['Key'].endswith('.parquet')]
            
            if not parquet_files:
                logger.warning("No movie parquet files found in S3")
                return pd.DataFrame()
            
            latest_file = parquet_files[0]
            logger.info(f"Loading movies from S3: {latest_file['Key']}")
            
            # Download and read parquet
            obj = s3_client.get_object(Bucket=self.s3_bucket, Key=latest_file['Key'])
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            logger.info(f"Loaded {len(df)} movies from S3")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load movies from S3: {e}")
            return pd.DataFrame()
    
    def load_processed_ratings(self) -> pd.DataFrame:
        """Load processed ratings - try local interaction features first, then S3"""
        # ── 1. Local interaction features file (fast, full 1M rows) ──────────
        try:
            feature_dir = self.data_dir / "features"
            parquet_files = sorted(feature_dir.glob("interaction_features_*.parquet"), reverse=True)
            if parquet_files:
                df = pd.read_parquet(parquet_files[0])
                # Keep only the core rating columns
                keep = [c for c in ['UserID', 'MovieID', 'Rating', 'Timestamp'] if c in df.columns]
                if keep:
                    df = df[keep]
                logger.info(f"Loaded {len(df)} ratings from local features: {parquet_files[0].name}")
                return df
        except Exception as local_err:
            logger.warning(f"Local ratings load failed, falling back to S3: {local_err}")

        # ── 2. S3 fallback: pick directory with most data, read all parts ────
        try:
            import boto3
            from io import BytesIO

            s3_client = boto3.client('s3', region_name=self.aws_region)

            # Paginate through ALL files under processed/ratings/
            all_files = []
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix='processed/ratings/'):
                all_files.extend(page.get('Contents', []))

            parquet_files = [
                f for f in all_files
                if f['Key'].endswith('.parquet') or f['Key'].endswith('.snappy.parquet')
            ]

            if not parquet_files:
                logger.warning("No rating parquet files found in S3")
                return pd.DataFrame()

            # Group files by their immediate parent directory
            dir_groups: dict = {}
            for f in parquet_files:
                # e.g. "processed/ratings/20260212_185501/part-00000-xxx.parquet"
                key = f['Key']
                relative = key[len('processed/ratings/'):]  # strip prefix
                parts = relative.split('/')
                dir_key = parts[0] if len(parts) > 1 else '_flat_'
                if dir_key not in dir_groups:
                    dir_groups[dir_key] = {'files': [], 'total_size': 0}
                dir_groups[dir_key]['files'].append(f)
                dir_groups[dir_key]['total_size'] += f['Size']

            # Select the directory with the largest total byte size
            best_dir, best_info = max(dir_groups.items(), key=lambda x: x[1]['total_size'])
            selected_files = best_info['files']
            logger.info(
                f"Loading ratings from S3 dir '{best_dir}': "
                f"{len(selected_files)} files, "
                f"{best_info['total_size'] // 1024 // 1024} MB"
            )

            # Download and concatenate all parts
            dfs = []
            for file_info in selected_files:
                obj = s3_client.get_object(Bucket=self.s3_bucket, Key=file_info['Key'])
                dfs.append(pd.read_parquet(BytesIO(obj['Body'].read())))

            if not dfs:
                return pd.DataFrame()

            df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Loaded {len(df)} ratings from S3 (multi-part)")
            return df

        except Exception as e:
            logger.error(f"Failed to load ratings from S3: {e}")
            return pd.DataFrame()
    
    def load_interaction_features(self) -> pd.DataFrame:
        """Load latest interaction features from feature engineering"""
        try:
            feature_dir = self.data_dir / "features"
            
            if not feature_dir.exists():
                logger.warning(f"Features directory not found: {feature_dir}")
                return pd.DataFrame()
            
            # Get latest interaction features
            csv_files = sorted(feature_dir.glob("interaction_features_*.csv"), reverse=True)
            parquet_files = sorted(feature_dir.glob("interaction_features_*.parquet"), reverse=True)
            
            # Prefer parquet, fallback to CSV
            if parquet_files:
                latest_file = parquet_files[0]
                df = pd.read_parquet(latest_file)
            elif csv_files:
                latest_file = csv_files[0]
                df = pd.read_csv(latest_file)
            else:
                logger.warning("No feature files found")
                return pd.DataFrame()
            
            logger.info(f"Loading features from: {latest_file}")
            logger.info(f"Loaded {len(df)} feature rows")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load features: {e}")
            return pd.DataFrame()
    
    def get_user_watch_history(self, user_id: int, limit: int = 20) -> pd.DataFrame:
        """Get user's watch history with movie details"""
        try:
            ratings_df = self.load_processed_ratings()
            movies_df = self.load_processed_movies()
            
            if ratings_df.empty or movies_df.empty:
                return pd.DataFrame()
            
            # Filter ratings for user
            if 'UserID' in ratings_df.columns:
                user_ratings = ratings_df[ratings_df['UserID'] == user_id].copy()
                
                if user_ratings.empty:
                    return pd.DataFrame()
                
                # Merge with movie details
                if 'MovieID' in user_ratings.columns and 'MovieID' in movies_df.columns:
                    history = user_ratings.merge(
                        movies_df[['MovieID', 'Title', 'Genres']],
                        on='MovieID',
                        how='left'
                    )
                    
                    # Sort by timestamp or rating (descending)
                    if 'Timestamp' in history.columns:
                        history = history.sort_values('Timestamp', ascending=False)
                    elif 'Rating' in history.columns:
                        history = history.sort_values('Rating', ascending=False)
                    
                    return history.head(limit)
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Failed to get watch history: {e}")
            return pd.DataFrame()
    
    # ==================== PREDICTIONS ====================
    
    def predict_rating(self, user_id: int, movie_id: int) -> Dict[str, Any]:
        """Get prediction from FastAPI service (backed by MLflow model)"""
        try:
            response = requests.post(
                f"{self.api_url}/predict",
                json={
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "user_avg_rating": 3.5,
                    "user_rating_count": 50,
                    "movie_popularity": 1.0,
                    "movie_avg_rating": 3.5,
                    "day_of_week": datetime.now().weekday(),
                    "month": datetime.now().month
                },
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Enrich with movie details
                movies_df = self.load_processed_movies()
                if not movies_df.empty and 'MovieID' in movies_df.columns:
                    movie_info = movies_df[movies_df['MovieID'] == movie_id]
                    if not movie_info.empty:
                        result['movie_title'] = movie_info.iloc[0].get('Title', 'Unknown')
                        result['movie_genres'] = movie_info.iloc[0].get('Genres', 'Unknown')
                
                logger.info(f"Prediction: User {user_id}, Movie {movie_id} -> {result.get('predicted_rating', 0):.2f}")
                return result
            else:
                return {"error": f"API returned status {response.status_code}"}
                
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            return {"error": "API timeout - service may be starting"}
        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to API")
            return {"error": "API not available - check if service is running"}
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {"error": str(e)}
    
    def batch_predict(self, movie_ids: List[int]) -> pd.DataFrame:
        """Batch prediction for multiple movies"""
        try:
            # Create batch request
            predictions_list = []
            for movie_id in movie_ids:
                predictions_list.append({
                    "user_id": 1,
                    "movie_id": movie_id,
                    "user_avg_rating": 3.5,
                    "user_rating_count": 50,
                    "movie_popularity": 1.0,
                    "movie_avg_rating": 3.5,
                    "day_of_week": datetime.now().weekday(),
                    "month": datetime.now().month
                })
            
            response = requests.post(
                f"{self.api_url}/predict/batch",
                json={"predictions": predictions_list},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                predictions = result.get('predictions', [])
                
                df = pd.DataFrame(predictions)
                
                # Enrich with movie details
                if not df.empty:
                    movies_df = self.load_processed_movies()
                    if not movies_df.empty and 'MovieID' in movies_df.columns and 'movie_id' in df.columns:
                        df = df.merge(
                            movies_df[['MovieID', 'Title', 'Genres']].rename(columns={
                                'MovieID': 'movie_id',
                                'Title': 'movie_title',
                                'Genres': 'movie_genres'
                            }),
                            on='movie_id',
                            how='left'
                        )
                
                logger.info(f"Batch prediction: {len(df)} results")
                return df
            else:
                logger.error(f"Batch API returned {response.status_code}")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Batch prediction failed: {e}")
            return pd.DataFrame()
    
    # ==================== AIRFLOW ====================
    
    def trigger_full_pipeline(self) -> Dict[str, Any]:
        """Trigger the mlops_full_pipeline DAG"""
        try:
            url = f"{self.airflow_url}/api/v1/dags/mlops_full_pipeline/dagRuns"
            
            dag_run_id = f"ui_trigger_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            payload = {
                "conf": {},
                "dag_run_id": dag_run_id,
                "logical_date": datetime.now(timezone.utc).isoformat()
            }
            
            response = requests.post(
                url,
                json=payload,
                auth=self.airflow_auth,
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"DAG triggered: {dag_run_id}")
                return {
                    "status": "success",
                    "dag_run_id": dag_run_id,
                    "message": "Pipeline triggered successfully"
                }
            elif response.status_code == 401:
                logger.error("Airflow authentication failed")
                return {
                    "status": "error",
                    "message": "Authentication failed. Check Airflow credentials (admin/admin123)"
                }
            else:
                error_msg = f"Status {response.status_code}: {response.text[:200]}"
                logger.error(f"Failed to trigger DAG: {error_msg}")
                return {
                    "status": "error",
                    "message": error_msg
                }
                
        except Exception as e:
            logger.error(f"Cannot trigger Airflow DAG: {e}")
            return {
                "status": "error",
                "message": f"Cannot connect to Airflow: {str(e)}"
            }
    
    def get_pipeline_status(self, dag_run_id: str = None) -> Dict[str, Any]:
        """Get status of the mlops_full_pipeline"""
        try:
            if dag_run_id:
                # Get specific run
                url = f"{self.airflow_url}/api/v1/dags/mlops_full_pipeline/dagRuns/{dag_run_id}"
                params = {}
            else:
                # Get latest run
                url = f"{self.airflow_url}/api/v1/dags/mlops_full_pipeline/dagRuns"
                params = {"limit": 1, "order_by": "-start_date"}
            
            response = requests.get(
                url,
                params=params,
                auth=self.airflow_auth,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if dag_run_id:
                    return data
                else:
                    runs = data.get('dag_runs', [])
                    return runs[0] if runs else {}
            elif response.status_code == 401:
                logger.error("Airflow authentication failed (401)")
                return {"error": "Authentication failed - Check Airflow is configured with auth disabled or use correct credentials"}
            else:
                return {"error": f"Airflow API returned {response.status_code}"}
            
        except Exception as e:
            logger.error(f"Cannot get pipeline status: {e}")
            return {"error": str(e)}
    
    def get_task_statuses(self, dag_run_id: str) -> List[Dict]:
        """Get individual task statuses from a DAG run"""
        try:
            url = f"{self.airflow_url}/api/v1/dags/mlops_full_pipeline/dagRuns/{dag_run_id}/taskInstances"
            
            response = requests.get(
                url,
                auth=self.airflow_auth,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('task_instances', [])
            else:
                logger.warning(f"Cannot get task statuses: {response.status_code}")
                return []
            
        except Exception as e:
            logger.error(f"Failed to get task statuses: {e}")
            return []
    
    # ==================== MLFLOW ====================
    
    def get_registered_models(self) -> List[Dict]:
        """Get models from MLflow registry (uses /search — /list is 404 in v2.9.2)"""
        try:
            response = requests.post(
                f"{self.mlflow_url}/api/2.0/mlflow/registered-models/search",
                json={"max_results": 100},
                timeout=5
            )

            if response.status_code == 200:
                data = response.json()
                models = data.get('registered_models', [])
                logger.info(f"Found {len(models)} registered models")
                return models
            else:
                logger.warning(f"MLflow registered-models/search returned {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Cannot get registered models: {e}")
            return []
    
    def get_model_metrics(self, model_name: str = "nmf_recommendation_production") -> Dict:
        """Get latest model metrics from MLflow"""
        try:
            # Try to searchall experiments for recent runs with metrics
            search_url = f"{self.mlflow_url}/api/2.0/mlflow/runs/search"
            
            # Search in all experiments (use /search — /list is 404 in MLflow v2.9.2)
            experiment_ids = ["0", "1"]  # safe defaults
            try:
                experiments_response = requests.post(
                    f"{self.mlflow_url}/api/2.0/mlflow/experiments/search",
                    json={"max_results": 50},
                    timeout=5
                )
                if experiments_response.status_code == 200:
                    experiments = experiments_response.json().get('experiments', [])
                    experiment_ids = [exp['experiment_id'] for exp in experiments
                                      if exp.get('lifecycle_stage') == 'active']
            except Exception:
                pass  # keep defaults
            
            # Search for runs with metrics
            search_response = requests.post(
                search_url,
                json={
                    "experiment_ids": experiment_ids[:5],  # Limit to first 5 experiments
                    "max_results": 10,
                    "order_by": ["start_time DESC"]
                },
                timeout=10
            )
            
            if search_response.status_code == 200:
                runs = search_response.json().get('runs', [])
                
                # Find run with metrics
                for run in runs:
                    metrics_list = run.get('data', {}).get('metrics', [])
                    if metrics_list:
                        # Convert list of metric objects to dict
                        metrics_dict = {m['key']: m['value'] for m in metrics_list}
                        logger.info(f"Found metrics from MLflow: {list(metrics_dict.keys())}")
                        return metrics_dict
                
                logger.warning("No runs with metrics found in MLflow")
            
            # Fallback: try registered model approach
            url = f"{self.mlflow_url}/api/2.0/mlflow/registered-models/get"
            response = requests.get(
                url,
                params={"name": model_name},
                timeout=5
            )
            
            if response.status_code == 200:
                model = response.json().get('registered_model', {})
                latest_versions = model.get('latest_versions', [])
                
                if latest_versions:
                    version = latest_versions[0]
                    run_id = version.get('run_id')
                    
                    if run_id:
                        # Get run metrics
                        run_url = f"{self.mlflow_url}/api/2.0/mlflow/runs/get"
                        run_response = requests.get(
                            run_url,
                            params={"run_id": run_id},
                            timeout=5
                        )
                        
                        if run_response.status_code == 200:
                            run_data = run_response.json().get('run', {})
                            metrics_list = run_data.get('data', {}).get('metrics', [])
                            if metrics_list:
                                # Convert list of metric objects to dict
                                metrics_dict = {m['key']: m['value'] for m in metrics_list}
                                logger.info(f"Retrieved metrics for model {model_name}")
                                return metrics_dict
            
            logger.warning("No metrics available - run training pipeline first")
            return {}
            
        except Exception as e:
            logger.error(f"Cannot get model metrics: {e}")
            return {}
    
    # ==================== DRIFT DETECTION ====================
    
    def get_latest_drift_report(self) -> Dict:
        """Get latest drift detection report from S3"""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            # Initialize S3 client using configured region
            s3_client = boto3.client('s3', region_name=self.aws_region)
            bucket_name = os.getenv('S3_BUCKET', 'mlops-movielens-poc')
            
            # List drift reports from S3
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix='reports/drift/',
                    Delimiter='/'
                )
                
                if 'Contents' in response:
                    # Get all JSON files
                    json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
                    
                    if json_files:
                        # Sort by key (which includes timestamp) and get latest
                        latest_key = sorted(json_files)[-1]
                        logger.info(f"Loading drift report from S3: {latest_key}")
                        
                        # Download and parse the report
                        obj = s3_client.get_object(Bucket=bucket_name, Key=latest_key)
                        report = json.loads(obj['Body'].read().decode('utf-8'))
                        
                        return report
                    else:
                        logger.warning("No drift reports found in S3")
                else:
                    logger.warning("No drift reports folder in S3")
            except ClientError as e:
                logger.error(f"S3 access error: {e}")
            
            # Fallback: Try local directory
            reports_dir = self.data_dir / "reports" / "drift"
            
            if reports_dir.exists():
                json_files = sorted(reports_dir.glob("*.json"), reverse=True)
                
                if json_files:
                    latest_file = json_files[0]
                    logger.info(f"Loading local drift report: {latest_file}")
                    
                    with open(latest_file, 'r') as f:
                        report = json.load(f)
                    
                    return report
            
            logger.warning("No drift reports found locally or in S3")
            return {}
            
        except Exception as e:
            logger.error(f"Cannot load drift report: {e}")
            return {}
    
    # ==================== STATISTICS ====================
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """Get real statistics from processed data"""
        try:
            # Load data
            movies_df = self.load_processed_movies()
            ratings_df = self.load_processed_ratings()
            features_df = self.load_interaction_features()
            
            # Calculate statistics
            stats = {
                "total_movies": len(movies_df) if not movies_df.empty else 0,
                "total_ratings": len(ratings_df) if not ratings_df.empty else 0,
                "total_interactions": len(features_df) if not features_df.empty else 0,
                "feature_columns": len(features_df.columns) if not features_df.empty else 0,
                "last_updated": datetime.now().isoformat()
            }
            
            # Add unique users
            if not ratings_df.empty:
                user_col = 'UserID' if 'UserID' in ratings_df.columns else 'user_id'
                if user_col in ratings_df.columns:
                    stats['unique_users'] = int(ratings_df[user_col].nunique())
                
                # Add average rating
                rating_col = 'Rating' if 'Rating' in ratings_df.columns else 'rating'
                if rating_col in ratings_df.columns:
                    stats['avg_rating'] = float(ratings_df[rating_col].mean())
            
            # Add movie genres count if available
            if not movies_df.empty and 'genres' in movies_df.columns:
                all_genres = set()
                for genres in movies_df['genres'].dropna():
                    all_genres.update(str(genres).split('|'))
                stats['unique_genres'] = len(all_genres)
            
            logger.info(f"Statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {
                "error": str(e),
                "total_movies": 0,
                "total_ratings": 0,
                "total_interactions": 0,
                "feature_columns": 0
            }
