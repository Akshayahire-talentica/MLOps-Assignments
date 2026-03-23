"""
Feature Engineering Module
=========================

Transforms validated raw data into engineered features for ML training.
Implements collaborative filtering features and temporal features.

Input: Validated Parquet data from Phase 1
Output: Feature vectors (Features v1)
"""

import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Any
import glob
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Implements feature engineering for MovieLens data"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize feature engineer with configuration"""
        self.config = config
        self.feature_version = config.get('features', {}).get('version', 'v1')
        self.output_path = Path(config.get('features', {}).get('output_path', 'data/features'))
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.movies_df = None
        self.ratings_df = None
        self.users_df = None
        self.user_features = None
        self.movie_features = None
        self.interaction_features = None
        
        logger.info(f"FeatureEngineer initialized - Version: {self.feature_version}")
    
    def load_validated_data(self, data_path: str = 'data/processed') -> Dict[str, pd.DataFrame]:
        """Load validated data from Parquet files or fall back to raw .dat files"""
        logger.info(f"Loading data from {data_path}")
        
        try:
            # Try loading from Parquet files first
            movies_files = glob.glob(f"{data_path}/movies/*.parquet")
            ratings_files = glob.glob(f"{data_path}/ratings/*.parquet")
            users_files = glob.glob(f"{data_path}/users/*.parquet")
            
            if movies_files and ratings_files and users_files:
                logger.info("Loading from Parquet files...")
                self.movies_df = pd.read_parquet(max(movies_files, key=lambda f: Path(f).stat().st_mtime))
                self.ratings_df = pd.read_parquet(max(ratings_files, key=lambda f: Path(f).stat().st_mtime))
                self.users_df = pd.read_parquet(max(users_files, key=lambda f: Path(f).stat().st_mtime))
            else:
                # Fall back to loading raw .dat files
                logger.warning("Parquet files not found, loading from raw .dat files...")
                self._load_from_raw_files()
            
            logger.info(f"[OK] Loaded movies: {len(self.movies_df)} rows")
            logger.info(f"[OK] Loaded ratings: {len(self.ratings_df)} rows")
            logger.info(f"[OK] Loaded users: {len(self.users_df)} rows")
            
            return {
                'movies': self.movies_df,
                'ratings': self.ratings_df,
                'users': self.users_df
            }
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _load_from_raw_files(self, raw_data_path: str = 'data/raw') -> None:
        """Load data from raw .dat files"""
        logger.info(f"Loading raw .dat files from {raw_data_path}")
        
        # Load movies
        movies_path = Path(raw_data_path) / 'movies.dat'
        if not movies_path.exists():
            raise FileNotFoundError(f"Movies file not found: {movies_path}")
        
        self.movies_df = pd.read_csv(
            movies_path,
            sep='::',
            engine='python',
            header=None,
            names=['MovieID', 'Title', 'Genres'],
            encoding='latin-1'
        )
        
        # Load ratings
        ratings_path = Path(raw_data_path) / 'ratings.dat'
        if not ratings_path.exists():
            raise FileNotFoundError(f"Ratings file not found: {ratings_path}")
        
        self.ratings_df = pd.read_csv(
            ratings_path,
            sep='::',
            engine='python',
            header=None,
            names=['UserID', 'MovieID', 'Rating', 'Timestamp'],
            encoding='latin-1'
        )
        
        # Load users
        users_path = Path(raw_data_path) / 'users.dat'
        if not users_path.exists():
            raise FileNotFoundError(f"Users file not found: {users_path}")
        
        self.users_df = pd.read_csv(
            users_path,
            sep='::',
            engine='python',
            header=None,
            names=['UserID', 'Gender', 'Age', 'Occupation', 'ZipCode'],
            encoding='latin-1'
        )
        
        logger.info("Raw .dat files loaded successfully")
    
    def engineer_user_features(self) -> pd.DataFrame:
        """Engineer user-level features"""
        logger.info("Engineering user features...")
        
        user_features = self.users_df.copy()
        
        # User activity features
        user_rating_count = self.ratings_df.groupby('UserID').size().reset_index(name='RatingCount')
        user_features = user_features.merge(user_rating_count, on='UserID', how='left')
        
        # User average rating
        user_avg_rating = self.ratings_df.groupby('UserID')['Rating'].mean().reset_index(name='AvgRating')
        user_features = user_features.merge(user_avg_rating, on='UserID', how='left')
        
        # User rating std dev
        user_std_rating = self.ratings_df.groupby('UserID')['Rating'].std().reset_index(name='StdRating')
        user_features = user_features.merge(user_std_rating, on='UserID', how='left')
        
        # User rating spread (max - min)
        user_rating_range = self.ratings_df.groupby('UserID')['Rating'].apply(
            lambda x: x.max() - x.min()
        ).reset_index(name='RatingRange')
        user_features = user_features.merge(user_rating_range, on='UserID', how='left')
        
        # Handle null values - fill with defaults for users with no ratings
        user_features['RatingCount'] = user_features['RatingCount'].fillna(0)
        user_features['AvgRating'] = user_features['AvgRating'].fillna(0.0)
        user_features['StdRating'] = user_features['StdRating'].fillna(0.0)
        user_features['RatingRange'] = user_features['RatingRange'].fillna(0)
        
        # Age group encoding
        user_features['AgeGroup'] = pd.cut(user_features['Age'], 
                                           bins=[0, 18, 25, 35, 45, 50, 56],
                                           labels=['<18', '18-25', '25-35', '35-45', '45-50', '50+'])
        
        # Occupation one-hot encoding
        occupation_dummies = pd.get_dummies(user_features['Occupation'], prefix='Occupation')
        user_features = pd.concat([user_features, occupation_dummies], axis=1)
        
        # Gender encoding
        user_features['GenderMale'] = (user_features['Gender'] == 'M').astype(int)
        
        self.user_features = user_features
        logger.info(f"[OK] Engineered user features: {user_features.shape}")
        
        return user_features
    
    def engineer_movie_features(self) -> pd.DataFrame:
        """Engineer movie-level features"""
        logger.info("Engineering movie features...")
        
        movie_features = self.movies_df.copy()
        
        # Movie popularity (rating count)
        movie_rating_count = self.ratings_df.groupby('MovieID').size().reset_index(name='Popularity')
        movie_features = movie_features.merge(movie_rating_count, on='MovieID', how='left')
        
        # Movie average rating
        movie_avg_rating = self.ratings_df.groupby('MovieID')['Rating'].mean().reset_index(name='AvgRating')
        movie_features = movie_features.merge(movie_avg_rating, on='MovieID', how='left')
        
        # Handle null values - movies without ratings
        movie_features['Popularity'] = movie_features['Popularity'].fillna(0.0)
        movie_features['AvgRating'] = movie_features['AvgRating'].fillna(0.0)
        
        # Movie rating count categories
        movie_features['PopularityCategory'] = pd.cut(movie_features['Popularity'],
                                                      bins=[0, 10, 50, 100, 500, float('inf')],
                                                      labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
        
        # Fill any remaining null categories
        movie_features['PopularityCategory'] = movie_features['PopularityCategory'].fillna('Very Low')
        
        # Genre one-hot encoding
        genre_dummies = movie_features['Genres'].str.get_dummies(sep='|')
        genre_dummies.columns = [f'Genre_{g}' for g in genre_dummies.columns]
        movie_features = pd.concat([movie_features, genre_dummies], axis=1)
        
        # Genre count
        movie_features['GenreCount'] = movie_features['Genres'].str.split('|').str.len()
        
        self.movie_features = movie_features
        logger.info(f"[OK] Engineered movie features: {movie_features.shape}")
        
        return movie_features
    
    def engineer_interaction_features(self) -> pd.DataFrame:
        """Engineer interaction-level features"""
        logger.info("Engineering interaction features...")
        
        interactions = self.ratings_df.copy()
        
        # Merge user features with rename to avoid conflicts
        user_cols = self.user_features[['UserID', 'AvgRating', 'RatingCount']].copy()
        user_cols = user_cols.rename(columns={'AvgRating': 'UserAvgRating', 'RatingCount': 'UserRatingCount'})
        interactions = interactions.merge(user_cols, on='UserID', how='left')
        
        # Merge movie features with rename to avoid conflicts
        movie_cols = self.movie_features[['MovieID', 'AvgRating', 'Popularity']].copy()
        movie_cols = movie_cols.rename(columns={'AvgRating': 'MovieAvgRating', 'Popularity': 'MoviePopularity'})
        interactions = interactions.merge(movie_cols, on='MovieID', how='left')
        
        # Temporal features
        interactions['Timestamp'] = pd.to_datetime(interactions['Timestamp'], unit='s')
        interactions['DayOfWeek'] = interactions['Timestamp'].dt.dayofweek
        interactions['Month'] = interactions['Timestamp'].dt.month
        interactions['Year'] = interactions['Timestamp'].dt.year
        interactions['Hour'] = interactions['Timestamp'].dt.hour
        
        # Time-based features
        interactions['TimeSinceFirstRating'] = (
            interactions.groupby('UserID')['Timestamp'].transform('max') - interactions['Timestamp']
        ).dt.days
        
        # Rating deviation from user average
        interactions['RatingDeviation'] = (
            interactions['Rating'] - interactions['UserAvgRating']
        )
        
        # Rating deviation from movie average
        interactions['RatingDevFromMovie'] = (
            interactions['Rating'] - interactions['MovieAvgRating']
        )
        
        # User-movie rating difference
        interactions['UserMovieDiff'] = (
            interactions['UserAvgRating'] - interactions['MovieAvgRating']
        )
        
        self.interaction_features = interactions
        logger.info(f"[OK] Engineered interaction features: {interactions.shape}")
        
        return interactions
    
    def save_features(self, timestamp: str = None) -> Dict[str, str]:
        """Save engineered features"""
        logger.info("Saving features...")
        
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as Parquet (for production use)
        user_path = self.output_path / f'user_features_{timestamp}.parquet'
        movie_path = self.output_path / f'movie_features_{timestamp}.parquet'
        interaction_path = self.output_path / f'interaction_features_{timestamp}.parquet'
        
        self.user_features.to_parquet(user_path)
        self.movie_features.to_parquet(movie_path)
        self.interaction_features.to_parquet(interaction_path)
        
        logger.info(f"[OK] Saved user features: {user_path}")
        logger.info(f"[OK] Saved movie features: {movie_path}")
        logger.info(f"[OK] Saved interaction features: {interaction_path}")
        
        # Save as CSV (for inspection and drift detection)
        self.user_features.to_csv(self.output_path / f'user_features_{timestamp}.csv', index=False)
        self.movie_features.to_csv(self.output_path / f'movie_features_{timestamp}.csv', index=False)
        self.interaction_features.to_csv(self.output_path / f'interaction_features_{timestamp}.csv', index=False)
        
        # Upload to S3 if enabled
        self._upload_features_to_s3(timestamp)
        
        return {
            'user_features': str(user_path),
            'movie_features': str(movie_path),
            'interaction_features': str(interaction_path)
        }
    
    def _upload_features_to_s3(self, timestamp: str) -> None:
        """Upload engineered features to S3"""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))
            from data_ingestion.s3_storage import S3DataStorage
            import yaml
            
            # Load S3 configuration
            config_file = 'config/data_ingestion_config.yaml'
            if not os.path.exists(config_file):
                logger.warning(f"Config file {config_file} not found, skipping S3 upload")
                return
            
            with open(config_file, 'r') as f:
                full_config = yaml.safe_load(f)
            
            s3_config = full_config.get('s3', {})
            
            if not s3_config.get('enabled', False):
                logger.info("S3 upload disabled in configuration")
                return
            
            bucket_name = s3_config.get('bucket_name')
            region = s3_config.get('region', 'ap-south-1')
            profile = s3_config.get('profile')
            
            storage = S3DataStorage(
                bucket_name=bucket_name,
                region=region,
                profile=profile
            )
            
            # Upload Parquet files
            feature_files = [
                (f'user_features_{timestamp}.parquet', 'features/user_features/'),
                (f'movie_features_{timestamp}.parquet', 'features/movie_features/'),
                (f'interaction_features_{timestamp}.parquet', 'features/interaction_features/')
            ]
            
            for filename, prefix in feature_files:
                local_path = self.output_path / filename
                s3_key = f"{prefix}{filename}"
                
                if local_path.exists():
                    logger.info(f"Uploading {filename} to s3://{bucket_name}/{s3_key}")
                    storage.upload_file(
                        str(local_path),
                        s3_key,
                        metadata={
                            'type': 'features',
                            'version': self.feature_version,
                            'timestamp': timestamp
                        }
                    )
            
            logger.info("[OK] Features uploaded to S3 successfully")
            
        except ImportError:
            logger.warning("boto3 not available, skipping S3 upload")
        except Exception as e:
            logger.warning(f"Failed to upload features to S3: {str(e)}")
    
    def get_feature_statistics(self) -> Dict[str, Any]:
        """Calculate feature statistics"""
        logger.info("Calculating feature statistics...")
        
        stats = {
            'user_features': {
                'shape': self.user_features.shape,
                'columns': list(self.user_features.columns),
                'null_counts': self.user_features.isnull().sum().to_dict(),
                'dtypes': {col: str(dtype) for col, dtype in self.user_features.dtypes.items()}
            },
            'movie_features': {
                'shape': self.movie_features.shape,
                'columns': list(self.movie_features.columns),
                'null_counts': self.movie_features.isnull().sum().to_dict(),
                'dtypes': {col: str(dtype) for col, dtype in self.movie_features.dtypes.items()}
            },
            'interaction_features': {
                'shape': self.interaction_features.shape,
                'columns': list(self.interaction_features.columns),
                'null_counts': self.interaction_features.isnull().sum().to_dict(),
                'dtypes': {col: str(dtype) for col, dtype in self.interaction_features.dtypes.items()}
            }
        }
        
        # Log data quality summary
        self._log_data_quality(stats)
        
        return stats
    
    def _log_data_quality(self, stats: Dict[str, Any]) -> None:
        """Log data quality metrics"""
        logger.info("=" * 80)
        logger.info("DATA QUALITY SUMMARY")
        logger.info("=" * 80)
        
        for feature_type, feature_stats in stats.items():
            logger.info(f"\n{feature_type.upper()}:")
            logger.info(f"  Shape: {feature_stats['shape']}")
            
            # Check for null values
            null_counts = feature_stats['null_counts']
            total_nulls = sum(null_counts.values())
            if total_nulls > 0:
                logger.warning(f"  Total null values: {total_nulls}")
                null_cols = {k: v for k, v in null_counts.items() if v > 0}
                for col, count in null_cols.items():
                    logger.warning(f"    - {col}: {count} nulls")
            else:
                logger.info("  No null values detected")
        
        logger.info("=" * 80)
    
    def engineer_all_features(self) -> Dict[str, Any]:
        """Execute complete feature engineering pipeline"""
        logger.info("=" * 80)
        logger.info("STARTING FEATURE ENGINEERING PIPELINE")
        logger.info("=" * 80)
        
        try:
            # Load data
            self.load_validated_data()
            
            # Engineer features
            self.engineer_user_features()
            self.engineer_movie_features()
            self.engineer_interaction_features()
            
            # Save features
            file_paths = self.save_features()
            
            # Get statistics
            stats = self.get_feature_statistics()
            
            logger.info("=" * 80)
            logger.info("FEATURE ENGINEERING COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
            return {
                'status': 'SUCCESS',
                'timestamp': datetime.now().isoformat(),
                'feature_version': self.feature_version,
                'file_paths': file_paths,
                'statistics': stats
            }
        
        except Exception as e:
            logger.error(f"Feature engineering failed: {str(e)}", exc_info=True)
            return {
                'status': 'FAILED',
                'error': str(e)
            }


if __name__ == '__main__':
    import yaml
    
    with open('config/data_ingestion_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    engineer = FeatureEngineer(config)
    result = engineer.engineer_all_features()
    
    print(json.dumps(result, indent=2, default=str))
