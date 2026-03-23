"""
Feature Validation Module
========================

Validates feature consistency and quality.
Implements feature schema validation and consistency checks.

Input: Engineered features
Output: Parity verified (validation report)
"""

import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureValidationStatus(Enum):
    """Feature validation status"""
    PASSED = "PASSED"
    PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
    FAILED = "FAILED"


class FeatureValidator:
    """Validates engineered features for consistency and quality"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize feature validator"""
        self.config = config.get('features', {})
        self.validation_results = []
        logger.info("FeatureValidator initialized")
    
    def validate_feature_schema(self, user_features: pd.DataFrame, 
                               movie_features: pd.DataFrame,
                               interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Validate feature schema and structure"""
        logger.info("Validating feature schema...")
        
        errors = []
        warnings = []
        
        # Check required user features
        required_user_cols = ['UserID', 'AvgRating', 'RatingCount']
        missing_user = [col for col in required_user_cols if col not in user_features.columns]
        if missing_user:
            errors.append(f"Missing user features: {missing_user}")
        
        # Check required movie features
        required_movie_cols = ['MovieID', 'AvgRating', 'Popularity']
        missing_movie = [col for col in required_movie_cols if col not in movie_features.columns]
        if missing_movie:
            errors.append(f"Missing movie features: {missing_movie}")
        
        # Check required interaction features
        required_interaction_cols = ['UserID', 'MovieID', 'Rating', 'Timestamp']
        missing_interaction = [col for col in required_interaction_cols 
                              if col not in interaction_features.columns]
        if missing_interaction:
            errors.append(f"Missing interaction features: {missing_interaction}")
        
        # Check data types
        if 'UserID' in user_features.columns and not pd.api.types.is_integer_dtype(user_features['UserID']):
            warnings.append("UserID should be integer type")
        
        if 'MovieID' in movie_features.columns and not pd.api.types.is_integer_dtype(movie_features['MovieID']):
            warnings.append("MovieID should be integer type")
        
        return {
            'check_name': 'schema_validation',
            'status': FeatureValidationStatus.FAILED if errors else 
                     (FeatureValidationStatus.PASSED_WITH_WARNINGS if warnings else FeatureValidationStatus.PASSED),
            'errors': errors,
            'warnings': warnings
        }
    
    def validate_feature_nulls(self, user_features: pd.DataFrame,
                              movie_features: pd.DataFrame,
                              interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Validate null/missing values in features"""
        logger.info("Validating null values...")
        
        errors = []
        warnings = []
        
        null_threshold = self.config.get('null_threshold', 0.1)  # 10%
        
        # Check user features
        user_nulls = user_features.isnull().sum() / len(user_features)
        for col, null_rate in user_nulls[user_nulls > 0].items():
            if null_rate > null_threshold:
                errors.append(f"User feature '{col}': {null_rate:.2%} null values")
            elif null_rate > 0:
                warnings.append(f"User feature '{col}': {null_rate:.2%} null values")
        
        # Check movie features
        movie_nulls = movie_features.isnull().sum() / len(movie_features)
        for col, null_rate in movie_nulls[movie_nulls > 0].items():
            if null_rate > null_threshold:
                errors.append(f"Movie feature '{col}': {null_rate:.2%} null values")
        
        # Check interaction features
        interaction_nulls = interaction_features.isnull().sum() / len(interaction_features)
        for col, null_rate in interaction_nulls[interaction_nulls > 0].items():
            if null_rate > null_threshold:
                errors.append(f"Interaction feature '{col}': {null_rate:.2%} null values")
        
        return {
            'check_name': 'null_validation',
            'status': FeatureValidationStatus.FAILED if errors else
                     (FeatureValidationStatus.PASSED_WITH_WARNINGS if warnings else FeatureValidationStatus.PASSED),
            'errors': errors,
            'warnings': warnings
        }
    
    def validate_feature_ranges(self, user_features: pd.DataFrame,
                               movie_features: pd.DataFrame,
                               interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Validate feature value ranges"""
        logger.info("Validating feature ranges...")
        
        errors = []
        warnings = []
        
        # Validate user rating average (should be 0-5)
        if 'AvgRating' in user_features.columns:
            invalid_ratings = ((user_features['AvgRating'] < 0) | 
                             (user_features['AvgRating'] > 5)).sum()
            if invalid_ratings > 0:
                errors.append(f"User AvgRating out of range [0, 5]: {invalid_ratings} records")
        
        # Validate movie rating average
        if 'AvgRating' in movie_features.columns:
            invalid_ratings = ((movie_features['AvgRating'] < 0) | 
                             (movie_features['AvgRating'] > 5)).sum()
            if invalid_ratings > 0:
                errors.append(f"Movie AvgRating out of range [0, 5]: {invalid_ratings} records")
        
        # Validate interaction ratings
        if 'Rating' in interaction_features.columns:
            invalid_ratings = ((interaction_features['Rating'] < 0.5) | 
                             (interaction_features['Rating'] > 5)).sum()
            if invalid_ratings > 0:
                errors.append(f"Interaction Rating out of range [0.5, 5]: {invalid_ratings} records")
        
        # Validate popularity (count should be non-negative)
        if 'Popularity' in movie_features.columns:
            invalid_popularity = (movie_features['Popularity'] < 0).sum()
            if invalid_popularity > 0:
                errors.append(f"Negative popularity values: {invalid_popularity} records")
        
        return {
            'check_name': 'range_validation',
            'status': FeatureValidationStatus.FAILED if errors else
                     (FeatureValidationStatus.PASSED_WITH_WARNINGS if warnings else FeatureValidationStatus.PASSED),
            'errors': errors,
            'warnings': warnings
        }
    
    def validate_feature_consistency(self, user_features: pd.DataFrame,
                                    movie_features: pd.DataFrame,
                                    interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Validate consistency between features"""
        logger.info("Validating feature consistency...")
        
        errors = []
        warnings = []
        
        # Check UserID consistency
        interaction_users = set(interaction_features['UserID'].unique())
        feature_users = set(user_features['UserID'].unique())
        missing_users = interaction_users - feature_users
        if missing_users:
            warnings.append(f"UserIDs in interactions but not in features: {len(missing_users)}")
        
        # Check MovieID consistency
        interaction_movies = set(interaction_features['MovieID'].unique())
        feature_movies = set(movie_features['MovieID'].unique())
        missing_movies = interaction_movies - feature_movies
        if missing_movies:
            warnings.append(f"MovieIDs in interactions but not in features: {len(missing_movies)}")
        
        # Validate feature cardinality
        if len(user_features) < 100:
            warnings.append(f"Low user feature cardinality: {len(user_features)} users")
        
        if len(movie_features) < 100:
            warnings.append(f"Low movie feature cardinality: {len(movie_features)} movies")
        
        return {
            'check_name': 'consistency_validation',
            'status': FeatureValidationStatus.FAILED if errors else
                     (FeatureValidationStatus.PASSED_WITH_WARNINGS if warnings else FeatureValidationStatus.PASSED),
            'errors': errors,
            'warnings': warnings
        }
    
    def validate_feature_statistics(self, user_features: pd.DataFrame,
                                   movie_features: pd.DataFrame,
                                   interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Validate feature statistical properties"""
        logger.info("Validating feature statistics...")
        
        errors = []
        warnings = []
        
        # Check for high variance features
        numeric_user_cols = user_features.select_dtypes(include=[np.number]).columns
        for col in numeric_user_cols:
            if user_features[col].std() > 1000:
                warnings.append(f"High variance in user feature '{col}': std={user_features[col].std():.2f}")
        
        # Check for constant features
        for col in numeric_user_cols:
            if user_features[col].std() == 0:
                errors.append(f"Constant feature detected: '{col}'")
        
        # Check feature correlation (no perfect multicollinearity)
        numeric_cols = interaction_features.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            corr_matrix = interaction_features[numeric_cols].corr().abs()
            high_corr = (corr_matrix > 0.95).sum().sum() / 2  # Divide by 2 for symmetry
            if high_corr > 5:
                warnings.append(f"High multicollinearity detected: {high_corr} pairs with r > 0.95")
        
        return {
            'check_name': 'statistical_validation',
            'status': FeatureValidationStatus.FAILED if errors else
                     (FeatureValidationStatus.PASSED_WITH_WARNINGS if warnings else FeatureValidationStatus.PASSED),
            'errors': errors,
            'warnings': warnings
        }
    
    def validate_all_features(self, user_features: pd.DataFrame,
                             movie_features: pd.DataFrame,
                             interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Run all feature validation checks"""
        logger.info("=" * 80)
        logger.info("STARTING FEATURE VALIDATION")
        logger.info("=" * 80)
        
        try:
            # Run all validations
            schema_result = self.validate_feature_schema(user_features, movie_features, interaction_features)
            null_result = self.validate_feature_nulls(user_features, movie_features, interaction_features)
            range_result = self.validate_feature_ranges(user_features, movie_features, interaction_features)
            consistency_result = self.validate_feature_consistency(user_features, movie_features, interaction_features)
            stats_result = self.validate_feature_statistics(user_features, movie_features, interaction_features)
            
            self.validation_results = [
                schema_result,
                null_result,
                range_result,
                consistency_result,
                stats_result
            ]
            
            # Calculate overall status
            failed_checks = [r for r in self.validation_results if r['status'] == FeatureValidationStatus.FAILED]
            passed_checks = [r for r in self.validation_results if r['status'] == FeatureValidationStatus.PASSED]
            warning_checks = [r for r in self.validation_results if r['status'] == FeatureValidationStatus.PASSED_WITH_WARNINGS]
            
            overall_status = FeatureValidationStatus.FAILED if failed_checks else (
                FeatureValidationStatus.PASSED_WITH_WARNINGS if warning_checks else FeatureValidationStatus.PASSED
            )
            
            logger.info("=" * 80)
            logger.info("FEATURE VALIDATION COMPLETED")
            logger.info("=" * 80)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_status': overall_status.value,
                'validation_results': [
                    {
                        'check_name': r['check_name'],
                        'status': r['status'].value,
                        'errors': r['errors'],
                        'warnings': r['warnings']
                    }
                    for r in self.validation_results
                ],
                'summary': {
                    'total_checks': len(self.validation_results),
                    'passed': len(passed_checks),
                    'passed_with_warnings': len(warning_checks),
                    'failed': len(failed_checks)
                }
            }
        
        except Exception as e:
            logger.error(f"Feature validation failed: {str(e)}", exc_info=True)
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_status': FeatureValidationStatus.FAILED.value,
                'error': str(e)
            }


if __name__ == '__main__':
    import yaml
    
    with open('config/data_ingestion_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Load features
    import glob
    feature_files = glob.glob('data/features/*.parquet')
    
    if feature_files:
        validator = FeatureValidator(config)
        print("Feature files found - add feature loading logic")
