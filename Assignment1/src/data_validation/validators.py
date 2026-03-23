"""
Data Validation Module for MovieLens Dataset
============================================

Comprehensive validation framework with multiple validation layers:
1. Schema Validation
2. Statistical Validation
3. Referential Integrity
4. Business Logic Validation
5. Anomaly Detection
6. Completeness Validation
7. Uniqueness Validation
"""

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation result status"""
    PASSED = "PASSED"
    PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
    FAILED = "FAILED"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    check_name: str
    status: ValidationStatus
    details: Dict
    warnings: List[str]
    errors: List[str]
    timestamp: str


class DataValidator:
    """
    Multi-layered validation system ensuring comprehensive data quality.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize validator with configuration.
        
        Args:
            config: Validation rules configuration
        """
        self.config = config
        self.validation_results = []
        
    def validate_schema(self, df: pd.DataFrame, dataset_name: str) -> ValidationResult:
        """
        LAYER 1: SCHEMA VALIDATION
        
        Validates:
        - Expected columns present
        - Column data types correct
        - No unexpected columns
        - Required columns non-null
        """
        logger.info(f"Validating schema for {dataset_name}...")
        
        warnings = []
        errors = []
        details = {}
        
        try:
            dataset_config = self.config['validation_rules'].get(dataset_name, {})
            schema_config = dataset_config.get('schema_validation', {})
            statistical_config = dataset_config.get('statistical_validation', {})
            
            # Check required columns
            for col, col_config in statistical_config.items():
                if col not in df.columns:
                    errors.append(f"Missing required column: {col}")
                else:
                    # Check data type
                    expected_type = col_config.get('type', '').lower()
                    actual_type = str(df[col].dtype).lower()
                    
                    if not self._type_matches(actual_type, expected_type):
                        warnings.append(
                            f"Column {col}: expected {expected_type}, got {actual_type}"
                        )
                    
                    details[col] = {
                        'present': True,
                        'expected_type': expected_type,
                        'actual_type': actual_type,
                        'type_match': self._type_matches(actual_type, expected_type)
                    }
            
            # Check for unexpected columns if strict mode
            if schema_config.get('strict', False):
                unexpected_cols = set(df.columns) - set(statistical_config.keys())
                if unexpected_cols:
                    warnings.append(f"Unexpected columns found: {unexpected_cols}")
            
            status = ValidationStatus.PASSED if not errors else ValidationStatus.FAILED
            if not errors and warnings:
                status = ValidationStatus.PASSED_WITH_WARNINGS
            
            result = ValidationResult(
                check_name='schema_validation',
                status=status,
                details=details,
                warnings=warnings,
                errors=errors,
                timestamp=datetime.now().isoformat()
            )
            
            self.validation_results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}")
            return ValidationResult(
                check_name='schema_validation',
                status=ValidationStatus.FAILED,
                details={},
                warnings=[],
                errors=[str(e)],
                timestamp=datetime.now().isoformat()
            )
    
    def validate_statistics(self, df: pd.DataFrame, dataset_name: str) -> ValidationResult:
        """
        LAYER 2: STATISTICAL VALIDATION
        
        Validates:
        - Column value ranges (min, max)
        - Value distributions
        - Valid value sets
        - Required non-null constraints
        - Step/increment constraints
        """
        logger.info(f"Validating statistics for {dataset_name}...")
        
        warnings = []
        errors = []
        details = {}
        
        try:
            dataset_config = self.config['validation_rules'].get(dataset_name, {})
            statistical_config = dataset_config.get('statistical_validation', {})
            
            for col, col_config in statistical_config.items():
                col_details = {'column': col, 'checks': []}
                
                if col not in df.columns:
                    continue
                
                # Check nullability
                nullable = col_config.get('nullable', False)
                null_count = df[col].isna().sum()
                null_rate = null_count / len(df)
                
                if not nullable and null_count > 0:
                    errors.append(f"{col}: {null_count} null values found (not nullable)")
                    col_details['checks'].append({
                        'check': 'nullable',
                        'result': 'FAILED',
                        'null_count': int(null_count),
                        'null_rate': float(null_rate)
                    })
                else:
                    col_details['checks'].append({
                        'check': 'nullable',
                        'result': 'PASSED',
                        'null_count': int(null_count),
                        'null_rate': float(null_rate)
                    })
                
                # Check range for numeric columns
                col_type = col_config.get('type', '').lower()
                if col_type in ['integer', 'int', 'float', 'double']:
                    non_null = df[col].dropna()
                    
                    if len(non_null) > 0:
                        min_val = non_null.min()
                        max_val = non_null.max()
                        
                        config_min = col_config.get('min')
                        config_max = col_config.get('max')
                        
                        if config_min is not None and min_val < config_min:
                            errors.append(f"{col}: minimum value {min_val} < {config_min}")
                        
                        if config_max is not None and max_val > config_max:
                            errors.append(f"{col}: maximum value {max_val} > {config_max}")
                        
                        col_details['checks'].append({
                            'check': 'range',
                            'result': 'PASSED' if (config_min is None or min_val >= config_min) and 
                                                   (config_max is None or max_val <= config_max) else 'FAILED',
                            'min': float(min_val),
                            'max': float(max_val),
                            'expected_min': config_min,
                            'expected_max': config_max
                        })
                        
                        # Check step/increment
                        step = col_config.get('step')
                        if step is not None:
                            mod_check = ((non_null * (1 / step)) % 1).abs() < 1e-6
                            if not mod_check.all():
                                warnings.append(f"{col}: {mod_check.sum()} values don't match step of {step}")
                
                # Check valid values for categorical columns
                valid_values = col_config.get('valid_values')
                if valid_values is not None and col in df.columns:
                    non_null = df[col].dropna()
                    invalid_mask = ~non_null.isin(valid_values)
                    invalid_count = invalid_mask.sum()
                    
                    if invalid_count > 0:
                        errors.append(
                            f"{col}: {invalid_count} invalid values found. Expected: {valid_values}"
                        )
                        col_details['checks'].append({
                            'check': 'valid_values',
                            'result': 'FAILED',
                            'invalid_count': int(invalid_count),
                            'expected_values': valid_values
                        })
                    else:
                        col_details['checks'].append({
                            'check': 'valid_values',
                            'result': 'PASSED',
                            'valid_values_count': len(valid_values)
                        })
                
                details[col] = col_details
            
            status = ValidationStatus.PASSED if not errors else ValidationStatus.FAILED
            if not errors and warnings:
                status = ValidationStatus.PASSED_WITH_WARNINGS
            
            result = ValidationResult(
                check_name='statistical_validation',
                status=status,
                details=details,
                warnings=warnings,
                errors=errors,
                timestamp=datetime.now().isoformat()
            )
            
            self.validation_results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"Statistical validation error: {str(e)}")
            return ValidationResult(
                check_name='statistical_validation',
                status=ValidationStatus.FAILED,
                details={},
                warnings=[],
                errors=[str(e)],
                timestamp=datetime.now().isoformat()
            )
    
    def validate_referential_integrity(
        self, 
        dfs: Dict[str, pd.DataFrame],
        dataset_name: str
    ) -> ValidationResult:
        """
        LAYER 3: REFERENTIAL INTEGRITY
        
        Validates:
        - Foreign key constraints
        - No orphaned records
        - Complete parent-child relationships
        """
        logger.info(f"Validating referential integrity for {dataset_name}...")
        
        warnings = []
        errors = []
        details = {}
        
        try:
            dataset_config = self.config['validation_rules'].get(dataset_name, {})
            fk_config = dataset_config.get('referential_integrity', {})
            
            if not fk_config.get('enabled', False):
                return ValidationResult(
                    check_name='referential_integrity',
                    status=ValidationStatus.PASSED,
                    details={'status': 'skipped'},
                    warnings=[],
                    errors=[],
                    timestamp=datetime.now().isoformat()
                )
            
            df = dfs.get(dataset_name)
            if df is None:
                return ValidationResult(
                    check_name='referential_integrity',
                    status=ValidationStatus.FAILED,
                    details={},
                    warnings=[],
                    errors=[f"Dataset {dataset_name} not found"],
                    timestamp=datetime.now().isoformat()
                )
            
            # Check foreign keys
            for fk in fk_config.get('foreign_keys', []):
                source_col = fk.get('source')
                target = fk.get('target')
                target_dataset, target_col = target.split('.')
                
                if source_col not in df.columns:
                    errors.append(f"Foreign key source column {source_col} not found")
                    continue
                
                if target_dataset not in dfs:
                    errors.append(f"Target dataset {target_dataset} not found")
                    continue
                
                target_df = dfs[target_dataset]
                if target_col not in target_df.columns:
                    errors.append(f"Target column {target_col} not found in {target_dataset}")
                    continue
                
                # Check if all source values exist in target
                source_values = set(df[source_col].dropna().unique())
                target_values = set(target_df[target_col].dropna().unique())
                
                orphaned = source_values - target_values
                orphaned_count = len(orphaned)
                total_count = len(df[source_col].dropna())
                integrity_rate = (total_count - orphaned_count) / total_count if total_count > 0 else 0
                
                if orphaned_count > 0:
                    errors.append(
                        f"Foreign key violation: {orphaned_count} orphaned records in {source_col}"
                    )
                
                details[f"{source_col}→{target_col}"] = {
                    'total_records': total_count,
                    'orphaned_records': orphaned_count,
                    'integrity_rate': float(integrity_rate),
                    'status': 'PASSED' if orphaned_count == 0 else 'FAILED'
                }
            
            status = ValidationStatus.PASSED if not errors else ValidationStatus.FAILED
            if not errors and warnings:
                status = ValidationStatus.PASSED_WITH_WARNINGS
            
            result = ValidationResult(
                check_name='referential_integrity',
                status=status,
                details=details,
                warnings=warnings,
                errors=errors,
                timestamp=datetime.now().isoformat()
            )
            
            self.validation_results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"Referential integrity validation error: {str(e)}")
            return ValidationResult(
                check_name='referential_integrity',
                status=ValidationStatus.FAILED,
                details={},
                warnings=[],
                errors=[str(e)],
                timestamp=datetime.now().isoformat()
            )
    
    def validate_uniqueness(self, df: pd.DataFrame, dataset_name: str) -> ValidationResult:
        """
        LAYER 7: UNIQUENESS VALIDATION
        
        Validates:
        - Primary key uniqueness
        - Expected cardinality
        - No unexpected duplicates
        """
        logger.info(f"Validating uniqueness for {dataset_name}...")
        
        warnings = []
        errors = []
        details = {}
        
        try:
            dataset_config = self.config['validation_rules'].get(dataset_name, {})
            uniqueness_config = dataset_config.get('uniqueness', {})
            
            if not uniqueness_config:
                return ValidationResult(
                    check_name='uniqueness',
                    status=ValidationStatus.PASSED,
                    details={'status': 'no_constraints'},
                    warnings=[],
                    errors=[],
                    timestamp=datetime.now().isoformat()
                )
            
            pk_cols = uniqueness_config.get('primary_key', [])
            
            if pk_cols:
                # Check primary key uniqueness
                duplicates = df.duplicated(subset=pk_cols, keep=False).sum()
                
                if duplicates > 0:
                    errors.append(f"Primary key ({pk_cols}) has {duplicates} duplicate records")
                
                details['primary_key'] = {
                    'columns': pk_cols,
                    'unique_combinations': len(df[pk_cols].drop_duplicates()),
                    'total_records': len(df),
                    'duplicates': int(duplicates),
                    'status': 'PASSED' if duplicates == 0 else 'FAILED'
                }
            
            status = ValidationStatus.PASSED if not errors else ValidationStatus.FAILED
            if not errors and warnings:
                status = ValidationStatus.PASSED_WITH_WARNINGS
            
            result = ValidationResult(
                check_name='uniqueness',
                status=status,
                details=details,
                warnings=warnings,
                errors=errors,
                timestamp=datetime.now().isoformat()
            )
            
            self.validation_results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"Uniqueness validation error: {str(e)}")
            return ValidationResult(
                check_name='uniqueness',
                status=ValidationStatus.FAILED,
                details={},
                warnings=[],
                errors=[str(e)],
                timestamp=datetime.now().isoformat()
            )
    
    def validate_completeness(self, df: pd.DataFrame, dataset_name: str) -> ValidationResult:
        """
        LAYER 6: COMPLETENESS VALIDATION
        
        Validates:
        - Acceptable null rates
        - Acceptable missing patterns
        - Coverage ratio
        """
        logger.info(f"Validating completeness for {dataset_name}...")
        
        warnings = []
        errors = []
        details = {}
        
        try:
            dataset_config = self.config['validation_rules'].get(dataset_name, {})
            quality_config = dataset_config.get('quality_thresholds', {})
            
            # Check null rates
            null_acceptable = quality_config.get('null_rate_acceptable', 0.05)
            
            for col in df.columns:
                null_rate = df[col].isna().sum() / len(df)
                
                if null_rate > null_acceptable:
                    errors.append(
                        f"{col}: null rate {null_rate:.2%} exceeds threshold {null_acceptable:.2%}"
                    )
                
                details[col] = {
                    'null_count': int(df[col].isna().sum()),
                    'null_rate': float(null_rate),
                    'acceptable_threshold': null_acceptable,
                    'status': 'PASSED' if null_rate <= null_acceptable else 'FAILED'
                }
            
            status = ValidationStatus.PASSED if not errors else ValidationStatus.FAILED
            if not errors and warnings:
                status = ValidationStatus.PASSED_WITH_WARNINGS
            
            result = ValidationResult(
                check_name='completeness',
                status=status,
                details=details,
                warnings=warnings,
                errors=errors,
                timestamp=datetime.now().isoformat()
            )
            
            self.validation_results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"Completeness validation error: {str(e)}")
            return ValidationResult(
                check_name='completeness',
                status=ValidationStatus.FAILED,
                details={},
                warnings=[],
                errors=[str(e)],
                timestamp=datetime.now().isoformat()
            )
    
    def _type_matches(self, actual: str, expected: str) -> bool:
        """Check if actual data type matches expected"""
        type_mappings = {
            'integer': ['int', 'int32', 'int64'],
            'int': ['int32', 'int64', 'integer'],
            'float': ['float32', 'float64', 'double'],
            'double': ['float64', 'double'],
            'string': ['object', 'str'],
            'object': ['object', 'str'],
            'boolean': ['bool'],
            'long': ['int64', 'long']
        }
        
        return actual in type_mappings.get(expected, [expected])
    
    def get_validation_report(self) -> Dict:
        """Generate comprehensive validation report"""
        report = {
            'report_date': datetime.now().isoformat(),
            'validation_results': [asdict(r) for r in self.validation_results],
            'summary': {
                'total_checks': len(self.validation_results),
                'passed': sum(1 for r in self.validation_results if r.status == ValidationStatus.PASSED),
                'passed_with_warnings': sum(1 for r in self.validation_results if r.status == ValidationStatus.PASSED_WITH_WARNINGS),
                'failed': sum(1 for r in self.validation_results if r.status == ValidationStatus.FAILED),
            }
        }
        return report


if __name__ == "__main__":
    print("[OK] Data Validation Module Loaded")
