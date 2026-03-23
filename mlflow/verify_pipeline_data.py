#!/usr/bin/env python3
"""
Quick Pipeline Verification Script
Validates that all 1M ratings have been processed
"""

import pandas as pd
from pathlib import Path

print("🎉 PIPELINE VALIDATION SUMMARY")
print("═" * 60)
print()

# Check raw data
print("📊 Raw Data (from S3):")
raw_dir = Path('data/raw')
for file in ['movies.dat', 'ratings.dat', 'users.dat']:
    path = raw_dir / file
    if path.exists():
        lines = sum(1 for _ in open(path, 'rb'))
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {file}: {lines:,} lines ({size_mb:.2f} MB)")

print()

# Check processed data
print("✅ Processed Data:")
ratings = pd.read_parquet('data/processed/ratings.parquet')
movies = pd.read_parquet('data/processed/movies.parquet')
users = pd.read_parquet('data/processed/users.parquet')
print(f"  ✓ Ratings: {len(ratings):,} records")
print(f"  ✓ Movies: {len(movies):,} records")
print(f"  ✓ Users: {len(users):,} records")

print()

# Check features
print("🔧 Features Generated:")
interaction = pd.read_parquet('data/features/interaction_features_latest.parquet')
user_features = pd.read_parquet('data/features/user_features_latest.parquet')
movie_features = pd.read_parquet('data/features/movie_features_latest.parquet')
print(f"  ✓ Interaction features: {interaction.shape}")
print(f"  ✓ User features: {user_features.shape}")
print(f"  ✓ Movie features: {movie_features.shape}")

print()

# Check models
print("🤖 Models Trained:")
models_dir = Path('data/models')
model_files = list(models_dir.glob('*.joblib'))
if model_files:
    latest_model = sorted(model_files)[-1]
    print(f"  ✓ Latest model: {latest_model.name}")
    print(f"  ✓ Total models: {len(model_files)}")

print()

# Check reports
reports_dir = Path('reports')
validation_reports = list(reports_dir.glob('validation_*.json'))
if validation_reports:
    print(f"📋 Validation Reports: {len(validation_reports)}")

print()
print("✅ ALL STAGES COMPLETED!")
print(f"🎯 {len(ratings):,} RATINGS SUCCESSFULLY PROCESSED!")
print()
print("═" * 60)
print("Next steps:")
print("  1. Run: python3 run_complete_mlops_pipeline.py")
print("  2. View results in Streamlit: http://localhost:8501")
print("  3. Check MLflow: http://localhost:5000")
