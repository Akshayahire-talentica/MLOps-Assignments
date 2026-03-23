#!/usr/bin/env python3
"""
Great Expectations Validation Runner
====================================

Validates datasets using Great Expectations and stores validation results.
Supports S3-backed data sources and uploads reports back to S3.

Usage:
  python src/data_validation/run_ge_validation.py --stage raw
  python src/data_validation/run_ge_validation.py --stage processed --force-profile
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add repo root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from src.data_validation.ge_utils import (
    load_ge_config,
    get_context,
    ensure_report_dirs,
    get_stage_config,
    build_batch_request,
    ensure_expectation_suite,
    run_checkpoint,
)
from src.data_ingestion.s3_storage import S3DataStorage


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("ge_validation")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def _result_to_dict(result: Any) -> Dict[str, Any]:
    if hasattr(result, "to_json_dict"):
        try:
            return result.to_json_dict()
        except Exception:
            pass
    if hasattr(result, "to_dict"):
        try:
            return result.to_dict()
        except Exception:
            pass
    return {"result": str(result)}


def _upload_reports(cfg: Dict[str, Any], reports_dir: Path, logger: logging.Logger) -> None:
    reporting_cfg = cfg.get("reporting", {})
    upload_cfg = reporting_cfg.get("s3_upload", {})

    if not upload_cfg.get("enabled", False):
        logger.info("[S3] Upload disabled for GE reports")
        return

    bucket_name = upload_cfg.get("bucket_name")
    if not bucket_name:
        logger.warning("[S3] No bucket name configured for GE report upload")
        return

    prefix = upload_cfg.get("prefix", "reports/great_expectations")
    region = upload_cfg.get("region", "ap-south-1")
    profile = upload_cfg.get("profile")

    logger.info("[S3] Uploading GE reports to s3://%s/%s", bucket_name, prefix)
    storage = S3DataStorage(
        bucket_name=bucket_name,
        region=region,
        profile=profile,
    )
    storage.upload_directory(str(reports_dir), prefix, exclude_patterns=["*.ipynb", "*.tmp"])


def run_ge_validation(
    config_path: str,
    stage: str,
    force_profile: bool = False,
    skip_upload: bool = False,
    logger: Optional[logging.Logger] = None,
) -> int:
    logger = logger or setup_logging()
    logger.info("=" * 80)
    logger.info("STARTING GREAT EXPECTATIONS VALIDATION")
    logger.info("=" * 80)

    try:
        cfg = load_ge_config(config_path)

        ge_cfg = cfg.get("ge", {})
        context_root = ge_cfg.get("context_root_dir", "great_expectations")
        context = get_context(context_root_dir=context_root)

        reporting_cfg = cfg.get("reporting", {})
        reports_dir = ensure_report_dirs(reporting_cfg.get("output_path", "reports/great_expectations"))

        stage_cfg = get_stage_config(cfg, stage)
        datasets = stage_cfg.get("datasets", {})
        if not datasets:
            raise ValueError(f"No datasets configured for stage '{stage}'")

        results = []
        for dataset_name, dataset_cfg in datasets.items():
            logger.info("Validating dataset: %s/%s", stage, dataset_name)
            batch_request, asset = build_batch_request(
                context=context,
                cfg=cfg,
                stage=stage,
                dataset_name=dataset_name,
                dataset_cfg=dataset_cfg,
            )

            suite_name = f"{stage}__{dataset_name}__suite"
            ensure_expectation_suite(
                context=context,
                batch_request=batch_request,
                asset=asset,
                suite_name=suite_name,
                force_profile=force_profile,
            )

            checkpoint_name = f"{stage}__{dataset_name}__checkpoint"
            checkpoint_result = run_checkpoint(context, asset, suite_name, checkpoint_name)

            payload = _result_to_dict(checkpoint_result)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_path = reports_dir / "validations" / f"{stage}__{dataset_name}__{ts}.json"
            with open(result_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            success = payload.get("success")
            results.append(
                {
                    "dataset": dataset_name,
                    "suite": suite_name,
                    "checkpoint": checkpoint_name,
                    "result_path": str(result_path),
                    "success": success,
                }
            )

        try:
            context.build_data_docs()
        except Exception as e:
            logger.warning("GE data docs build failed: %s", e)

        if not skip_upload:
            _upload_reports(cfg, reports_dir, logger)

        summary_path = reports_dir / f"ge_validation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w") as f:
            json.dump({"stage": stage, "results": results}, f, indent=2)

        logger.info("[OK] GE validation completed")
        logger.info("Summary: %s", summary_path)
        return 0

    except Exception as e:
        logger.error("[FAILED] GE validation failed: %s", e, exc_info=True)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Great Expectations validation")
    parser.add_argument(
        "--config",
        type=str,
        default="config/ge_validation.yaml",
        help="Path to GE configuration file",
    )
    parser.add_argument(
        "--stage",
        type=str,
        default="processed",
        help="Stage to validate (raw, processed, features, etc.)",
    )
    parser.add_argument(
        "--force-profile",
        action="store_true",
        help="Force suite profiling even if suite exists",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip uploading GE reports to S3",
    )
    args = parser.parse_args()

    return run_ge_validation(
        config_path=args.config,
        stage=args.stage,
        force_profile=args.force_profile,
        skip_upload=args.skip_upload,
    )


if __name__ == "__main__":
    sys.exit(main())
