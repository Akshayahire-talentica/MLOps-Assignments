import glob
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import yaml

import great_expectations as ge
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.validation_definition import ValidationDefinition

logger = logging.getLogger(__name__)


def load_ge_config(config_path: str = "config/ge_validation.yaml") -> Dict[str, Any]:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_context(context_root_dir: str = "great_expectations"):
    Path(context_root_dir).mkdir(parents=True, exist_ok=True)
    try:
        return ge.get_context(context_root_dir=context_root_dir)
    except Exception as e:
        raise RuntimeError(
            f"Great Expectations context not initialized at '{context_root_dir}'. "
            "Run `great_expectations init` or ensure a valid great_expectations.yml exists."
        ) from e


def ensure_report_dirs(base_dir: str = "reports/great_expectations") -> Path:
    base = Path(base_dir)
    (base / "validations").mkdir(parents=True, exist_ok=True)
    (base / "data_docs").mkdir(parents=True, exist_ok=True)
    return base


def get_stage_config(cfg: Dict[str, Any], stage: str) -> Dict[str, Any]:
    stages = cfg.get("stages", {})
    if stage not in stages:
        raise ValueError(f"Stage '{stage}' not found in config")
    return stages[stage]


def _get_s3_base(cfg: Dict[str, Any], stage: str) -> str:
    env_key = f"GE_S3_{stage.upper()}_BASE"
    return os.getenv(env_key) or cfg.get("s3_base", {}).get(stage, "") or ""


def _strip_stage_prefix(path: str, stage: str) -> str:
    normalized = path.replace("\\", "/")
    prefix = f"data/{stage}/"
    if normalized.startswith(prefix):
        return normalized[len(prefix):]
    return os.path.basename(normalized)


def resolve_dataset_path(cfg: Dict[str, Any], stage: str, path: str) -> str:
    if path.startswith("s3://"):
        return path

    s3_base = _get_s3_base(cfg, stage)
    if s3_base:
        relative = _strip_stage_prefix(path, stage)
        return f"{s3_base.rstrip('/')}/{relative.lstrip('/')}"

    return path


def _s3_glob(path: str) -> str:
    try:
        import s3fs
    except Exception as e:
        raise RuntimeError("s3fs is required to glob S3 paths") from e

    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    fs = s3fs.S3FileSystem(anon=False)
    
    # Handle recursive globs (**/) by using s3fs glob which supports them
    matches = fs.glob(f"{bucket}/{key}")
    
    # If no direct matches, try without the filename pattern to find subdirs
    if not matches and "**" in key:
        # Try a more flexible pattern
        logger.info(f"No direct matches for {path}, trying recursive search...")
        # For patterns like processed/ratings/**/*.parquet, try processed/ratings/*/*.parquet
        alt_key = key.replace("**", "*")
        matches = fs.glob(f"{bucket}/{alt_key}")
    
    if not matches:
        raise FileNotFoundError(f"No S3 objects match {path}")

    def _mtime(obj_key: str):
        try:
            info = fs.info(obj_key)
            return info.get("LastModified") or info.get("mtime")
        except Exception:
            return None

    # Sort by modification time and get the latest
    matches_sorted = sorted(matches, key=lambda k: _mtime(k) or datetime.min)
    latest = matches_sorted[-1]
    
    logger.info(f"Found {len(matches)} file(s) matching {path}, using latest: s3://{latest}")
    return f"s3://{latest}"


def resolve_glob_path(path: str) -> str:
    if "*" not in path and "?" not in path:
        return path

    if path.startswith("s3://"):
        return _s3_glob(path)

    matches = glob.glob(path)
    if not matches:
        raise FileNotFoundError(f"No files match {path}")
    return max(matches, key=os.path.getmtime)


def _get_or_create_datasource(context, name: str):
    try:
        return context.data_sources.get(name=name)
    except Exception:
        return context.data_sources.add_pandas(name=name)


def _try_update_asset_path(asset, path: str) -> bool:
    for attr in ("path", "filepath_or_buffer"):
        if hasattr(asset, attr):
            try:
                current = getattr(asset, attr)
            except Exception:
                current = None
            if current == path:
                return True
            try:
                setattr(asset, attr, path)
                return True
            except Exception:
                return False

    for container_attr in ("kwargs", "_kwargs"):
        if hasattr(asset, container_attr):
            data = getattr(asset, container_attr)
            if isinstance(data, dict) and "filepath_or_buffer" in data:
                data["filepath_or_buffer"] = path
                return True

    return False


def _get_or_create_asset(datasource, asset_name: str, path: str, reader_options: Optional[Dict[str, Any]]):
    try:
        asset = datasource.get_asset(name=asset_name)
        if not _try_update_asset_path(asset, path):
            try:
                datasource.delete_asset(name=asset_name)
            except Exception:
                logger.warning(
                    "Asset '%s' exists but path could not be updated; using existing asset.",
                    asset_name,
                )
                return asset
            asset = None
        if asset is not None:
            return asset
    except Exception:
        pass

    lower = path.lower()
    if lower.endswith(".parquet"):
        return datasource.add_parquet_asset(name=asset_name, path=path)

    csv_kwargs = reader_options or {}
    return datasource.add_csv_asset(name=asset_name, filepath_or_buffer=path, **csv_kwargs)


def build_batch_request(
    context,
    cfg: Dict[str, Any],
    stage: str,
    dataset_name: str,
    dataset_cfg: Dict[str, Any],
) -> Tuple[Any, Any]:
    ge_cfg = cfg.get("ge", {})
    path = dataset_cfg.get("path")
    if not path:
        raise ValueError(f"Missing path for dataset '{dataset_name}' in stage '{stage}'")

    resolved = resolve_dataset_path(cfg, stage, path)
    resolved = resolve_glob_path(resolved)

    datasource_name = ge_cfg.get("datasource_name", "pandas_runtime")
    datasource = _get_or_create_datasource(context, datasource_name)
    asset_name = f"{stage}__{dataset_name}"
    asset = _get_or_create_asset(datasource, asset_name, resolved, dataset_cfg.get("reader_options"))
    batch_request = asset.build_batch_request()

    return batch_request, asset


def ensure_expectation_suite(
    context,
    batch_request,
    asset,
    suite_name: str,
    force_profile: bool = False,
) -> None:
    if not force_profile:
        try:
            context.suites.get(name=suite_name)
            return
        except Exception:
            pass

    if hasattr(context, "assistants") and getattr(context.assistants, "onboarding", None):
        try:
            context.assistants.onboarding.run(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
            )
            context.suites.get(name=suite_name)
            return
        except Exception as e:
            logger.warning(f"Onboarding assistant failed for {suite_name}: {e}")

    batch = asset.get_batch(batch_request)
    validator = context.get_validator(
        batch=batch,
        create_expectation_suite_with_name=suite_name,
    )
    try:
        columns = list(validator.columns)
    except Exception:
        columns = []
    if columns:
        validator.expect_table_columns_to_match_ordered_list(columns)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    context.suites.add_or_update(validator.get_expectation_suite())


def run_checkpoint(context, asset, suite_name: str, checkpoint_name: str):
    suite = context.suites.get(name=suite_name)
    try:
        batch_def = asset.get_batch_definition("whole")
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe("whole")

    validation_definition = ValidationDefinition(
        name=f"{suite_name}__validation",
        data=batch_def,
        suite=suite,
    )
    checkpoint = Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
    )
    context.checkpoints.add_or_update(checkpoint)
    return checkpoint.run()
