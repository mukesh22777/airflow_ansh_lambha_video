from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


def validate_required_columns(actual_cols: Iterable[str], required_cols: Iterable[str]) -> None:
    missing = [col for col in required_cols if col not in set(actual_cols)]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def assert_supported_source(source_format: str) -> None:
    supported = {"csv", "json"}
    if source_format.lower() not in supported:
        raise ValueError(f"Unsupported source format '{source_format}'. Expected one of {supported}.")


def is_source_fresh(source_path: str, freshness_hours: int) -> bool:
    path = Path(source_path)
    if not path.exists():
        return False
    modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return modified_at >= datetime.now(timezone.utc) - timedelta(hours=freshness_hours)
