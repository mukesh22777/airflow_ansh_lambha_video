import json
import os
from datetime import datetime, timezone
from typing import Optional


def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def read_watermark(state_file: str) -> Optional[str]:
    if not os.path.exists(state_file):
        return None
    with open(state_file, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload.get("last_successful_load_date")


def read_watermark_dt(state_file: str) -> Optional[datetime]:
    value = read_watermark(state_file)
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_watermark(state_file: str, load_date_iso: str, env: str) -> None:
    _ensure_parent(state_file)
    payload = {
        "env": env,
        "last_successful_load_date": load_date_iso,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    with open(state_file, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
