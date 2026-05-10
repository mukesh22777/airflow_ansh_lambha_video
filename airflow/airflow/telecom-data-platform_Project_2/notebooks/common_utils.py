import re


def normalize_service_tier(service_level: str) -> str:
    normalized = service_level.strip().lower()
    mapping = {
        "basic": "BASIC",
        "standard": "STANDARD",
        "premium": "PREMIUM",
        "gold": "GOLD",
        "platinum": "PLATINUM"
    }
    return mapping.get(normalized, "STANDARD")


def sanitize_text(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9 \-]", "", value).strip()
