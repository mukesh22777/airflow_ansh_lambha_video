import json
from notebooks.common_utils import normalize_service_tier


def test_normalize_service_tier():
    assert normalize_service_tier("premium") == "PREMIUM"
    assert normalize_service_tier("Gold") == "GOLD"
    assert normalize_service_tier("unknown") == "STANDARD"


def test_etl_output_serialization(tmp_path):
    sample_customers = [
        {"customer_id": 1, "name": "Test User", "service_level": "basic", "region": "East"}
    ]
    output_file = tmp_path / "customer_etl_output.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sample_customers, f)

    with open(output_file, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    assert loaded[0]["customer_id"] == 1
    assert loaded[0]["name"] == "Test User"
