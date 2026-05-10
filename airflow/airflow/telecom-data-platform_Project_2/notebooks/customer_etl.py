import os
import json
from notebooks.common_utils import normalize_service_tier
from notebooks.configs import settings

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))


def load_raw_customers():
    with open(settings.SOURCE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def enrich_customers(customers):
    return [
        {
            "customer_id": cust["customer_id"],
            "full_name": cust["name"].strip(),
            "service_tier": normalize_service_tier(cust["service_level"]),
            "region_code": cust["region"][0].upper(),
            "score": settings.REGION_SCORE_MAP.get(cust["region"], 0)
        }
        for cust in customers
    ]


def persist_customers(customers):
    os.makedirs(DATA_DIR, exist_ok=True)
    target_file = os.path.join(DATA_DIR, "customer_etl_output.json")
    with open(target_file, "w", encoding="utf-8") as f:
        json.dump(customers, f, indent=2)
    return target_file


if __name__ == "__main__":
    raw_customers = load_raw_customers()
    enriched = enrich_customers(raw_customers)
    output_path = persist_customers(enriched)
    print(f"ETL completed. Output written to {output_path}")
