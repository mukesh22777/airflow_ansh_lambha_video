import json
from pathlib import Path

def merge_files(login_path, product_path, checkout_path, merged_path):
    merged_path = Path(merged_path)
    merged_path.parent.mkdir(parents=True, exist_ok=True)

    def load_json(path):
        if not path:
            return []
        p = Path(path)
        if not p.exists():
            return []
        with open(p) as f:
            return json.load(f)

    login = load_json(login_path)
    product = load_json(product_path)
    checkout = load_json(checkout_path)

    merged = {
        "login_users": login,
        "product_users": product,
        "checkout_users": checkout
    }

    with open(merged_path, "w") as f:
        json.dump(merged, f, indent=2)

    return str(merged_path)



def calculate_checkout_amount(checkout_path):
    with open(checkout_path) as f:
        checkout = json.load(f)

    return sum(item.get("total_value", 0) for item in checkout)
