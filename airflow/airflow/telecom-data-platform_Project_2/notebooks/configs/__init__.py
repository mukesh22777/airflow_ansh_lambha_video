import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_FILE = os.path.join(BASE_DIR, "..", "data", "customer_source.json")
REGION_SCORE_MAP = {
    "North": 10,
    "South": 8,
    "East": 9,
    "West": 7
}
