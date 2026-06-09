"""
Steel Supply Chain - Kafka Configuration (Portable Version)
"""
import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

TOPICS = {
    "market":     "steel_market_prices",
    "orders":     "steel_orders",
    "production": "steel_production",
    "shipments":  "steel_shipments",
}

# Dynamically find the root directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

DATA_FILES = {
    "market":     os.path.join(DATA_DIR, "market_data.csv"),
    "orders":     os.path.join(DATA_DIR, "orders.csv"),
    "production": os.path.join(DATA_DIR, "production.csv"),
    "shipments":  os.path.join(DATA_DIR, "shipments.csv"),
}

INTERVALS = {
    "market":     5,
    "orders":     10,
    "production": 15,
    "shipments":  20,
}

BATCH_SIZES = {
    "market":     1,
    "orders":     5,
    "production": 3,
    "shipments":  2,
}