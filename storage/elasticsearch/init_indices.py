import os
import sys
import json
from elasticsearch import Elasticsearch

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import singleton config object
from config.configs import configs


def get_es_client() -> Elasticsearch:
    """
    Creates and returns an Elasticsearch client based on system configurations.
    """
    es_conf = configs.elasticsearch
    url = f"{es_conf.scheme}://{es_conf.host}:{es_conf.port}"
    
    print(f"Connecting to Elasticsearch at {url}...")
    client = Elasticsearch(url)
    
    try:
        if client.ping():
            print("Connected successfully!")
            return client
        else:
            print("Could not connect to Elasticsearch (Ping failed).")
            sys.exit(1)
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        sys.exit(1)


def _load_mapping_from_file(mapping_file_name: str) -> dict:
    """
    Reads index mapping configuration from a JSON file.
    """
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "mappings", mapping_file_name)
    
    if not os.path.exists(file_path):
        print(f"Error: Mapping file not found at {file_path}")
        sys.exit(1)
        
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file {file_path}: {e}")
        sys.exit(1)


def _create_index_if_not_exists(es_client: Elasticsearch, index_name: str, mapping_file: str):
    """
    Helper function to create an index if it doesn't exist using a mapping file.
    """
    if es_client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists.")
        return

    print(f"Creating index '{index_name}'...")
    mapping = _load_mapping_from_file(mapping_file)
    es_client.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully.")


def create_market_prices_index(es_client: Elasticsearch):
    """Creates the index for market prices data."""
    _create_index_if_not_exists(es_client, "crypto_market_prices", "crypto_market_prices.json")


def create_trending_metrics_index(es_client: Elasticsearch):
    """Creates the index for calculated trending metrics."""
    _create_index_if_not_exists(es_client, "crypto_trending_metrics", "crypto_trending_metrics.json")


def create_whale_alerts_index(es_client: Elasticsearch):
    """Creates the index for whale alert transactions."""
    _create_index_if_not_exists(es_client, "crypto_whale_alerts", "crypto_whale_alerts.json")


if __name__ == "__main__":
    es = get_es_client()
    create_market_prices_index(es)
    create_trending_metrics_index(es)
    create_whale_alerts_index(es)
    print("Initialization complete.")
