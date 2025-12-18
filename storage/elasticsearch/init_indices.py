import os
import sys
import json
from elasticsearch import Elasticsearch

# Cấu hình kết nối
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_SCHEME = os.getenv("ES_SCHEME", "http")

def get_es_client():
    """Tạo kết nối đến Elasticsearch"""
    url = f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"
    print(f"Connecting to Elasticsearch at {url}...")
    client = Elasticsearch(url)
    if client.ping():
        print("Connected successfully!")
    else:
        print("Could not connect to Elasticsearch.")
        sys.exit(1)
    return client

def _load_mapping_from_file(mapping_file_name: str) -> dict:
    """Đọc cấu hình mapping từ file JSON."""
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "mappings", mapping_file_name)
    
    if not os.path.exists(file_path):
        print(f"Error: Mapping file not found at {file_path}")
        sys.exit(1)
        
    with open(file_path, 'r') as f:
        return json.load(f)

def create_market_prices_index(es):
    index_name = "crypto_market_prices"
    # Đọc mapping từ file JSON
    mapping = _load_mapping_from_file("crypto_market_prices.json")
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

def create_trending_metrics_index(es):
    index_name = "crypto_trending_metrics"
    # Đọc mapping từ file JSON
    mapping = _load_mapping_from_file("crypto_trending_metrics.json")
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

if __name__ == "__main__":
    es_client = get_es_client()
    create_market_prices_index(es_client)
    create_trending_metrics_index(es_client)
    print("Initialization complete.")