import os
import json
from elasticsearch import Elasticsearch

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Elasticsearch Client")


class ElasticsearchClient:
    def __init__(self):
        """
        Initializes the Elasticsearch client based on system configurations.
        """
        es_conf = configs.elasticsearch
        self.url = f"{es_conf.scheme}://{es_conf.host}:{es_conf.port}"
        
        print(f"Connecting to Elasticsearch at {self.url}...")
        self.client = Elasticsearch(self.url)
        
        try:
            # Use info() instead of ping() to get more details if connection fails (e.g. 400 Bad Request body)
            info = self.client.info()
            logger.info(f"Connected successfully to Elasticsearch! Cluster: {info.get('cluster_name')}, Version: {info.get('version', {}).get('number')}")
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            raise e

    @staticmethod
    def _load_mapping_from_file(mapping_file_name: str) -> dict:
        """
        Reads index mapping configuration from a JSON file.
        """
        current_dir = os.path.dirname(__file__)
        file_path = os.path.join(current_dir, "mappings", mapping_file_name)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Error: Mapping file not found at {file_path}")
            
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding JSON file {file_path}: {e}")

    def _create_index_if_not_exists(self, index_name: str, mapping_file: str):
        """
        Helper function to create an index if it doesn't exist using a mapping file.
        """
        if self.client.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists.")
            return

        logger.info(f"Creating index '{index_name}'...")
        mapping = self._load_mapping_from_file(mapping_file)
        self.client.indices.create(index=index_name, body=mapping)
        logger.info(f"Index '{index_name}' created successfully.")

    def create_market_prices_index(self):
        """Creates the index for market prices data."""
        self._create_index_if_not_exists("crypto_market_prices", "crypto_market_prices.json")

    def create_trending_metrics_index(self):
        """Creates the index for calculated trending metrics."""
        self._create_index_if_not_exists("crypto_trending_metrics", "crypto_trending_metrics.json")

    def create_whale_alerts_index(self):
        """Creates the index for whale alert transactions."""
        self._create_index_if_not_exists("crypto_whale_alerts", "crypto_whale_alerts.json")


if __name__ == "__main__":
    es_client = ElasticsearchClient()
    es_client.create_market_prices_index()
    es_client.create_trending_metrics_index()
    es_client.create_whale_alerts_index()
    logger.info("Initialization complete.")
