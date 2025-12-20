import click
from storage.elasticsearch.elasticsearch_client import ElasticsearchClient
from utils.logger_utils import get_logger

logger = get_logger("Init Elasticsearch Schema")

@click.command()
def init_elasticsearch_schema():
    """
    Initializes Elasticsearch indices and mappings.
    """
    logger.info("Starting Elasticsearch schema initialization...")
    
    try:
        es_client = ElasticsearchClient()
        
        # Initialize indices
        es_client.create_market_prices_index()
        es_client.create_trending_metrics_index()
        es_client.create_whale_alerts_index()
        
        logger.info("Elasticsearch schema initialization completed successfully.")
        
    except Exception as e:
        logger.exception(f"Failed to initialize Elasticsearch schema: {e}")
        raise e
