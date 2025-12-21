import click
from typing import Tuple
from storage.elasticsearch.elasticsearch_client import ElasticsearchClient
from storage.elasticsearch import INDEX_METADATA
from utils.logger_utils import get_logger

logger = get_logger("Init Elasticsearch Schema")

@click.command()
@click.option('--all', 'create_all', is_flag=True, default=False, help="Initialize all indices.")
@click.option(
    '--indexes',
    multiple=True,
    type=click.Choice(list(INDEX_METADATA.keys())),
    help="Specify indices to initialize (e.g., crypto_market_prices)."
)
def init_elasticsearch_schema(create_all: bool, indexes: Tuple[str, ...]):
    """
    Initializes Elasticsearch indices and mappings.
    """
    logger.info("Starting Elasticsearch schema initialization...")
    
    try:
        es_client = ElasticsearchClient()
        
        if create_all or not indexes:
            if not create_all and not indexes:
                logger.info("No specific options provided. Defaulting to initializing ALL indices.")
            es_client.initialize_indices()
        else:
            es_client.initialize_indices(indices=list(indexes))
        
        logger.info("Elasticsearch schema initialization completed successfully.")
        
    except Exception as e:
        logger.exception(f"Failed to initialize Elasticsearch schema: {e}")
        raise e
