import click
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient
from utils.logger_utils import get_logger

logger = get_logger("Init ClickHouse Schema")

@click.command()
def init_clickhouse_schema():
    """
    Initializes ClickHouse database schema (Tables, Kafka Engines, Materialized Views).
    """
    logger.info("Starting ClickHouse schema initialization...")
    
    ch_conf = configs.clickhouse
    storage_uris = f"{ch_conf.host}:{ch_conf.port}"
    
    try:
        client = ClickHouseClient(
            storage_uris=storage_uris,
            database=ch_conf.database,
            user=ch_conf.user,
            password=ch_conf.password
        )
        
        # Create DB if not exists
        client.create_database()
        
        # Initialize Schema (Tables + Kafka + MVs)
        client.initialize_schema()
        
        logger.info("Schema initialization completed successfully.")
        
    except Exception as e:
        logger.exception(f"Failed to initialize ClickHouse schema: {e}")
        raise e
    finally:
        if 'client' in locals():
            client.close()
