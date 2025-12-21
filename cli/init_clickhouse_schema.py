import click
from typing import Tuple
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient
from storage.clickhouse.models import TABLE_METADATA
from utils.logger_utils import get_logger

logger = get_logger("Init ClickHouse Schema")

@click.command()
@click.option('--all', 'create_all', is_flag=True, default=False, help="Initialize all schemas.")
@click.option(
    '--tables',
    multiple=True,
    type=click.Choice(list(TABLE_METADATA.keys())),
    help="Specify tables to initialize (e.g., blocks, transactions)."
)
def init_clickhouse_schema(create_all: bool, tables: Tuple[str, ...]):
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
        if create_all or not tables:
            if not create_all and not tables:
                logger.info("No specific options provided. Defaulting to initializing ALL schemas.")
            client.initialize_schema()
        else:
            client.initialize_schema(tables=list(tables))
        
        logger.info("Schema initialization completed successfully.")
        
    except Exception as e:
        logger.exception(f"Failed to initialize ClickHouse schema: {e}")
        raise e
    finally:
        if 'client' in locals():
            client.close()
