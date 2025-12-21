import logging
from typing import List

from sqlalchemy import create_engine, text
from clickhouse_sqlalchemy import make_session

from storage.clickhouse.models import Base, MATERIALIZED_VIEWS_SQL, KAFKA_TABLES_SQL, TABLE_METADATA

logger = logging.getLogger("ClickHouse Client")


class ClickHouseClient:
    """Manages ClickHouse schema initialization and operations using SQLAlchemy."""
    
    def __init__(
        self,
        storage_uris: str,
        database: str = 'crypto',
        user: str = 'default',
        password: str = ''
    ):
        """
        Initialize schema manager.
        
        Args:
            storage_uris: Comma-separated ClickHouse URIs (e.g., 'ch1:9000,ch2:9000,ch3:9000')
            database: Database name
            user: ClickHouse user
            password: ClickHouse password
        """
        self.database = database
        self.storage_uris = storage_uris
        self.hosts = self._parse_uris(storage_uris)
        
        first_host, first_port = self.hosts[0]
        
        # Construct SQLAlchemy connection URL
        # Format: clickhouse+native://user:password@host:port/database
        self.url = f'clickhouse+native://{user}:{password}@{first_host}:{first_port}/{database}'
        
        try:
            self.engine = create_engine(self.url)
            self.session = make_session(self.engine)
            logger.info(
                f"Connected to ClickHouse at {first_host}:{first_port} "
                f"(cluster: {len(self.hosts)} nodes)"
            )
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    @staticmethod
    def _parse_uris(storage_uris: str) -> List[tuple]:
        """
        Parse storage URIs string into list of (host, port) tuples.
        """
        hosts = []
        for uri in storage_uris.split(','):
            uri = uri.strip()
            if ':' in uri:
                host, port = uri.rsplit(':', 1)
                hosts.append((host, int(port)))
            else:
                # Default port
                hosts.append((uri, 9000))
        
        if not hosts:
            raise ValueError("Invalid storage_uris format")
        
        return hosts
    
    def execute_sql(self, query: str) -> any:
        """Execute a single SQL query."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                logger.debug(f"Query executed successfully")
                return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def initialize_schema(self, tables: List[str] = None) -> None:
        """
        Initialize database schema using SQLAlchemy models.
        
        Args:
            tables: List of table names to initialize. If None, initializes all.
        """
        logger.info(f"Initializing ClickHouse schema using SQLAlchemy models...")
        
        target_tables = []
        kafka_sqls = []
        mv_sqls = []

        if tables:
            # Validate table names
            valid_tables = set(TABLE_METADATA.keys())
            invalid_tables = set(tables) - valid_tables
            if invalid_tables:
                raise ValueError(f"Invalid table names: {invalid_tables}. Available: {valid_tables}")
            
            logger.info(f"Initializing specific tables: {tables}")
            for t in tables:
                meta = TABLE_METADATA[t]
                target_tables.append(meta['model'].__table__)
                kafka_sqls.extend(meta['kafka_tables'])
                mv_sqls.extend(meta['materialized_views'])
            
            # Deduplicate SQLs while preserving order
            kafka_sqls = list(dict.fromkeys(kafka_sqls))
            mv_sqls = list(dict.fromkeys(mv_sqls))
            
        else:
            logger.info("Initializing ALL tables.")
            kafka_sqls = KAFKA_TABLES_SQL
            mv_sqls = MATERIALIZED_VIEWS_SQL

        # 1. Create MergeTree Tables (Target)
        try:
            if tables:
                Base.metadata.create_all(self.engine, tables=target_tables)
            else:
                Base.metadata.create_all(self.engine)
            logger.info("Base MergeTree tables created successfully.")
        except Exception as e:
            logger.error(f"Failed to create base MergeTree tables: {e}")
            raise

        # 2. Create Kafka Engine Tables (Raw SQL)
        if kafka_sqls:
            logger.info(f"Creating {len(kafka_sqls)} Kafka Engine tables...")
            for i, kafka_sql in enumerate(kafka_sqls, 1):
                try:
                    logger.debug(f"[{i}/{len(kafka_sqls)}] Creating Kafka table...")
                    self.execute_sql(kafka_sql)
                except Exception as e:
                    logger.error(f"Failed to create Kafka table {i}: {str(e)}")
                    raise

        # 3. Create Materialized Views (Raw SQL)
        if mv_sqls:
            logger.info(f"Creating {len(mv_sqls)} Materialized Views...")
            for i, mv_sql in enumerate(mv_sqls, 1):
                try:
                    logger.debug(f"[{i}/{len(mv_sqls)}] Creating MV...")
                    self.execute_sql(mv_sql)
                except Exception as e:
                    logger.error(f"Failed to create MV {i}: {str(e)}")
                    raise
        
        logger.info("Schema initialization completed.")
    
    def create_database(self) -> None:
        """Create database if it doesn't exist."""
        # Note: SQLAlchemy engine usually connects TO a database.
        # If 'crypto' doesn't exist, connection might fail depending on driver behavior.
        # clickhouse-driver allows connecting to 'default' and switching/creating.
        # For now, we assume the engine url has the db, and we might need to connect to default to create it if it fails.
        
        query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
        try:
            # Try executing on current engine
            self.execute_sql(query)
            logger.info(f"Database '{self.database}' ready")
        except Exception as e:
            # Fallback: try connecting to default database to create 'crypto'
            logger.warning(f"Failed to create database on current connection: {e}. Trying via 'default' db...")
            try:
                # Construct URL for default db
                base_url = self.url.rsplit('/', 1)[0] + '/default'
                temp_engine = create_engine(base_url)
                with temp_engine.connect() as conn:
                    conn.execute(text(query))
                logger.info(f"Database '{self.database}' created via default connection")
            except Exception as e2:
                logger.error(f"Failed to create database via fallback: {e2}")
                raise
    
    def list_tables(self) -> List[str]:
        """List all tables in database."""
        query = f"SELECT name FROM system.tables WHERE database = '{self.database}'"
        try:
            # We cannot easily iterate ResultProxy in all sqlalchemy versions consistently for scalars
            # But clickhouse-driver cursor usually returns list of tuples.
            # with self.engine.connect() as conn:
            #     result = conn.execute(text(query))
            #     return [row[0] for row in result]
            
            # Using raw execute from driver if possible or just standard SA
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [row[0] for row in result]
                
        except Exception as e:
            if "doesn't exist" in str(e) or "does not exist" in str(e):
                logger.info(f"Database '{self.database}' does not exist yet")
                return []
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def describe_table(self, table_name: str) -> None:
        """Describe table structure."""
        query = f"DESCRIBE TABLE {self.database}.{table_name}"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                print(f"\nTable: {self.database}.{table_name}")
                print("=" * 80)
                for row in result:
                    print(row)
        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise
    
    def check_health(self) -> bool:
        """Check ClickHouse connection health."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                # result.scalar() works in SA 1.4+
                val = result.scalar()
                return val == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close connection to ClickHouse."""
        if self.engine:
            self.engine.dispose()
            logger.info("ClickHouse connection closed")
