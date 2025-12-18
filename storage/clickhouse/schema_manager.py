import logging
from typing import List
from pathlib import Path

try:
    from clickhouse_driver import Client
except ImportError:
    raise ImportError(
        "clickhouse-driver is required. Install with: pip install clickhouse-driver"
    )

logger = logging.getLogger(__name__)


class ClickHouseSchemaManager:
    """Manages ClickHouse schema initialization and operations."""
    
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
        
        try:
            self.client = Client(
                host=first_host,
                port=first_port,
                user=user,
                password=password
            )
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
        
        Args:
            storage_uris: Format: 'host1:port1,host2:port2,host3:port3'
            
        Returns:
            List of (host, port) tuples
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
            result = self.client.execute(query)
            logger.debug(f"Query executed successfully")
            return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    @staticmethod
    def _parse_sql_statements(sql_content: str) -> List[str]:
        """
        Parse SQL file into individual CREATE/DROP statements.
        """
        import re
        
        statements = []
        
        # Split by CREATE or DROP, preserving the keyword
        parts = re.split(r'(?=CREATE|DROP)', sql_content, flags=re.IGNORECASE)
        
        for part in parts:
            part = part.strip()
            if not part or part.startswith('--'):
                continue
            
            # Extract statement up to semicolon
            if ';' in part:
                stmt = part[:part.rfind(';') + 1].strip()
            else:
                stmt = part.strip()
                if not stmt.endswith(';'):
                    stmt += ';'
            
            if stmt and len(stmt) > 10 and ';' in stmt:
                statements.append(stmt)
        
        return statements
    
    def initialize_schema(self, schema_file_path: str) -> None:
        """
        Initialize database schema from SQL file.
        
        This should be called BEFORE any data ingestion/publication to Kafka.
        
        Args:
            schema_file_path: Path to schema SQL file
            
        Raises:
            FileNotFoundError: If schema file not found
            Exception: If any statement fails
        """
        path = Path(schema_file_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file_path}")
        
        logger.info(f"Initializing ClickHouse schema from: {schema_file_path}")
        
        with open(path, 'r') as f:
            sql_content = f.read()
        
        statements = self._parse_sql_statements(sql_content)
        
        if not statements:
            logger.warning("No SQL statements found in schema file")
            return
        
        logger.info(f"Executing {len(statements)} SQL statements...")
        
        executed_count = 0
        for i, statement in enumerate(statements, 1):
            try:
                logger.debug(f"[{i}/{len(statements)}] Executing statement...")
                self.execute_sql(statement)
                executed_count += 1
            except Exception as e:
                logger.error(f"Failed at statement {i}/{len(statements)}: {str(e)}")
                raise
        
        logger.info(f"Schema initialization completed: {executed_count} statements executed")
    
    def create_database(self) -> None:
        """Create database if it doesn't exist."""
        query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
        try:
            self.execute_sql(query)
            logger.info(f"Database '{self.database}' ready")
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise
    
    def list_tables(self) -> List[str]:
        """List all tables in database. Returns empty list if database doesn't exist."""
        query = f"SELECT name FROM system.tables WHERE database = '{self.database}'"
        try:
            result = self.execute_sql(query)
            tables = [row[0] for row in result]
            if tables:
                logger.info(f"Found {len(tables)} tables in database '{self.database}'")
            return tables
        except Exception as e:
            if "doesn't exist" in str(e) or "does not exist" in str(e):
                logger.info(f"Database '{self.database}' does not exist yet - will be created")
                return []
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def describe_table(self, table_name: str) -> None:
        """Describe table structure."""
        query = f"DESCRIBE TABLE {self.database}.{table_name}"
        try:
            result = self.execute_sql(query)
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
            result = self.client.execute("SELECT 1")
            return result == [(1,)]
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close connection to ClickHouse."""
        if self.client:
            try:
                self.client.disconnect()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")


def get_schema_file_path() -> str:
    """Get absolute path to schema file."""
    current_dir = Path(__file__).parent
    return str(current_dir / 'schemas' / 'ethereum_schema_all.sql')
