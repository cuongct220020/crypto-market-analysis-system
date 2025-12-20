import click


from cli.get_eth_market_data import get_eth_market_data
from cli.get_eth_latest_token_price import get_eth_latest_token_price
from cli.get_eth_historical_token_data import get_eth_historical_token_data

from cli.get_eth_block_range_by_date import get_eth_block_range_by_date
from cli.stream_ethereum import stream_ethereum
from cli.init_clickhouse_schema import init_clickhouse_schema
from cli.init_elasticsearch_schema import init_elasticsearch_schema
from cli.import_kibana_dashboard import import_kibana_dashboard


@click.group()
@click.version_option(version="2.4.2")
@click.pass_context
def cli(ctx):
    pass


# Get block number
cli.add_command(get_eth_block_range_by_date, "get_eth_block_range_by_date")

# Ethereum streaming
cli.add_command(stream_ethereum, "stream_ethereum")

# Ethereum market data
cli.add_command(get_eth_market_data, "get_eth_market_data")

# Ethereum token historical data
cli.add_command(get_eth_historical_token_data, "get_eth_historical_token_data")

# Latest price feed
# cli.add_command(get_eth_latest_token_price, "get_eth_latest_token_price")

# Initialize ClickHouse Schema
cli.add_command(init_clickhouse_schema, "init_clickhouse_schema")

# Initialize Elasticsearch Schema
cli.add_command(init_elasticsearch_schema, "init_elasticsearch_schema")

# Import Kibana Dashboard
cli.add_command(import_kibana_dashboard, "import_kibana_dashboard")