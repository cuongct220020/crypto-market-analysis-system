import click


from cli.get_eth_market_data import get_eth_market_data
from cli.get_eth_latest_token_price import get_eth_latest_token_price
from cli.get_eth_historical_token_data import get_eth_historical_token_data

from cli.get_eth_block_range_by_date import get_eth_block_range_by_date
from cli.stream_ethereum import stream_ethereum


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
cli.add_command(get_eth_latest_token_price, "get_eth_latest_token_price")