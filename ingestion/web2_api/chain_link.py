from typing import Any, Dict, List, Optional
import aiohttp
import asyncio

from config.settings import settings
from utils.logger_utils import get_logger

logger = get_logger("Chainlink API")

class ChainlinkAPI:
    """
    A class to interact with Chainlink API for collecting real-time price data.
    """
    
    def __init__(self):
        self.base_url = "https://market.link/api/v1"
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
    
    async def get_price_feeds(self, page: int = 1, per_page: int = 100) -> Dict[str, Any]:
        """
        Get the list of available price feeds
        """
        logger.info(f"Fetching price feeds from Chainlink (page={page}, per_page={per_page})...")
        try:
            # Note: This is a hypothetical API call as Chainlink's actual API structure varies
            url = f"{self.base_url}/price_feeds"
            params = {
                "page": page,
                "per_page": per_page
            }
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved {len(data.get('data', []))} price feeds from Chainlink")
                    return data
                else:
                    logger.error(f"Failed to fetch price feeds. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching price feeds from Chainlink: {str(e)}")
            raise
    
    async def get_aggregator_prices(self, pair_name: str) -> Dict[str, Any]:
        """
        Get the latest price for a specific feed/asset pair
        """
        logger.debug(f"Fetching price for pair: {pair_name}")
        try:
            # Note: This is a hypothetical API call as Chainlink's actual API structure varies
            url = f"{self.base_url}/aggregator_prices"
            params = {"pair": pair_name}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved price for pair: {pair_name}")
                    return data
                else:
                    logger.error(f"Failed to fetch price for {pair_name}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching price for {pair_name} from Chainlink: {str(e)}")
            raise
    
    async def get_recent_round_data(self, feed_address: str) -> Dict[str, Any]:
        """
        Get recent round data for a specific feed
        """
        logger.debug(f"Fetching recent round data for feed: {feed_address}")
        try:
            # Note: This is a hypothetical API call
            url = f"{self.base_url}/recent_round_data"
            params = {"feed": feed_address}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved recent round data for feed: {feed_address}")
                    return data
                else:
                    logger.error(f"Failed to fetch round data for {feed_address}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching round data for {feed_address} from Chainlink: {str(e)}")
            raise

    async def get_aggregators_by_network(self, network: str = "ethereum") -> List[Dict[str, Any]]:
        """
        Get all aggregators for a specific network
        """
        logger.info(f"Fetching aggregators for network: {network}")
        try:
            # Note: This is a hypothetical API call
            url = f"{self.base_url}/aggregators"
            params = {"network": network}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved {len(data.get('data', []))} aggregators for {network}")
                    return data.get('data', [])
                else:
                    logger.error(f"Failed to fetch aggregators for {network}. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching aggregators for {network} from Chainlink: {str(e)}")
            raise