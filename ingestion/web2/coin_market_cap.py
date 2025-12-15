from typing import Any, Dict, List, Optional
import aiohttp
import asyncio

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Coin Market Cap API")

class CoinMarketCapAPI:
    """
    A class to interact with CoinMarketCap API for collecting cryptocurrency market data.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.api_key = api_key or configs.get("COINMARKETCAP_API_KEY", "")
        self.session: Optional[aiohttp.ClientSession] = None
        
        if not self.api_key:
            logger.warning("CoinMarketCap API key not found in settings. API calls may be limited or fail.")
    
    async def __aenter__(self):
        """Context manager entry"""
        headers = {"X-CMC_PRO_API_KEY": self.api_key} if self.api_key else {}
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers=headers
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
    
    async def get_listings_latest(self, start: int = 1, limit: int = 100, convert: str = "USD") -> Dict[str, Any]:
        """
        Get the latest listings of all cryptocurrencies
        """
        logger.info(f"Fetching latest listings from CoinMarketCap (start={start}, limit={limit}, convert={convert})...")
        try:
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                "start": str(start),
                "limit": str(limit),
                "convert": convert
            }
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data:
                        logger.info(f"Successfully retrieved {len(data['data'])} listings from CoinMarketCap")
                        return data
                    else:
                        logger.warning("Response from CoinMarketCap does not contain expected data structure")
                        return data
                else:
                    logger.error(f"Failed to fetch listings. Status: {response.status}, Reason: {response.reason}")
                    logger.error(f"Response text: {await response.text()}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching listings from CoinMarketCap: {str(e)}")
            raise
    
    async def get_quotes_latest(self, symbol: str) -> Dict[str, Any]:
        """
        Get the latest quotes for a specific cryptocurrency symbol
        """
        logger.debug(f"Fetching latest quotes for symbol {symbol} from CoinMarketCap...")
        try:
            url = f"{self.base_url}/cryptocurrency/quotes/latest"
            params = {"symbol": symbol}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved quotes for {symbol} from CoinMarketCap")
                    return data
                else:
                    logger.error(f"Failed to fetch quotes for {symbol}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching quotes for {symbol} from CoinMarketCap: {str(e)}")
            raise
    
    async def get_global_metrics(self, convert: str = "USD") -> Dict[str, Any]:
        """
        Get global cryptocurrency market metrics
        """
        logger.info(f"Fetching global metrics from CoinMarketCap (convert={convert})...")
        try:
            url = f"{self.base_url}/global-metrics/quotes/latest"
            params = {"convert": convert} if convert else {}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info("Successfully retrieved global metrics from CoinMarketCap")
                    return data
                else:
                    logger.error(f"Failed to fetch global metrics. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching global metrics from CoinMarketCap: {str(e)}")
            raise