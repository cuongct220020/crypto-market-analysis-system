from typing import Any, Dict, List, Optional
import aiohttp
import asyncio

from config.configs import settings
from utils.logger_utils import get_logger

logger = get_logger("Coin Gecko API")

class CoinGeckoAPI:
    """
    A class to interact with CoinGecko API for collecting cryptocurrency market data.
    """
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": f"{settings.app.name}/1.0"}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
    
    async def get_coin_list(self) -> List[Dict[str, Any]]:
        """
        Get the list of all supported coins
        """
        logger.info("Fetching coin list from CoinGecko API...")
        try:
            url = f"{self.base_url}/coins/list"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved {len(data)} coins from CoinGecko")
                    return data
                else:
                    logger.error(f"Failed to fetch coin list. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching coin list from CoinGecko: {str(e)}")
            raise
    
    async def get_coin_markets(self, vs_currency: str = "usd", limit: int = 250) -> List[Dict[str, Any]]:
        """
        Get the top coin markets by market cap
        """
        logger.info(f"Fetching top {limit} coin markets from CoinGecko in {vs_currency}...")
        try:
            url = f"{self.base_url}/coins/markets"
            params = {
                "vs_currency": vs_currency,
                "order": "market_cap_desc",
                "per_page": limit,
                "page": 1,
                "sparkline": "false"
            }
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved {len(data)} coin markets from CoinGecko")
                    return data
                else:
                    logger.error(f"Failed to fetch coin markets. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching coin markets from CoinGecko: {str(e)}")
            raise
    
    async def get_coin_price(self, coin_id: str, vs_currencies: List[str]) -> Dict[str, Any]:
        """
        Get current price for a specific coin
        """
        logger.debug(f"Fetching price for coin {coin_id} in {vs_currencies}...")
        try:
            url = f"{self.base_url}/simple/price"
            params = {
                "ids": coin_id,
                "vs_currencies": ",".join(vs_currencies)
            }
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved price for coin {coin_id}")
                    return data
                else:
                    logger.error(f"Failed to fetch price for {coin_id}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching price for coin {coin_id} from CoinGecko: {str(e)}")
            raise
    
    async def get_coin_history(self, coin_id: str, date: str) -> Dict[str, Any]:
        """
        Get historical data for a specific coin on a specific date
        """
        logger.debug(f"Fetching historical data for coin {coin_id} on date {date}...")
        try:
            url = f"{self.base_url}/coins/{coin_id}/history"
            params = {"date": date}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved historical data for coin {coin_id} on {date}")
                    return data
                else:
                    logger.error(f"Failed to fetch history for {coin_id} on {date}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching historical data for {coin_id} from CoinGecko: {str(e)}")
            raise