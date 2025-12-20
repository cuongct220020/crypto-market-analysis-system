import ssl
import asyncio
from typing import Any, Dict, List, Optional
import aiohttp

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Coin Gecko Client")


class CoinGeckoClient(object):
    """
    A class to interact with CoinGecko API for collecting cryptocurrency market data.
    """
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.coingecko_api_key = configs.coingecko.api_key
        self.headers = {"x-cg-demo-api-key": self.coingecko_api_key}
        self.session: Optional[aiohttp.ClientSession] = None
        # SSL Context that ignores verification (Fallback)
        self.ssl_context_unsafe = ssl.create_default_context()
        self.ssl_context_unsafe.check_hostname = False
        self.ssl_context_unsafe.verify_mode = ssl.CERT_NONE
    
    async def __aenter__(self):
        """Context manager entry"""
        # Create a new session with TCPConnector for pooling
        # limit=0 means unlimited connections, or set specific limit if needed
        connector = aiohttp.TCPConnector(limit=100, ssl=self.ssl_context_unsafe, force_close=False)
        
        session_headers = {"User-Agent": f"{configs.app.name}/1.0"}
        if self.headers.get("x-cg-demo-api-key"):
            session_headers.update(self.headers)
            
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30),
            headers=session_headers
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()

    async def _request(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """
        Internal method to handle requests with retries and rate limiting.
        """
        url = f"{self.base_url}/{endpoint}"
        max_retries = 5
        base_wait = 5  # seconds

        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    
                    elif response.status == 429:
                        # Rate limited
                        wait_time = base_wait * (2 ** attempt)
                        logger.warning(f"Rate limited (429) on {endpoint}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    elif response.status == 401:
                        logger.error(f"Unauthorized (401) on {endpoint}. Check your COINGECKO_API_KEY. It might be invalid or missing. "
                                     f"Note: Public/Demo keys are restricted to 365 days of historical data for 'market_chart/range'.")
                        return None
                    
                    else:
                        logger.error(f"Failed to fetch {endpoint}. Status: {response.status}, Reason: {response.reason}")
                        return None
                        
            except Exception as e:
                logger.error(f"Error requesting {endpoint}: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = base_wait * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                else:
                    raise
        
        logger.error(f"Max retries reached for {endpoint}.")
        return None
    
    async def get_coin_list(self, include_platform: bool = False) -> List[Dict[str, Any]]:
        """
        Get the list of all supported coins
        """
        logger.info("Fetching coin list from CoinGecko API...")
        data = await self._request("coins/list", params={"include_platform": str(include_platform).lower()})
        if data:
            logger.info(f"Successfully retrieved {len(data)} coins from CoinGecko")
            return data
        return []
    
    async def get_coin_markets(self, vs_currency: str = "usd", limit: int = 250, page: int = 1, ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get the top coin markets by market cap, or specific coins by IDs.
        """
        logger.info(f"Fetching coin markets page {page} (limit {limit}) from CoinGecko in {vs_currency}...")
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d"  # Add 1h change for Real-time Analytics
        }
        
        if ids:
            # CoinGecko accepts comma-separated IDs
            params["ids"] = ",".join(ids)
            logger.info(f"Filtering by {len(ids)} specific coin IDs.")

        data = await self._request("coins/markets", params=params)
        if data:
            logger.info(f"Successfully retrieved {len(data)} coin markets from CoinGecko (page {page})")
            return data
        return []

    async def get_all_coin_markets(self, vs_currency: str = "usd", max_pages: int = 50, ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch all coin markets by paginating through results.
        """
        logger.info("Fetching ALL coin markets from CoinGecko...")
        all_markets = []
        limit = 250
        
        for page in range(1, max_pages + 1):
            markets = await self.get_coin_markets(vs_currency=vs_currency, limit=limit, page=page, ids=ids)
            if not markets:
                break
            
            all_markets.extend(markets)
            
            # If we got fewer results than the limit, we've reached the end
            if len(markets) < limit:
                break
                
            # Rate limit safety
            await asyncio.sleep(1)
            
        logger.info(f"Total coin markets fetched: {len(all_markets)}")
        return all_markets
    
    async def get_coin_price(self, coin_id: str, vs_currencies: List[str]) -> Dict[str, Any]:
        """
        Get current price for a specific coin
        """
        logger.debug(f"Fetching price for coin {coin_id} in {vs_currencies}...")
        params = {
            "ids": coin_id,
            "vs_currencies": ",".join(vs_currencies)
        }
        data = await self._request("simple/price", params=params)
        return data if data else {}
    
    async def get_coin_history(self, coin_id: str, date: str) -> Dict[str, Any]:
        """
        Get historical data for a specific coin on a specific date
        """
        logger.debug(f"Fetching historical data for coin {coin_id} on date {date}...")
        data = await self._request(f"coins/{coin_id}/history", params={"date": date})
        return data if data else {}

    async def get_coin_market_chart_range(
            self,
            coin_id: str,
            vs_currency: str,
            from_timestamp: int,
            to_timestamp: int
    ) -> Dict[str, Any]:
        """
        Get historical market data within a range of timestamp (granularity auto)
        """
        logger.debug(f"Fetching market chart range for {coin_id}...")
        params = {
            "vs_currency": vs_currency,
            "from": from_timestamp,
            "to": to_timestamp
        }
        data = await self._request(f"coins/{coin_id}/market_chart/range", params=params)
        return data if data else {}

    async def get_coin_ohlc(self, coin_id: str, vs_currency: str, days: str) -> List[Any]:
        """
        Get coin's OHLC
        days: 1/7/14/30/90/180/365/max
        """
        logger.debug(f"Fetching OHLC for {coin_id} (days={days})...")
        params = {
            "vs_currency": vs_currency,
            "days": days
        }
        data = await self._request(f"coins/{coin_id}/ohlc", params=params)
        return data if data else []