import aiohttp
import asyncio
from typing import Any, Dict, List, Optional
from utils.logger_utils import get_logger

logger = get_logger("DefiLlama Client")

class DefiLlamaClient:
    """
    A class to interact with DefiLlama API for collecting protocol TVL and volume/revenue data.
    """
    
    def __init__(self):
        self.base_url = "https://api.llama.fi"
        self.dexs_url = "https://api.llama.fi/summary/dexs"
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _request(self, url: str, params: Dict[str, Any] = None) -> Any:
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Failed to fetch {url}. Status: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error requesting {url}: {str(e)}")
            return None
