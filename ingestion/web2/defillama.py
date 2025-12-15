from typing import Any, Dict, List, Optional
import aiohttp
import asyncio

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("DefiLlama API")

class DefiLlamaAPI:
    """
    A class to interact with DefiLlama API for collecting DeFi protocol data.
    """
    
    def __init__(self):
        self.base_url = "https://api.llama.fi"
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
    
    async def get_protocols(self) -> List[Dict[str, Any]]:
        """
        Get the list of all DeFi protocols
        """
        logger.info("Fetching all DeFi protocols from DefiLlama...")
        try:
            url = f"{self.base_url}/protocols"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        logger.info(f"Successfully retrieved {len(data)} protocols from DefiLlama")
                        return data
                    else:
                        logger.warning("Response from DefiLlama does not contain expected list structure")
                        return []
                else:
                    logger.error(f"Failed to fetch protocols. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching protocols from DefiLlama: {str(e)}")
            raise
    
    async def get_protocol_data(self, protocol_slug: str) -> Dict[str, Any]:
        """
        Get detailed data for a specific protocol
        """
        logger.debug(f"Fetching data for protocol: {protocol_slug}")
        try:
            url = f"{self.base_url}/protocol/{protocol_slug}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved data for protocol: {protocol_slug}")
                    return data
                else:
                    logger.error(f"Failed to fetch data for protocol {protocol_slug}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching data for protocol {protocol_slug} from DefiLlama: {str(e)}")
            raise
    
    async def get_tvl_history(self, protocol_slug: str) -> List[Dict[str, Any]]:
        """
        Get TVL history for a specific protocol
        """
        logger.debug(f"Fetching TVL history for protocol: {protocol_slug}")
        try:
            url = f"{self.base_url}/protocol/{protocol_slug}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    tvl_history = data.get("chainTvls", {})
                    logger.debug(f"Successfully retrieved TVL history for protocol: {protocol_slug}")
                    return tvl_history
                else:
                    logger.error(f"Failed to fetch TVL history for {protocol_slug}. Status: {response.status}, Reason: {response.reason}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching TVL history for {protocol_slug} from DefiLlama: {str(e)}")
            raise
    
    async def get_current_tvl(self) -> List[Dict[str, Any]]:
        """
        Get current TVL for all protocols
        """
        logger.info("Fetching current TVL for all protocols from DefiLlama...")
        try:
            url = f"{self.base_url}/tvl"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved current TVL data for {len(data)} protocols")
                    return data
                else:
                    logger.error(f"Failed to fetch current TVL. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching current TVL from DefiLlama: {str(e)}")
            raise
    
    async def get_historical_tvl_chain(self, chain: str) -> List[Dict[str, Any]]:
        """
        Get historical TVL for a specific chain
        """
        logger.debug(f"Fetching historical TVL for chain: {chain}")
        try:
            url = f"{self.base_url}/v2/historicalChainTvl/{chain}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully retrieved historical TVL for chain: {chain}")
                    return data
                else:
                    logger.error(f"Failed to fetch historical TVL for {chain}. Status: {response.status}, Reason: {response.reason}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching historical TVL for {chain} from DefiLlama: {str(e)}")
            raise