import aiohttp
import asyncio
import ssl
from typing import List, Dict, Any, Union, Optional
from utils.logger_utils import get_logger

logger = get_logger("Rpc Client")

class RpcClient(object):
    """
    High-performance JSON-RPC Client supporting Batch Requests and Failover.
    Uses a persistent ClientSession for Connection Pooling.
    """
    def __init__(self, rpc_url: Union[str, List[str]], max_retries: int = 3, timeout: int = 60):
        if isinstance(rpc_url, str):
            self.rpc_urls = [rpc_url]
        else:
            self.rpc_urls = rpc_url
            
        if not self.rpc_urls:
            raise ValueError("At least one RPC URL must be provided.")

        self.id_counter = 0
        self.max_retries = max_retries
        # Total timeout for the request (connect + read)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        # SSL Context that ignores verification (Fallback)
        self.ssl_context_unsafe = ssl.create_default_context()
        self.ssl_context_unsafe.check_hostname = False
        self.ssl_context_unsafe.verify_mode = ssl.CERT_NONE
        
        # Persistent Session
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy loads or returns the existing session."""
        if self._session is None or self._session.closed:
            # Create a new session with TCPConnector for pooling
            # limit=0 means unlimited connections, or set specific limit if needed
            connector = aiohttp.TCPConnector(limit=100, ssl=self.ssl_context_unsafe, force_close=False)
            self._session = aiohttp.ClientSession(
                connector=connector, 
                timeout=self.timeout
            )
        return self._session

    async def close(self):
        """Closes the underlying session."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _generate_id(self) -> int:
        self.id_counter += 1
        return self.id_counter

    async def get_latest_block_number(self) -> int:
        """
        Fetches the latest block number from the blockchain.
        Tries all configured RPC URLs in a round-robin/failover fashion per attempt.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": self._generate_id()
        }

        session = await self._get_session()

        for attempt in range(1, self.max_retries + 1):
            # Try each provider in the list
            for url in self.rpc_urls:
                try:
                    # Note: We don't use 'async with session' here because we don't want to close it
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "result" in data:
                                return int(data["result"], 16)
                            else:
                                logger.error(f"RPC Error getting block number from {url}: {data}")
                        elif response.status == 429:
                            logger.warning(f"RPC 429 Rate Limit (get_block_number) at {url}. Trying next provider...")
                        else:
                            logger.error(f"RPC HTTP Error {response.status} (get_block_number) at {url}.")
                except Exception as e:
                    logger.error(f"Error fetching latest block number from {url}: {e}")
            
            # If all providers failed in this attempt, wait before retrying the loop
            wait_time = 2 ** attempt
            logger.warning(f"All providers failed to get latest block number (Attempt {attempt}/{self.max_retries}). Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
        
        raise Exception("Failed to get latest block number from all providers after retries")

    async def fetch_blocks_and_receipts(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Fetches Blocks (with full transactions) and Receipts using Batch Request.
        Supports Failover across multiple RPC URLs.
        """
        payloads = []
        
        # 1. Generate payloads for eth_getBlockByNumber
        for blk in range(start_block, end_block + 1):
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(blk), True],
                "id": self._generate_id()
            })
        
        # 2. Generate payloads for eth_getBlockReceipts
        for blk in range(start_block, end_block + 1):
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getBlockReceipts",
                "params": [hex(blk)], 
                "id": self._generate_id()
            })
            
        if not payloads:
            return []

        session = await self._get_session()

        # Retry Loop
        for attempt in range(1, self.max_retries + 1):
            # Failover Loop
            for url in self.rpc_urls:
                try:
                    async with session.post(url, json=payloads) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            logger.warning(f"RPC 429 Rate Limit at {url} for batch {start_block}-{end_block}. Trying next provider...")
                        else:
                            logger.error(f"RPC HTTP Error {response.status} at {url}. Trying next provider...")

                except aiohttp.ClientError as e:
                    logger.error(f"Network Error at {url}: {str(e)}")
                except asyncio.TimeoutError:
                    logger.error(f"Timeout at {url}.")
                except Exception as e:
                    logger.error(f"Unexpected Error at {url}: {str(e)}")
            
            # All providers failed
            wait_time = 2 ** attempt
            logger.warning(f"All providers failed for batch {start_block}-{end_block}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
        
        logger.critical(f"FAILED to fetch batch {start_block}-{end_block} from all providers after {self.max_retries} attempts.")
        return []

    async def get_code(self, address: str, block: str = "latest") -> Optional[str]:
        """
        Fetches code at a given address.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getCode",
            "params": [address, block],
            "id": self._generate_id()
        }
        return await self._make_request("eth_getCode", payload)

    async def get_storage_at(self, address: str, position: str, block: str = "latest") -> Optional[str]:
        """
        Fetches storage at a given position.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getStorageAt",
            "params": [address, position, block],
            "id": self._generate_id()
        }
        return await self._make_request("eth_getStorageAt", payload)

    async def batch_call(self, calls: List[Dict[str, Any]], block: str = "latest") -> List[Any]:
        """
        Executes a batch of eth_call requests.
        calls: List of dicts [{"to": "0x...", "data": "0x..."}, ...]
        """
        if not calls:
            return []

        payloads = []
        for call in calls:
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [call, block],
                "id": self._generate_id()
            })

        return await self._make_batch_request("batch_eth_call", payloads)

    async def _make_request(self, method_name: str, payload: Dict[str, Any]) -> Any:
        """Helper for single RPC request with failover."""
        session = await self._get_session()
        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "result" in data:
                                return data["result"]
                            else:
                                logger.debug(f"RPC Error {method_name} at {url}: {data}")
                        elif response.status == 429:
                            logger.warning(f"RPC 429 {method_name} at {url}.")
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Network error in {method_name} at {url}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {method_name} at {url}: {e}")
            await asyncio.sleep(2 ** attempt)
        return None

    async def _make_batch_request(self, method_name: str, payloads: List[Dict]) -> List[Any]:
        """Helper for batch RPC request with failover."""
        session = await self._get_session()
        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    async with session.post(url, json=payloads) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                             logger.warning(f"RPC 429 {method_name} at {url}.")
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Network error in {method_name} at {url}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {method_name} at {url}: {e}")
            await asyncio.sleep(2 ** attempt)
        return []

