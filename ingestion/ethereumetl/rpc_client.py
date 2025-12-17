import aiohttp
import asyncio
import ssl
import time
import random
from typing import List, Dict, Any, Union, Optional
from utils.logger_utils import get_logger

logger = get_logger("Rpc Client")

class RpcClient(object):
    """
    High-performance JSON-RPC Client supporting Batch Requests and Failover.
    Uses a persistent ClientSession for Connection Pooling.
    Now includes Adaptive Rate Limiting and Exponential Backoff for 429s.
    """
    def __init__(self, rpc_url: Union[str, List[str]], max_retries: int = 5, timeout: int = 60, rpc_min_interval: float = 0.15):
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
        
        # Global Rate Limiter
        self._last_request_time = 0
        self._min_interval = rpc_min_interval 
        self._base_min_interval = rpc_min_interval

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy loads or returns the existing session."""
        if self._session is None or self._session.closed:
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
        
    async def _enforce_rate_limit(self):
        """Ensures a minimum interval between requests to avoid bursting."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    async def _handle_429_backoff(self, url: str, attempt: int, method_name: str = "request"):
        """
        Adaptive handling for 429 Too Many Requests.
        1. Increases the rate limit interval (slows down worker permanently).
        2. Sleeps with exponential backoff + jitter.
        """
        # Adaptive Rate Limiting: 
        # If we hit a 429, we are going too fast globally. 
        # Increase the delay between requests by 50%, up to a max of 2.0s per request.
        previous_interval = self._min_interval
        self._min_interval = min(self._min_interval * 1.5, 2.0)
        
        if self._min_interval > previous_interval:
            logger.warning(f"RPC 429 Rate Limit at {url}. Increasing per-request delay from {previous_interval:.2f}s to {self._min_interval:.2f}s")
        
        # Exponential Backoff with Jitter
        # 1s, 2s, 4s, 8s, 16s... + random(0, 1s)
        backoff_time = (2 ** (attempt - 1)) + random.uniform(0, 1)
        logger.warning(f"RPC 429 at {url} for {method_name}. Backing off for {backoff_time:.2f}s...")
        await asyncio.sleep(backoff_time)

    async def get_latest_block_number(self) -> int:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": self._generate_id()
        }

        session = await self._get_session()

        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    await self._enforce_rate_limit()
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "result" in data:
                                return int(data["result"], 16)
                            else:
                                logger.error(f"RPC Error getting block number from {url}: {data}")
                        elif response.status == 429:
                            await self._handle_429_backoff(url, attempt, "get_latest_block_number")
                        else:
                            logger.error(f"RPC HTTP Error {response.status} (get_block_number) at {url}.")
                except Exception as e:
                    logger.error(f"Error fetching latest block number from {url}: {e}")
            
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"All providers failed to get latest block number (Attempt {attempt}). Retrying in {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)
        
        raise Exception("Failed to get latest block number from all providers after retries")

    async def fetch_blocks_and_receipts(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Fetches Blocks and Receipts.
        """
        payloads = []
        for blk in range(start_block, end_block + 1):
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(blk), True],
                "id": self._generate_id()
            })
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

        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    await self._enforce_rate_limit()
                    async with session.post(url, json=payloads) as response:
                        if response.status == 200:
                            # Success: Maybe slowly recover speed if we were penalized? 
                            # (Optional: decay _min_interval back to _base_min_interval)
                            return await response.json()
                        elif response.status == 429:
                            await self._handle_429_backoff(url, attempt, f"batch {start_block}-{end_block}")
                        else:
                            logger.error(f"RPC HTTP Error {response.status} at {url}. Trying next provider...")

                except aiohttp.ClientError as e:
                    logger.error(f"Network Error at {url}: {str(e)}")
                except asyncio.TimeoutError:
                    logger.error(f"Timeout at {url}.")
                except Exception as e:
                    logger.error(f"Unexpected Error at {url}: {str(e)}")
            
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"All providers failed for batch {start_block}-{end_block}. Retrying in {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)
        
        logger.critical(f"FAILED to fetch batch {start_block}-{end_block} from all providers after {self.max_retries} attempts.")
        return []

    async def get_code(self, address: str, block: str = "latest") -> Optional[str]:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getCode",
            "params": [address, block],
            "id": self._generate_id()
        }
        return await self._make_request("eth_getCode", payload)

    async def get_storage_at(self, address: str, position: str, block: str = "latest") -> Optional[str]:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getStorageAt",
            "params": [address, position, block],
            "id": self._generate_id()
        }
        return await self._make_request("eth_getStorageAt", payload)

    async def batch_call(self, calls: List[Dict[str, Any]], block: str = "latest") -> List[Any]:
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
        session = await self._get_session()
        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    await self._enforce_rate_limit()
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "result" in data:
                                return data["result"]
                            else:
                                logger.debug(f"RPC Error {method_name} at {url}: {data}")
                        elif response.status == 429:
                            await self._handle_429_backoff(url, attempt, method_name)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Network error in {method_name} at {url}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {method_name} at {url}: {e}")
            
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            await asyncio.sleep(wait_time)
        return None

    async def _make_batch_request(self, method_name: str, payloads: List[Dict]) -> List[Any]:
        session = await self._get_session()
        for attempt in range(1, self.max_retries + 1):
            for url in self.rpc_urls:
                try:
                    await self._enforce_rate_limit()
                    async with session.post(url, json=payloads) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                             await self._handle_429_backoff(url, attempt, method_name)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Network error in {method_name} at {url}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {method_name} at {url}: {e}")
            
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            await asyncio.sleep(wait_time)
        return []