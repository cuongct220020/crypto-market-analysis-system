import aiohttp
import asyncio
import ssl
from typing import List, Dict, Any
from utils.logger_utils import get_logger

logger = get_logger("RpcClient")

class RpcClient:
    """
    High-performance JSON-RPC Client supporting Batch Requests.
    Bypasses high-level wrappers to maximize throughput and control.
    """
    def __init__(self, rpc_url: str, max_retries: int = 3, timeout: int = 60):
        self.rpc_url = rpc_url
        self.id_counter = 0
        self.max_retries = max_retries
        # Total timeout for the request (connect + read)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        # SSL Context that ignores verification (Fallback)
        self.ssl_context_unsafe = ssl.create_default_context()
        self.ssl_context_unsafe.check_hostname = False
        self.ssl_context_unsafe.verify_mode = ssl.CERT_NONE

    def _generate_id(self) -> int:
        self.id_counter += 1
        return self.id_counter

    async def fetch_blocks_and_receipts(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Fetches Blocks (with full transactions) and Receipts for a given range
        using a SINGLE HTTP Batch Request.
        
        This minimizes network RTT (Round Trip Time).
        
        Payload Structure:
        [
            {method: eth_getBlockByNumber, params: [hex(start), True]}, ...
            {method: eth_getBlockReceipts, params: [hex(start)]}, ...
        ]
        
        Returns:
            A list of RPC Response objects corresponding to the request order.
            Returns empty list on total failure after retries (caller should handle).
        """
        payloads = []
        
        # 1. Generate payloads for eth_getBlockByNumber
        for blk in range(start_block, end_block + 1):
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(blk), True],  # True = full transactions
                "id": self._generate_id()
            })
        
        # 2. Generate payloads for eth_getBlockReceipts
        # Note: 'eth_getBlockReceipts' is an optimized method supported by Alchemy, QuickNode, Erigon.
        # It fetches ALL receipts for a block in one go.
        for blk in range(start_block, end_block + 1):
            payloads.append({
                "jsonrpc": "2.0",
                "method": "eth_getBlockReceipts",
                "params": [hex(blk)], 
                "id": self._generate_id()
            })
            
        if not payloads:
            return []

        # Retry Loop
        for attempt in range(1, self.max_retries + 1):
            try:
                # Always use unsafe SSL context to avoid certificate errors on local macOS environments
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.post(self.rpc_url, json=payloads, ssl=self.ssl_context_unsafe) as response:
                        if response.status == 200:
                            # Parse JSON
                            return await response.json()
                        elif response.status == 429: # Too Many Requests
                            wait_time = 2 ** attempt # Exponential Backoff
                            logger.warning(f"RPC 429 Rate Limit. Retrying batch {start_block}-{end_block} in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"RPC HTTP Error {response.status}. Attempt {attempt}/{self.max_retries}")
                            await asyncio.sleep(1)

            except aiohttp.ClientError as e:
                logger.error(f"Network Error fetching batch {start_block}-{end_block}: {str(e)}. Attempt {attempt}/{self.max_retries}")
                await asyncio.sleep(1)
            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching batch {start_block}-{end_block}. Attempt {attempt}/{self.max_retries}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected Error fetching batch {start_block}-{end_block}: {str(e)}")
                await asyncio.sleep(1)
        
        logger.critical(f"FAILED to fetch batch {start_block}-{end_block} after {self.max_retries} attempts.")
        return []

