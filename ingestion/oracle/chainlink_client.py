from typing import Dict, List, Any, Optional
import asyncio
from web3 import Web3
from eth_abi import decode

from ingestion.rpc_client import RpcClient
from abi.chainlink_abi import CHAINLINK_AGGREGATOR_ABI
from constants.price_feed_contract_address import PRICE_FEED_CONTRACT_ADDRESS
from utils.logger_utils import get_logger

logger = get_logger("Chainlink Client")

class ChainlinkClient:
    """
    Client to interact with Chainlink Price Feeds using direct eth_call batches via RpcClient.
    """
    
    def __init__(self, rpc_client: RpcClient):
        self.rpc_client = rpc_client
        self.w3 = Web3() # Helper for encoding/decoding
        
        # Pre-compute selector for latestRoundData() -> 0xfeaf968c
        self.latest_round_data_selector = self.w3.keccak(text="latestRoundData()")[:4].hex()
        
        # Pre-compute selector for decimals() -> 0x313ce567
        self.decimals_selector = self.w3.keccak(text="decimals()")[:4].hex()

        self._decimals_cache: Dict[str, int] = {} # Cache for decimals

    async def get_decimals(self, feed_addresses: List[str]) -> Dict[str, int]:
        """
        Fetches decimals for a list of feed addresses. Uses caching.
        """
        # Initialize result dict with default values for all addresses to prevent KeyError
        result = {}
        for addr in feed_addresses:
            result[addr] = 18  # default value
            # If already in cache, use cached value
            if addr in self._decimals_cache:
                result[addr] = self._decimals_cache[addr]

        # Find addresses that need to be fetched
        missing_decimals = [addr for addr in feed_addresses if addr not in self._decimals_cache]

        if not missing_decimals:
            return result

        batch_calls = [
            {"to": addr, "data": self.decimals_selector} for addr in missing_decimals
        ]

        logger.info(f"Fetching decimals for {len(missing_decimals)} feeds...")
        results = await self.rpc_client.batch_call(batch_calls)

        for i, res in enumerate(results):
            addr = missing_decimals[i]
            
            # Extract result from JSON-RPC response dict
            rpc_result = res
            if isinstance(res, dict):
                if "error" in res:
                    logger.error(f"RPC Error for {addr}: {res['error']}")
                    rpc_result = None
                else:
                    rpc_result = res.get("result")

            if rpc_result and rpc_result != "0x":
                try:
                    # decode returns a tuple
                    decoded = decode(["uint8"], bytes.fromhex(rpc_result[2:]))[0]
                    self._decimals_cache[addr] = decoded
                    result[addr] = decoded  # Update result with actual value
                except Exception as e:
                    logger.error(f"Error decoding decimals for {addr}: {e}")
                    self._decimals_cache[addr] = 18 # Default fallback
            else:
                logger.warning(f"Could not get decimals for {addr}, defaulting to 18.")
                self._decimals_cache[addr] = 18

        return result

    async def get_latest_prices(self, pair_names: List[str]) -> Dict[str, Any]:
        """
        Fetches the latest prices for the given list of pair names (e.g., ['eth-usd', 'btc-usd']).
        Returns a dict mapped by pair name.
        """
        # 1. Resolve addresses
        resolved_pairs = []
        for name in pair_names:
            addr = PRICE_FEED_CONTRACT_ADDRESS.get(name.lower())
            if addr:
                resolved_pairs.append((name, addr))
            else:
                logger.warning(f"No contract address found for pair: {name}")
        
        if not resolved_pairs:
            return {}

        feed_addresses = [addr for _, addr in resolved_pairs]
        
        # 2. Ensure decimals are known (Chainlink feeds have different decimals)
        decimals_map = await self.get_decimals(feed_addresses)
        
        # 3. Batch call for latestRoundData
        batch_calls = [
            {"to": addr, "data": self.latest_round_data_selector} for addr in feed_addresses
        ]
        
        logger.info(f"Fetching prices for {len(feed_addresses)} pairs...")
        results = await self.rpc_client.batch_call(batch_calls)
        
        # 4. Decode results
        final_data = {}
        
        for i, res in enumerate(results):
            name, addr = resolved_pairs[i]
            
            # Extract result from JSON-RPC response dict
            rpc_result = res
            if isinstance(res, dict):
                if "error" in res:
                    logger.error(f"RPC Error for {name} ({addr}): {res['error']}")
                    rpc_result = None
                else:
                    rpc_result = res.get("result")
            
            if not rpc_result or rpc_result == "0x":
                logger.error(f"Empty or invalid response for {name} ({addr})")
                continue
                
            try:
                # latestRoundData returns: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
                # We need 'answer' (index 1) and 'updatedAt' (index 3)
                output_types = ["uint80", "int256", "uint256", "uint256", "uint80"]
                decoded = decode(output_types, bytes.fromhex(rpc_result[2:]))
                
                raw_price = decoded[1]
                updated_at = decoded[3]
                
                decimal = decimals_map.get(addr, 18)
                human_price = raw_price / (10 ** decimal)
                
                final_data[name] = {
                    "pair": name,
                    "price": human_price,
                    "updated_at": updated_at,
                    "contract_address": addr
                }
                
            except Exception as e:
                logger.error(f"Error decoding price for {name}: {e}")
                
        return final_data
