import asyncio
from typing import List, Optional, Any
from eth_utils import function_signature_to_4byte_selector
from eth_abi import decode

from ingestion.ethereumetl.models.contract import EthContract, ContractCategory, ProxyType
from ingestion.ethereumetl.service.eth_contract_analyzer_service import EthContractAnalyzerService
from ingestion.ethereumetl.rpc_client import RpcClient
from utils.logger_utils import get_logger
from utils.formatter_utils import hex_to_dec, to_normalized_address
from constants.constants import SLOT_EIP1967_IMPL, SLOT_EIP1967_BEACON

logger = get_logger("ETH Contract Service")

# Selectors
ERC20_METHODS = {
    "name": "name()",
    "symbol": "symbol()",
    "decimals": "decimals()",
    "totalSupply": "totalSupply()"
}

class EthContractService:
    def __init__(self, rpc_client: RpcClient):
        self._rpc_client = rpc_client
        self._analyzer_service = EthContractAnalyzerService()
        
        # Pre-compute selectors
        self._selectors = {
            k: "0x" + function_signature_to_4byte_selector(v).hex() 
            for k, v in ERC20_METHODS.items()
        }

    async def get_contracts(self, addresses: List[str], block_number: int) -> List[EthContract]:
        """
        Fetches multiple contracts in parallel.
        """
        if not addresses:
            return []
            
        tasks = [self.get_contract(addr, block_number) for addr in addresses]
        return await asyncio.gather(*tasks)

    async def get_contract(self, address: str, block_number: int) -> EthContract:
        contract = EthContract()
        contract.address = address
        contract.updated_block_number = block_number
        
        # Note: chain_id is typically static per environment, but if we need it in the record:
        # contract.chain_id = "1" # Should ideally come from config or RPC 

        # 1. Get Bytecode
        try:
            bytecode = await self._rpc_client.get_code(address)
            if not bytecode or bytecode == "0x":
                return contract # EOA or destroyed
            
            contract.bytecode = bytecode

            # 2. Analyze Bytecode
            sighashes = self._analyzer_service.get_function_sighashes(bytecode)
            contract.function_sighashes = sighashes
            contract.is_erc20 = self._analyzer_service.is_erc20_contract(sighashes)
            contract.is_erc721 = self._analyzer_service.is_erc721_contract(sighashes)

            # Categorize
            if contract.is_erc20:
                contract.category = ContractCategory.TOKEN
            elif contract.is_erc721:
                contract.category = ContractCategory.NFT
            else:
                contract.category = ContractCategory.UNKNOWN

            # 3. Detect Proxy
            await self._detect_proxy(contract)

            # 4. Fetch Metadata (only for Token/NFT/Unknown that might be tokens)
            if contract.is_erc20 or contract.is_erc721 or True: # Try fetching basic info for all contracts
                await self._enrich_metadata(contract)
                
        except Exception as e:
            logger.error(f"Error processing contract {address}: {e}")

        return contract

    async def _detect_proxy(self, contract: EthContract):
        # 1. Minimal Proxy (Bytecode check)
        impl_addr = self._analyzer_service.is_minimal_proxy(contract.bytecode)
        if impl_addr:
            contract.is_proxy = True
            contract.proxy_type = ProxyType.EIP1167
            contract.implementation_address = impl_addr
            return

        # 2. EIP-1967 Beacon Slot
        beacon_data = await self._rpc_client.get_storage_at(contract.address, SLOT_EIP1967_BEACON)
        beacon_addr = self._bytes32_to_address(hex_data=beacon_data)
        if beacon_addr:
            contract.is_proxy = True
            contract.proxy_type = ProxyType.BEACON
            contract.implementation_address = beacon_addr
            return

        # 3. EIP-1967 Implementation Slot
        impl_data = await self._rpc_client.get_storage_at(contract.address, SLOT_EIP1967_IMPL)
        impl_addr = self._bytes32_to_address(impl_data)
        if impl_addr:
            contract.is_proxy = True
            contract.implementation_address = impl_addr
            
            if self._analyzer_service.is_gnosis_safe(contract.function_sighashes):
                contract.proxy_type = ProxyType.GNOSIS_SAFE
            elif self._analyzer_service.is_uups_proxy(contract.function_sighashes):
                contract.proxy_type = ProxyType.UUPS
            else:
                contract.proxy_type = ProxyType.TRANSPARENT
            return

        # 4. Diamond Proxy
        if self._analyzer_service.is_diamond_proxy(contract.function_sighashes):
            contract.is_proxy = True
            contract.proxy_type = ProxyType.DIAMOND
            return

    @staticmethod
    def _bytes32_to_address(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x": return None
        if len(hex_data) < 42: return None
        
        addr_hex = "0x" + hex_data[-40:]
        if addr_hex == "0x0000000000000000000000000000000000000000":
            return None
        
        return to_normalized_address(addr_hex)

    async def _enrich_metadata(self, contract: EthContract):
        # Prepare batch calls
        calls = []
        # keys order: name, symbol, decimals, totalSupply
        keys = ["name", "symbol", "decimals", "totalSupply"]
        
        for key in keys:
            calls.append({
                "to": contract.address,
                "data": self._selectors[key]
            })

        # Execute Batch Call
        try:
            results = await self._rpc_client.batch_call(calls)
            
            if not results or len(results) != len(keys):
                return

            # Decode Results
            contract.name = self._decode_string(self._get_result_value(results[0]))
            contract.symbol = self._decode_string(self._get_result_value(results[1]))
            
            # Decimals (uint8)
            dec_val = self._get_result_value(results[2])
            if dec_val and dec_val != "0x":
                try:
                    contract.decimals = hex_to_dec(dec_val)
                except: pass

            # Total Supply (uint256)
            ts_val = self._get_result_value(results[3])
            if ts_val and ts_val != "0x":
                try:
                    contract.total_supply = str(hex_to_dec(ts_val))
                except: pass
                
            # ERC721 usually has 0 decimals
            if contract.is_erc721:
                contract.decimals = 0
                
        except Exception as e:
            logger.debug(f"Metadata fetch failed for {contract.address}: {e}")


    def _get_result_value(self, rpc_result: Any) -> Optional[str]:
        if isinstance(rpc_result, dict):
            if "result" in rpc_result:
                return rpc_result["result"]
            # Handle error response?
        return None

    @staticmethod
    def _decode_string(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x":
            return None
        try:
            # Remove 0x
            data_bytes = bytes.fromhex(hex_data[2:])
            if not data_bytes: return None

            # Try to decode as string (ABI encoded string is dynamic)
            # Standard ABI string: offset (32) + length (32) + data
            try:
                return decode(['string'], data_bytes)[0]
            except:
                pass
                
            # Fallback: bytes32
            try:
                return decode(['bytes32'], data_bytes)[0].decode('utf-8').strip('\x00')
            except:
                pass
            
            # Fallback: raw utf-8 if implementation is non-standard
            return data_bytes.decode('utf-8', errors='ignore').strip('\x00')

        except Exception:
            return None
