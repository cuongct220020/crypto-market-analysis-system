import asyncio
from typing import List, Optional, Any, Union
from eth_utils import function_signature_to_4byte_selector
from eth_abi import decode

from ingestion.ethereumetl.models.contract import (
    EthBaseContract, EthImplementationContract, EthProxyContract, 
    ImplContractCategory, ProxyType
)
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
        
        self._selectors = {
            k: "0x" + function_signature_to_4byte_selector(v).hex() 
            for k, v in ERC20_METHODS.items()
        }

    async def get_contracts(self, addresses: List[str], block_number: int) -> List[EthBaseContract]:
        if not addresses:
            return []
        tasks = [self.get_contract(addr, block_number) for addr in addresses]
        return await asyncio.gather(*tasks)

    async def get_contract(self, address: str, block_number: int) -> EthBaseContract:
        # We start by assuming it's an Implementation (standard contract)
        # If we detect proxy, we will switch to EthProxyContract
        # But for now, let's keep logic linear.
        
        # 1. Get Bytecode
        try:
            bytecode = await self._rpc_client.get_code(address)
            if not bytecode or bytecode == "0x":
                # EOA or destroyed -> Return base with minimal info
                return EthImplementationContract(address=address, block_number=block_number) 
            
            # --- STEP 2: CHECK MINIMAL PROXY (EIP-1167) FIRST ---
            minimal_impl_addr = self._analyzer_service.is_minimal_proxy(bytecode)
            
            if minimal_impl_addr:
                # It IS a Proxy
                proxy_contract = EthProxyContract(
                    address=address, 
                    block_number=block_number,
                    bytecode=bytecode,
                    is_proxy=True,
                    proxy_type=ProxyType.MINIMAL,
                    implementation_address=minimal_impl_addr
                )
                
                # Fetch Logic from Implementation
                impl_logic = await self._analyze_implementation(minimal_impl_addr, block_number)
                proxy_contract.implementation = impl_logic
                
                # Enrich Metadata (Call Proxy address, but logic is from Impl)
                if impl_logic.is_erc20 or impl_logic.is_erc721:
                    await self._enrich_metadata(proxy_contract, address) # Pass proxy address for calls
                
                return proxy_contract

            # Not Minimal Proxy -> Analyze as Implementation candidate first
            contract = EthImplementationContract(
                address=address, 
                block_number=block_number,
                bytecode=bytecode
            )
            
            # --- STEP 3: ANALYZE BYTECODE ---
            sighashes = self._analyzer_service.get_function_sighashes(bytecode)
            contract.function_sighashes = sighashes
            contract.is_erc20 = self._analyzer_service.is_erc20_contract(sighashes)
            contract.is_erc721 = self._analyzer_service.is_erc721_contract(sighashes)

            # Categorize
            if contract.is_erc20:
                contract.impl_category = ImplContractCategory.TOKEN
            elif contract.is_erc721:
                contract.impl_category = ImplContractCategory.NFT
            else:
                contract.impl_category = ImplContractCategory.UNKNOWN

            # --- STEP 4: DETECT OTHER PROXIES ---
            # If standard analysis failed or returned generic, check if it's a hidden proxy
            proxy_type, impl_addr = await self._detect_proxy_slots(address, sighashes)
            
            if proxy_type != ProxyType.UNKNOWN and impl_addr:
                # Convert to Proxy Object
                proxy_contract = EthProxyContract(
                    address=address,
                    block_number=block_number,
                    bytecode=bytecode,
                    is_proxy=True,
                    proxy_type=proxy_type,
                    implementation_address=impl_addr
                )
                
                # Fetch Logic from Implementation
                impl_logic = await self._analyze_implementation(impl_addr, block_number)
                proxy_contract.implementation = impl_logic
                
                # Enrich Metadata (Call Proxy address)
                if impl_logic.is_erc20 or impl_logic.is_erc721:
                    await self._enrich_metadata(proxy_contract, address)
                    
                return proxy_contract

            # --- STEP 5: ENRICH METADATA (Standard Contract) ---
            if contract.is_erc20 or contract.is_erc721: 
                await self._enrich_metadata(contract, address)
            
            return contract
                
        except Exception as e:
            logger.error(f"Error processing contract {address}: {e}")
            return EthImplementationContract(address=address)

    async def _analyze_implementation(self, address: str, block_number: int) -> EthImplementationContract:
        """
        Helper to fetch and analyze implementation logic.
        """
        impl = EthImplementationContract(address=address, block_number=block_number)
        try:
            bytecode = await self._rpc_client.get_code(address)
            if not bytecode or bytecode == "0x":
                return impl
            
            impl.bytecode = bytecode
            sighashes = self._analyzer_service.get_function_sighashes(bytecode)
            impl.function_sighashes = sighashes
            impl.is_erc20 = self._analyzer_service.is_erc20_contract(sighashes)
            impl.is_erc721 = self._analyzer_service.is_erc721_contract(sighashes)
            
            if impl.is_erc20:
                impl.impl_category = ImplContractCategory.TOKEN
            elif impl.is_erc721:
                impl.impl_category = ImplContractCategory.NFT
                
        except Exception as e:
            logger.warning(f"Failed to analyze implementation {address}: {e}")
        return impl

    async def _detect_proxy_slots(self, address: str, sighashes: List[str]) -> tuple[ProxyType, Optional[str]]:
        """
        Detects EIP-1967, Diamond, etc.
        Returns (ProxyType, ImplementationAddress)
        """
        # 1. EIP-1967 Beacon
        beacon_data = await self._rpc_client.get_storage_at(address, SLOT_EIP1967_BEACON)
        beacon_addr = self._bytes32_to_address(hex_data=beacon_data)
        if beacon_addr:
            return ProxyType.BEACON, beacon_addr

        # 2. EIP-1967 Implementation
        impl_data = await self._rpc_client.get_storage_at(address, SLOT_EIP1967_IMPL)
        impl_addr = self._bytes32_to_address(impl_data)
        if impl_addr:
            p_type = ProxyType.TRANSPARENT
            if self._analyzer_service.is_gnosis_safe(sighashes):
                p_type = ProxyType.GNOSIS_SAFE
            elif self._analyzer_service.is_uups_proxy(sighashes):
                p_type = ProxyType.UUPS
            return p_type, impl_addr

        # 3. Diamond
        if self._analyzer_service.is_diamond_proxy(sighashes):
             return ProxyType.DIAMOND, None # Diamond has multiple facets, complex handling

        return ProxyType.UNKNOWN, None

    @staticmethod
    def _bytes32_to_address(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x": return None
        if len(hex_data) < 42: return None
        addr_hex = "0x" + hex_data[-40:]
        if addr_hex == "0x0000000000000000000000000000000000000000": return None
        return to_normalized_address(addr_hex)

    async def _enrich_metadata(self, contract: Union[EthImplementationContract, EthProxyContract], call_address: str):
        # We populate metadata into the contract object passed (could be Impl or Proxy wrapper)
        # But for ProxyWrapper, we want to set metadata on the IMPLEMENTATION object if it exists?
        # Actually, the Mapper handles flattening. So we can set it on the object directly or the impl.
        # Let's set it on the object passed for simplicity, but wait... 
        # For Proxy, 'name' comes from calling Proxy address, but reflects Implementation logic.
        # Our model: Proxy has 'implementation' object.
        # Let's attach metadata to the implementation object inside proxy if possible, or main object.
        
        target = contract
        if isinstance(contract, EthProxyContract) and contract.implementation:
            target = contract.implementation # Store metadata in the logic holder
        
        calls = []
        keys = ["name", "symbol", "decimals", "totalSupply"]
        for key in keys:
            calls.append({"to": call_address, "data": self._selectors[key]})

        try:
            results = await self._rpc_client.batch_call(calls)
            if not results or len(results) != len(keys): return

            target.name = self._decode_string(self._get_result_value(results[0]))
            target.symbol = self._decode_string(self._get_result_value(results[1]))
            
            dec_val = self._get_result_value(results[2])
            if dec_val and dec_val != "0x":
                try: target.decimals = hex_to_dec(dec_val)
                except: pass

            ts_val = self._get_result_value(results[3])
            if ts_val and ts_val != "0x":
                try: target.total_supply = str(hex_to_dec(ts_val))
                except: pass
                
            if target.is_erc721: target.decimals = 0
                
        except Exception as e:
            logger.debug(f"Metadata fetch failed for {call_address}: {e}")

    def _get_result_value(self, rpc_result: Any) -> Optional[str]:
        if isinstance(rpc_result, dict) and "result" in rpc_result:
            return rpc_result["result"]
        return None

    @staticmethod
    def _decode_string(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x": return None
        try:
            data_bytes = bytes.fromhex(hex_data[2:])
            if not data_bytes: return None
            try: return decode(['string'], data_bytes)[0]
            except: pass
            try: return decode(['bytes32'], data_bytes)[0].decode('utf-8').strip('\x00')
            except: pass
            return data_bytes.decode('utf-8', errors='ignore').strip('\x00')
        except Exception: return None
