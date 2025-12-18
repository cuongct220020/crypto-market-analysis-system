import asyncio
from typing import List, Optional, Any, Union, Tuple
from eth_abi import decode
from async_lru import alru_cache

from ingestion.ethereumetl.models.contract import (
    EthBaseContract, EthImplementationContract, EthProxyContract, 
    ImplContractCategory, ProxyType
)
from ingestion.ethereumetl.service.eth_contract_analyzer_service import EthContractAnalyzerService
from ingestion.rpc_client import RpcClient
from utils.logger_utils import get_logger
from utils.formatter_utils import hex_to_dec, to_normalized_address
from constants.contract_function_selectors import ERC20_FUNCTION_SELECTORS
from constants.contract_proxy_constants import (
    SLOT_EIP1967_IMPL, 
    SLOT_EIP1967_BEACON
)
from constants.safe_canonical_master_copies import SAFE_CANONICAL_MASTER_COPIES

logger = get_logger("ETH Contract Service")

# Selectors for Metadata Enrichment
ERC20_METADATA_METHODS = {
    "name": "name()",
    "symbol": "symbol()",
    "decimals": "decimals()",
    "totalSupply": "totalSupply()"
}

ZERO_ADDRESS = to_normalized_address("0x0000000000000000000000000000000000")

class EthContractService:
    def __init__(self, rpc_client: RpcClient):
        self._rpc_client = rpc_client
        
        # Create a reverse mapping for efficient lookup: function_signature -> selector
        signature_to_selector = {v: k for k, v in ERC20_FUNCTION_SELECTORS.items()}
        
        # Pre-compute metadata selectors using the reverse mapping
        self._metadata_selectors = {
            k: signature_to_selector[v]
            for k, v in ERC20_METADATA_METHODS.items()
        }

    async def get_contracts(self, addresses: List[str], block_number: int) -> List[EthBaseContract]:
        """
        Fetches multiple contracts in parallel using asyncio.gather with a semaphore to limit concurrency.
        """
        if not addresses:
            return []
            
        unique_addresses = list(set(addresses))
        semaphore = asyncio.Semaphore(2)  # Limit to 2 concurrent contract analysis tasks

        async def semi_get_contract(addr):
            async with semaphore:
                return await self.get_contract(addr, block_number)
        
        tasks = [semi_get_contract(addr) for addr in unique_addresses]
        return await asyncio.gather(*tasks)

    @alru_cache(maxsize=10000, ttl=3600)
    async def get_contract(self, address: str, block_number: int) -> EthBaseContract:
        """
        The "Waterfall" logic for contract detection.
        """
        # Default fallback object (acts as EOA or Empty)
        base_contract = EthImplementationContract(address=address, block_number=block_number)

        try:
            # --- STEP 1: EOA CHECK ---
            bytecode = await self._rpc_client.get_code(address)
            if not bytecode or bytecode == "0x":
                return base_contract
            
            # --- STEP 2: MINIMAL PROXY CHECK (Static) ---
            minimal_impl_addr = EthContractAnalyzerService.is_minimal_proxy(bytecode)
            if minimal_impl_addr:
                return await self._handle_proxy(
                    address, block_number, bytecode, 
                    ProxyType.MINIMAL, minimal_impl_addr
                )

            # Not Minimal Proxy -> Analyze as Implementation candidate first
            # Get sighashes from its own bytecode for initial classification and further proxy checks
            sighashes = EthContractAnalyzerService.get_function_sighashes(bytecode)

            # --- STEP 3: DEEP PROXY CHECK (Storage Slots & Sighashes) ---
            proxy_type, impl_addr = await self._detect_proxy_slots(address, sighashes)
            
            if proxy_type != ProxyType.UNKNOWN and impl_addr:
                # It IS a Proxy (Transparent, UUPS, Beacon, Gnosis Safe)
                return await self._handle_proxy(
                    address, block_number, bytecode,
                    proxy_type, impl_addr
                )
            
            # --- STEP 4: DIAMOND PROXY CHECK ---
            # Diamond doesn't rely on EIP-1967 slots but on specific selectors
            if EthContractAnalyzerService.is_diamond_proxy(sighashes):
                 proxy = EthProxyContract(
                     address=address,
                     block_number=block_number,
                     bytecode=bytecode,
                     proxy_type=ProxyType.DIAMOND,
                     is_proxy=True
                 )
                 # Diamond is complex, we just know it's a Diamond but don't resolve single impl_addr easily
                 # Enrich metadata on Proxy itself
                 await self._enrich_metadata(proxy, address)
                 return proxy


            # --- STEP 5: STANDARD IMPLEMENTATION CONTRACT (Direct) ---
            # If not a known proxy, treat as a direct implementation contract
            contract = EthImplementationContract(
                address=address,
                block_number=block_number,
                bytecode=bytecode,
                bytecode_hash=str(hash(bytecode)) # Simple hash for diff
            )
            contract.function_sighashes = sighashes # Use already fetched sighashes
            
            # Analyze Standards
            contract.is_erc20 = EthContractAnalyzerService.is_erc20_contract(sighashes)
            contract.is_erc721 = EthContractAnalyzerService.is_erc721_contract(sighashes)
            contract.is_erc1155 = EthContractAnalyzerService.is_erc1155_contract(sighashes) # Added Missing Check

            # Categorize & Assign Confidence
            if contract.is_erc20:
                contract.impl_category = ImplContractCategory.TOKEN
                contract.impl_detected_by.append("heuristic:erc20_selectors")
                contract.impl_classify_confidence = 1.0
            elif contract.is_erc721:
                contract.impl_category = ImplContractCategory.NFT
                contract.impl_detected_by.append("heuristic:erc721_selectors")
                contract.impl_classify_confidence = 1.0
            elif contract.is_erc1155:
                contract.impl_category = ImplContractCategory.MULTI_TOKEN
                contract.impl_detected_by.append("heuristic:erc1155_selectors")
                contract.impl_classify_confidence = 1.0

            # --- STEP 6: FINAL ENRICHMENT (Standard Contract) ---
            # Attempt to fetch metadata for any Token standard
            if contract.is_erc20 or contract.is_erc721 or contract.is_erc1155:
                await self._enrich_metadata(contract, address)
            
            return contract

        except Exception as e:
            logger.error(f"Error processing contract {address}: {e}")
            return base_contract # Return default EOA-like if error

    async def _handle_proxy(
        self, address: str, block_number: int, bytecode: str, 
        proxy_type: ProxyType, impl_address: str
    ) -> EthProxyContract:
        """
        Creates a Proxy object, resolves its Implementation logic, and enriches metadata.
        """
        proxy = EthProxyContract(
            address=address,
            block_number=block_number,
            bytecode=bytecode,
            is_proxy=True,
            proxy_type=proxy_type,
            implementation_address=impl_address
        )
        
        # Recursive Resolution: Analyze the Implementation Contract
        # We need to pass the block_number to ensure analysis is at the correct state
        impl_contract = await self._analyze_implementation(impl_address, block_number)
        proxy.implementation = impl_contract
        
        # Flattening Strategy:
        # If Implementation is a Token, we must fetch Name/Symbol from the PROXY address.
        # (Because state is in Proxy, calls to name/symbol will be delegated from Proxy)
        if impl_contract.is_erc20 or impl_contract.is_erc721 or impl_contract.is_erc1155:
            await self._enrich_metadata(proxy, address)
            
        return proxy

    @alru_cache(maxsize=1024, ttl=3600)
    async def _analyze_implementation(self, address: str, block_number: int) -> EthImplementationContract:
        """
        Fetches and statically analyzes the Implementation Contract.
        Does NOT fetch metadata (name/symbol) because Implementation usually doesn't hold state.
        """
        impl = EthImplementationContract(address=address, block_number=block_number)
        
        try:
            bytecode = await self._rpc_client.get_code(address)
            if not bytecode or bytecode == "0x":
                return impl
            
            impl.bytecode = bytecode
            sighashes = EthContractAnalyzerService.get_function_sighashes(bytecode)
            impl.function_sighashes = sighashes
            
            impl.is_erc20 = EthContractAnalyzerService.is_erc20_contract(sighashes)
            impl.is_erc721 = EthContractAnalyzerService.is_erc721_contract(sighashes)
            impl.is_erc1155 = EthContractAnalyzerService.is_erc1155_contract(sighashes)
            
            # Confidence scoring logic added
            if impl.is_erc20:
                impl.impl_category = ImplContractCategory.TOKEN
                impl.impl_detected_by.append("implementation:erc20")
                impl.impl_classify_confidence = 1.0
            elif impl.is_erc721:
                impl.impl_category = ImplContractCategory.NFT
                impl.impl_detected_by.append("implementation:erc721")
                impl.impl_classify_confidence = 1.0
            elif impl.is_erc1155:
                impl.impl_category = ImplContractCategory.MULTI_TOKEN
                impl.impl_detected_by.append("implementation:erc1155")
                impl.impl_classify_confidence = 1.0
            
            # Check other categories if not a Token/NFT
            if impl.impl_category == ImplContractCategory.UNKNOWN:
                if EthContractAnalyzerService.is_dex_factory(sighashes):
                    impl.impl_category = ImplContractCategory.FACTORY
                    impl.impl_detected_by.append("heuristic:dex_factory")
                    impl.impl_classify_confidence = 0.9 # Heuristic
                elif EthContractAnalyzerService.is_dex_router(sighashes):
                    impl.impl_category = ImplContractCategory.ROUTER
                    impl.impl_detected_by.append("heuristic:dex_router")
                    impl.impl_classify_confidence = 0.9
                elif EthContractAnalyzerService.is_erc4626_vault(sighashes):
                    impl.impl_category = ImplContractCategory.VAULT
                    impl.impl_detected_by.append("heuristic:erc4626")
                    impl.impl_classify_confidence = 0.95 # EIP standard
                elif EthContractAnalyzerService.is_governance(sighashes):
                    impl.impl_category = ImplContractCategory.GOVERNANCE
                    impl.impl_detected_by.append("heuristic:governance")
                    impl.impl_classify_confidence = 0.8 # Broad heuristics
                elif EthContractAnalyzerService.is_oracle(sighashes):
                    impl.impl_category = ImplContractCategory.ORACLE
                    impl.impl_detected_by.append("heuristic:oracle")
                    impl.impl_classify_confidence = 0.9
                elif EthContractAnalyzerService.is_bridge(sighashes):
                    impl.impl_category = ImplContractCategory.BRIDGE
                    impl.impl_detected_by.append("heuristic:bridge")
                    impl.impl_classify_confidence = 0.8
                
        except Exception as e:
            logger.warning(f"Failed to analyze implementation {address}: {e}")
            
        return impl

    async def _detect_proxy_slots(self, address: str, sighashes: List[str]) -> Tuple[ProxyType, Optional[str]]:
        """
        Checks EIP-1967 Storage Slots and Gnosis Safe Slot 0 to identify Proxy Type and Implementation Address.
        """
        # 1. Check Gnosis Safe Slot 0 (Custom Proxy)
        # Gnosis Safe stores master copy in SLOT 0
        slot_0_data = await self._rpc_client.get_storage_at(address, ZERO_ADDRESS)
        slot_0_addr = self._bytes32_to_address(slot_0_data)
        
        # Access chain_id asynchronously
        if slot_0_addr and (slot_0_addr in SAFE_CANONICAL_MASTER_COPIES.get(1, {})):
            return ProxyType.GNOSIS_SAFE, slot_0_addr
            
        # 2. Check EIP-1967 Implementation Slot
        impl_data = await self._rpc_client.get_storage_at(address, SLOT_EIP1967_IMPL)
        impl_addr = self._bytes32_to_address(impl_data)
        
        if impl_addr:
            if EthContractAnalyzerService.is_transparent_proxy(sighashes):
                return ProxyType.TRANSPARENT, impl_addr
            elif EthContractAnalyzerService.is_uups_proxy(sighashes):
                return ProxyType.UUPS, impl_addr
            else:
                # Default to Transparent if unable to distinguish further
                return ProxyType.TRANSPARENT, impl_addr

        # 3. Check Beacon Slot
        beacon_data = await self._rpc_client.get_storage_at(address, SLOT_EIP1967_BEACON)
        beacon_addr = self._bytes32_to_address(beacon_data)
        
        if beacon_addr:
            return ProxyType.BEACON, beacon_addr

        return ProxyType.UNKNOWN, None

    @staticmethod
    def _bytes32_to_address(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x": return None
        if len(hex_data) < 42: return None
        
        addr_hex = "0x" + hex_data[-40:]
        
        if addr_hex == "0x0000000000000000000000000000000000000000": 
            return None
            
        return to_normalized_address(addr_hex)

    @staticmethod
    def _get_result_value(rpc_result: Any) -> Optional[str]:
        if isinstance(rpc_result, dict) and "result" in rpc_result:
            return rpc_result["result"]
        return None

    async def _enrich_metadata(
        self,
        contract: Union[EthImplementationContract, EthProxyContract],
        target_address: str
    ):
        """
        Fetches Name, Symbol, Decimals, TotalSupply via eth_call.
        """
        storage_object = contract
        # If it's a Proxy with a resolved implementation, store metadata in the implementation object
        if isinstance(contract, EthProxyContract) and contract.implementation:
            storage_object = contract.implementation
        
        keys = []
        if storage_object.is_erc20:
            keys = ["name", "symbol", "decimals", "totalSupply"]
        elif storage_object.is_erc721 or storage_object.is_erc1155:
            # ERC1155 usually doesn't have name/symbol in the main contract but some do (OpenSea Storefront)
            # We attempt to fetch anyway for richer data
            keys = ["name", "symbol"]
        
        if not keys:
            return

        calls = []
        for key in keys:
            calls.append({"to": target_address, "data": self._metadata_selectors[key]})

        try:
            results = await self._rpc_client.batch_call(calls)
            
            if not results or len(results) != len(keys):
                return
            
            # Map results back to keys
            for i, key in enumerate(keys):
                res_val = self._get_result_value(results[i])
                
                if key == "name":
                    storage_object.name = self._decode_string(res_val)
                elif key == "symbol":
                    storage_object.symbol = self._decode_string(res_val)
                elif key == "decimals":
                    if res_val and res_val != "0x":
                        try:
                            storage_object.decimals = hex_to_dec(res_val)
                        except (ValueError, TypeError):
                            pass
                elif key == "totalSupply":
                     if res_val and res_val != "0x":
                        try:
                            storage_object.total_supply = str(hex_to_dec(res_val))
                        except (ValueError, TypeError):
                            pass
            
            # For ERC721/1155, force decimals to 0
            if storage_object.is_erc721 or storage_object.is_erc1155:
                storage_object.decimals = 0
                
        except Exception as e:
            logger.debug(f"Metadata fetch failed for {target_address}: {e}")

    @staticmethod
    def _decode_string(hex_data: Optional[str]) -> Optional[str]:
        if not hex_data or hex_data == "0x": return None
        try:
            data_bytes = bytes.fromhex(hex_data[2:])
            if not data_bytes: return None
            
            try:
                return decode(['string'], data_bytes)[0]
            except:
                pass
            
            try:
                return decode(['bytes32'], data_bytes)[0].decode('utf-8').strip('\x00')
            except:
                pass
            
            return data_bytes.decode('utf-8', errors='ignore').strip('\x00')
        except Exception:
            return None