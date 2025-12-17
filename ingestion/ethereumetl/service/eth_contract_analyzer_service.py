import logging
from typing import List, Optional

from eth_utils import function_signature_to_4byte_selector
from pyevmasm import disassemble_all

from constants.contract_proxy_constants import EIP1167_PREFIX, EIP1167_SUFFIX
from constants.contract_function_selectors import (
    TRANSPARENT_PROXY_FUNCTION_SELECTORS,
    UUPS_PROXY_FUNCTION_SELECTORS,
    DIAMOND_PROXY_FUNCTION_SELECTORS,
    SAFE_GNOSIS_FUNCTION_SELECTORS,
    # ERC20_FUNCTION_SELECTORS,
    # ERC721_FUNCTION_SELECTORS,
    # ERC1155_FUNCTION_SELECTORS,
    DEX_FACTORY_SELECTORS,
    DEX_ROUTER_SELECTORS,
    ERC4626_FUNCTION_SELECTORS,
    GOVERNANCE_FUNCTION_SELECTORS,
    CHAINLINK_ORACLE_SELECTORS,
    CROSS_CHAIN_BRIDGE_SELECTORS
)

logger = logging.getLogger("ETH Contract Analyzer Service")

class EthContractAnalyzerService:
    # Pre-define required selectors as Sets for O(1) checking and cleaner code
    # We use the keys directly from constants to avoid re-hashing strings
    
    # ERC20: totalSupply, balanceOf, transfer
    ERC20_REQUIRED = {
        "0x18160ddd", "0x70a08231", "0xa9059cbb"
    }

    # ERC721: ownerOf, balanceOf, setApprovalForAll, supportsInterface
    ERC721_REQUIRED = {
        "0x6352211e", "0x70a08231", "0xa22cb465", "0x01ffc9a7"
    }

    # ERC1155: balanceOfBatch, safeBatchTransferFrom, setApprovalForAll, supportsInterface
    ERC1155_REQUIRED = {
        "0x4e1273f4", "0x2eb2c2d6", "0xa22cb465", "0x01ffc9a7"
    }

    @staticmethod
    def is_minimal_proxy(bytecode: str) -> str | None:
        """
        Checks if bytecode matches EIP-1167 Minimal Proxy pattern.
        Returns implementation address if found, None otherwise.
        """
        if not bytecode or bytecode == "0x":
            return None
            
        cleaned_bytecode = EthContractAnalyzerService._clean_bytecode(bytecode)
        
        if len(cleaned_bytecode) < 90:
             return None

        if cleaned_bytecode.startswith(EIP1167_PREFIX) and cleaned_bytecode.endswith(EIP1167_SUFFIX):
            impl_hex = cleaned_bytecode[20:60]
            return "0x" + impl_hex

        return None


    @staticmethod
    def is_diamond_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(DIAMOND_PROXY_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_gnosis_safe_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_all_of_selectors(list(SAFE_GNOSIS_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_transparent_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(TRANSPARENT_PROXY_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_uups_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(UUPS_PROXY_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_erc20_contract(function_sighashes: List[str]) -> bool:
        """Checks for minimal set of required ERC20 functions."""
        c = ContractWrapper(function_sighashes)
        return c.implements_all_of_selectors(EthContractAnalyzerService.ERC20_REQUIRED)


    @staticmethod
    def is_erc721_contract(function_sighashes: List[str]) -> bool:
        """Checks for minimal set of required ERC721 functions."""
        c = ContractWrapper(function_sighashes)
        # Must implement required core functions AND at least one transfer variant
        if not c.implements_all_of_selectors(EthContractAnalyzerService.ERC721_REQUIRED):
            return False
        return c.implements_any_of("transferFrom(address,address,uint256)", "safeTransferFrom(address,address,uint256)")


    @staticmethod
    def is_erc1155_contract(function_sighashes: List[str]) -> bool:
        """Checks for minimal set of required ERC1155 functions."""
        c = ContractWrapper(function_sighashes)
        return c.implements_all_of_selectors(EthContractAnalyzerService.ERC1155_REQUIRED)


    @staticmethod
    def is_dex_factory(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(DEX_FACTORY_SELECTORS.keys()))


    @staticmethod
    def is_dex_router(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(DEX_ROUTER_SELECTORS.keys()))


    @staticmethod
    def is_erc4626_vault(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_all_of_selectors(list(ERC4626_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_governance(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(GOVERNANCE_FUNCTION_SELECTORS.keys()))


    @staticmethod
    def is_oracle(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(CHAINLINK_ORACLE_SELECTORS.keys()))


    @staticmethod
    def is_bridge(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_any_of_selectors(list(CROSS_CHAIN_BRIDGE_SELECTORS.keys()))


    @staticmethod
    def get_function_sighashes(bytecode: Optional[str]) -> List[str]:
        bytecode = EthContractAnalyzerService._clean_bytecode(bytecode)
        if bytecode is None:
            return []

        try:
            bytecode_bytes = bytes.fromhex(bytecode)
            # Limit disassembly to first 500 instructions for performance
            instruction_list = list(disassemble_all(bytecode_bytes))
            scan_limit = min(len(instruction_list), 500)
            
            push4_operands = []
            for i in range(scan_limit):
                inst = instruction_list[i]
                if inst.name == "PUSH4":
                    hex_val = hex(inst.operand)[2:].zfill(8)
                    push4_operands.append("0x" + hex_val)

            return sorted(list(set(push4_operands)))

        except Exception as e:
            logger.error(f"Error parsing bytecode: {e}")
            return []

    @staticmethod
    def _clean_bytecode(bytecode: Optional[str]) -> Optional[str]:
        if bytecode is None or bytecode == "0x":
            return None
        elif bytecode.startswith("0x"):
            return bytecode[2:]
        else:
            return bytecode


class ContractWrapper:
    def __init__(self, sighashes: List[str]):
        # Optimization: Use Set for O(1) lookup
        self.sighashes = set(sighashes)

    def implements(self, function_signature: str) -> bool:
        """Checks if a function signature (e.g., 'transfer(address,uint256)') exists."""
        # Consider caching this conversion if called repeatedly for same signatures
        sighash = "0x" + function_signature_to_4byte_selector(function_signature).hex()
        return sighash in self.sighashes
    
    def implements_selector(self, selector: str) -> bool:
        """Checks if a raw 4-byte selector (e.g., '0x01ffc9a7') exists."""
        return selector in self.sighashes

    def implements_any_of(self, *function_signatures: str) -> bool:
        return any(self.implements(sig) for sig in function_signatures)
        
    def implements_any_of_selectors(self, selectors: List[str]) -> bool:
        return any(self.implements_selector(sel) for sel in selectors)

    def implements_all_of_selectors(self, selectors: set | List[str]) -> bool:
        """Checks if all provided raw 4-byte selectors exist."""
        # Optimization: if input is a set, we can verify subset relationship
        if isinstance(selectors, set):
            return selectors.issubset(self.sighashes)
        return all(self.implements_selector(sel) for sel in selectors)