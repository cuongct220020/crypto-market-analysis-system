# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Modified By: Cuong CT, 6/12/2025
# Change Description:
# - Optimized ContractWrapper using Set for O(1) lookups.
# - Added ERC165 selector check support.
# - Refactored is_erc721_contract logic.

import logging
from typing import List, Optional

from eth_utils import function_signature_to_4byte_selector
from pyevmasm import disassemble_all

from constants.contract_proxy_constants import EIP1167_PREFIX, EIP1167_SUFFIX, SIG_DIAMOND_CUT, SIG_GNOSIS_SETUP, SIG_UPGRADE_TO

logger = logging.getLogger("ETH Contract Analyzer Service")

# ERC165 supportsInterface(bytes4) selector: 0x01ffc9a7
ERC165_SELECTOR = "0x01ffc9a7"

class EthContractAnalyzerService:
    @staticmethod
    def is_minimal_proxy(bytecode: str) -> Optional[str]:
        """
        Checks if bytecode matches EIP-1167 Minimal Proxy pattern.
        Returns implementation address if found, None otherwise.
        """
        if not bytecode or bytecode == "0x":
            return None
            
        clean_bytecode = bytecode.replace("0x", "")
        
        # EIP-1167 Pattern: 363d3d373d3d3d363d73<address>5af43d82803e903d91602b57fd5bf3
        # Prefix (20 chars for 10 bytes) + Address (40 chars for 20 bytes) + Suffix (30 chars for 15 bytes)
        # Total length: 90 chars (45 bytes)
        
        # Basic length check
        if len(clean_bytecode) < 45: 
             return None

        if clean_bytecode.startswith(EIP1167_PREFIX) and clean_bytecode.endswith(EIP1167_SUFFIX):
            # Extract implementation address (20 bytes = 40 chars)
            # Prefix length is 20 chars (10 bytes: 0x363d3d373d3d3d363d73)
            impl_hex = clean_bytecode[20:60]
            return "0x" + impl_hex
        return None

    @staticmethod
    def get_function_sighashes(bytecode: Optional[str]) -> List[str]:
        bytecode = clean_bytecode(bytecode)
        if bytecode is None:
            return []

        try:
            # Convert hex string to bytes
            bytecode_bytes = bytes.fromhex(bytecode)

            # Disassemble bytecode
            instruction_list = list(disassemble_all(bytecode_bytes))

            # Logic to find PUSH4 instructions:
            # In EVM, the dispatcher, which uses PUSH4 instructions for function selectors, is typically at the start.
            # We scan a limited number of initial instructions (e.g., 500) for performance optimization
            # and to avoid processing excessively large contracts.
            push4_operands: List[str] = []

            scan_limit = min(len(instruction_list), 500)

            for i in range(scan_limit):
                inst = instruction_list[i]
                if inst.name == "PUSH4":
                    # pyevmasm returns the operand as an int; convert it to an 8-character zero-padded hex string.
                    # Example: "0x" + hex_value_without_0x_prefix + 8_chars_zfill
                    hex_val = hex(inst.operand)[2:].zfill(8)
                    push4_operands.append("0x" + hex_val)

            return sorted(list(set(push4_operands)))

        except Exception as e:
            # Log any error encountered during bytecode parsing
            logger.error(f"Error parsing bytecode: {e}")
            return []

    @staticmethod
    def is_diamond_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_selector(SIG_DIAMOND_CUT)

    @staticmethod
    def is_gnosis_safe(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_selector(SIG_GNOSIS_SETUP)
        
    @staticmethod
    def is_uups_proxy(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return c.implements_selector(SIG_UPGRADE_TO)

    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-20.md
    # https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC20/ERC20.sol
    @staticmethod
    def is_erc20_contract(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        return (
            c.implements("totalSupply()")
            and c.implements("balanceOf(address)")
            and c.implements("transfer(address,uint256)")
            and c.implements("transferFrom(address,address,uint256)")
            and c.implements("approve(address,uint256)")
            and c.implements("allowance(address,address)")
        )

    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-721.md
    # https://github.com/OpenZeppelin/openzeppelin-contracts/blob/master/contracts/token/ERC721/ERC721.sol
    # Doesn't check the below ERC721 methods to match CryptoKitties contract
    # getApproved(uint256)
    # setApprovalForAll(address,bool)
    # isApprovedForAll(address,address)
    # transferFrom(address,address,uint256)
    # safeTransferFrom(address,address,uint256)
    # safeTransferFrom(address,address,uint256,bytes)
    @staticmethod
    def is_erc721_contract(function_sighashes: List[str]) -> bool:
        c = ContractWrapper(function_sighashes)
        
        # Optional: Check for ERC165 supportsInterface (0x01ffc9a7)
        # has_erc165 = c.implements_selector(ERC165_SELECTOR)
        
        return (
            c.implements("balanceOf(address)")
            and c.implements("ownerOf(uint256)")
            and c.implements_any_of("transfer(address,uint256)", "transferFrom(address,address,uint256)")
            and c.implements("approve(address,uint256)")
        )


def clean_bytecode(bytecode: Optional[str]) -> Optional[str]:
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
        sighash = "0x" + function_signature_to_4byte_selector(function_signature).hex()
        return sighash in self.sighashes
    
    def implements_selector(self, selector: str) -> bool:
        """Checks if a raw 4-byte selector (e.g., '0x01ffc9a7') exists."""
        return selector in self.sighashes

    def implements_any_of(self, *function_signatures: str) -> bool:
        return any(self.implements(function_signature) for function_signature in function_signatures)