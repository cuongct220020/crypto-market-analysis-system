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
# - Fixed `get_function_sighashes` method signature (removed `self` from `@staticmethod`).
# - Added comprehensive type hints to all functions, methods, parameters, and return types.
# - Replaced `print` statements with `logger.error` for improved error logging.
# - Refactored comments for clarity, conciseness, and consistency; translated Vietnamese comments to English.
# - Explicitly marked `is_erc20_contract` as a `@staticmethod` for consistency.

import logging
from typing import List, Optional

from eth_utils import function_signature_to_4byte_selector
from pyevmasm import disassemble_all

logger = logging.getLogger("ETH Contract Analyzer Service")


class EthContractAnalyzerService:
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


def get_function_sighash(signature: str) -> str:
    return "0x" + function_signature_to_4byte_selector(signature).hex()


class ContractWrapper:
    def __init__(self, sighashes: List[str]):
        self.sighashes = sighashes

    def implements(self, function_signature: str) -> bool:
        sighash = get_function_sighash(function_signature)
        return sighash in self.sighashes

    def implements_any_of(self, *function_signatures: str) -> bool:
        return any(self.implements(function_signature) for function_signature in function_signatures)
