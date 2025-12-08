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

from eth_utils import function_signature_to_4byte_selector
from pyevmasm import disassemble_all


class EthContractService:

    def get_function_sighashes(self, bytecode):
        bytecode = clean_bytecode(bytecode)
        if bytecode is None:
            return []

        try:
            # Chuyen hex string sang bytes
            bytecode_bytes = bytes.fromhex(bytecode)

            # Disassemble bytecode
            instruction_list = list(disassemble_all(bytecode_bytes))

            # Logic tim PUSH4:
            # Trong EVM, dispatcher thuong nam o day.
            # Chung ta quet toan bo hoac gioi han o 500 lenh dau tien de toi uu hieu nang
            push4_operands = []

            # Gioi han quet (vi du 500 lenh dau) de tranh loop qua cac contract qua lon
            # Contract dispatcher luon nam o dau chuong trinh.
            scan_limit = min(len(instruction_list), 500)

            for i in range(scan_limit):
                inst = instruction_list[i]
                if inst.name == "PUSH4":
                    # pyevmasm tra ve operand dang int, can chuyen ve hex string
                    # 0x + hex value (bo 0x o dau cua hex()) + zfill 8 ky tu
                    hex_val = hex(inst.operand)[2:].zfill(8)
                    push4_operands.append("0x" + hex_val)

            return sorted(list(set(push4_operands)))

        except Exception as e:
            # Log error neu can
            print(f"Error parsing bytecode: {e}")
            return []

    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-20.md
    # https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC20/ERC20.sol
    def is_erc20_contract(self, function_sighashes):
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
    def is_erc721_contract(self, function_sighashes):
        c = ContractWrapper(function_sighashes)
        return (
            c.implements("balanceOf(address)")
            and c.implements("ownerOf(uint256)")
            and c.implements_any_of("transfer(address,uint256)", "transferFrom(address,address,uint256)")
            and c.implements("approve(address,uint256)")
        )


def clean_bytecode(bytecode):
    if bytecode is None or bytecode == "0x":
        return None
    elif bytecode.startswith("0x"):
        return bytecode[2:]
    else:
        return bytecode


def get_function_sighash(signature):
    return "0x" + function_signature_to_4byte_selector(signature).hex()


class ContractWrapper:
    def __init__(self, sighashes):
        self.sighashes = sighashes

    def implements(self, function_signature):
        sighash = get_function_sighash(function_signature)
        return sighash in self.sighashes

    def implements_any_of(self, *function_signatures):
        return any(self.implements(function_signature) for function_signature in function_signatures)
