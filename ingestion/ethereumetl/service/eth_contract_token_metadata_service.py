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
# - Refactored `get_token` to support metadata extraction for both ERC20 and ERC721 tokens.
# - Integrated bytecode retrieval and function sighash extraction to determine contract type.
# - Populated `EthContract` model with `bytecode`, `function_sighashes`, `is_erc20`, `is_erc721`, and `category`.
# - Introduced `_populate_erc20_metadata` and `_populate_erc721_metadata` helper methods for specific token metadata fetching.
# - Updated import of `EthContractService` to `EthContractAnalyzerService`.


from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from abi.erc20_abi import ERC20_ABI, ERC20_ABI_ALTERNATIVE_1
from abi.erc721_abi import ERC721_ABI
from ingestion.ethereumetl.models.contract import EthContract, ContractCategory
from ingestion.ethereumetl.service.eth_contract_analyzer_service import EthContractAnalyzerService
from utils.logger_utils import get_logger

logger = get_logger("ETH Contract Token Metadata Service")


class EthContractTokenMetadataService(object):
    def __init__(self, web3, function_call_result_transformer=None):
        self._web3 = web3
        self._function_call_result_transformer = function_call_result_transformer
        self._eth_contract_service = EthContractAnalyzerService()

    async def get_token(self, token_address):
        checksum_address = self._web3.toChecksumAddress(token_address)
        
        # Initialize contract model
        contract = EthContract()
        contract.address = token_address

        # 1. Get bytecode
        try:
            bytecode_bytes = await self._web3.eth.get_code(checksum_address)
            bytecode = bytecode_bytes.hex()
            contract.bytecode = bytecode
        except Exception as e:
            logger.debug(f"Failed to get bytecode for {token_address}: {e}")
            # If no bytecode, we can't determine much, but maybe we can still try generic calls?
            # But usually no bytecode means no contract (or destroyed).
            # We'll proceed with empty bytecode logic or just return.
            # If bytecode is 0x, it's an EOA or empty.
            if not contract.bytecode or contract.bytecode == '0x':
                 return contract

        # 2. Extract function sighashes
        try:
            function_sighashes = self._eth_contract_service.get_function_sighashes(contract.bytecode)
            contract.function_sighashes = function_sighashes
        except Exception as e:
            logger.debug(f"Failed to extract sighashes for {token_address}: {e}")
            function_sighashes = []

        # 3. Determine contract type
        is_erc20 = self._eth_contract_service.is_erc20_contract(function_sighashes)
        is_erc721 = self._eth_contract_service.is_erc721_contract(function_sighashes)
        
        contract.is_erc20 = is_erc20
        contract.is_erc721 = is_erc721

        if is_erc20:
            contract.category = ContractCategory.TOKEN
        elif is_erc721:
            contract.category = ContractCategory.NFT
        else:
            contract.category = ContractCategory.UNKNOWN

        # 4. Fetch Metadata based on type
        # Define contracts for calling
        erc20_contract = self._web3.eth.contract(address=checksum_address, abi=ERC20_ABI)
        erc20_contract_alt = self._web3.eth.contract(address=checksum_address, abi=ERC20_ABI_ALTERNATIVE_1)
        erc721_contract = self._web3.eth.contract(address=checksum_address, abi=ERC721_ABI)

        if is_erc20:
            await self._populate_erc20_metadata(contract, erc20_contract, erc20_contract_alt)
        elif is_erc721:
            await self._populate_erc721_metadata(contract, erc721_contract)
        else:
            # Try to fetch basic info even if unknown, assuming it might be ERC20-like or just has name/symbol
            # Many contracts have name/symbol even if not fully ERC20
            await self._populate_erc20_metadata(contract, erc20_contract, erc20_contract_alt)

        return contract

    async def _populate_erc20_metadata(self, contract_model, contract, contract_alt):
        symbol = await self._get_first_result(
            contract.functions.symbol(),
            contract.functions.SYMBOL(),
            contract_alt.functions.symbol(),
            contract_alt.functions.SYMBOL(),
        )
        if isinstance(symbol, bytes):
            symbol = self._bytes_to_string(symbol)

        name = await self._get_first_result(
            contract.functions.name(),
            contract.functions.NAME(),
            contract_alt.functions.name(),
            contract_alt.functions.NAME(),
        )
        if isinstance(name, bytes):
            name = self._bytes_to_string(name)

        decimals = await self._get_first_result(contract.functions.decimals(), contract.functions.DECIMALS())
        total_supply = await self._get_first_result(contract.functions.totalSupply())

        contract_model.symbol = symbol
        contract_model.name = name
        contract_model.decimals = decimals
        contract_model.total_supply = total_supply

    async def _populate_erc721_metadata(self, contract_model, contract):
        symbol = await self._get_first_result(contract.functions.symbol())
        if isinstance(symbol, bytes):
            symbol = self._bytes_to_string(symbol)

        name = await self._get_first_result(contract.functions.name())
        if isinstance(name, bytes):
            name = self._bytes_to_string(name)
        
        # ERC721 usually doesn't have decimals. TotalSupply exists in Enumerable extension but not base 721 metadata often.
        # We try to fetch totalSupply if available.
        total_supply = await self._get_first_result(contract.functions.totalSupply())

        contract_model.symbol = symbol
        contract_model.name = name
        contract_model.total_supply = total_supply
        contract_model.decimals = 0 # NFTs usually 0 decimals

    async def _get_first_result(self, *funcs):
        for func in funcs:
            result = await self._call_contract_function(func)
            if result is not None:
                return result
        return None

    async def _call_contract_function(self, func):
        # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
        # or was self-destructed
        # OverflowError exception happens if the return type of the function doesn't match the expected type
        result = await call_contract_function(
            func=func,
            ignore_errors=(BadFunctionCallOutput, ContractLogicError, OverflowError, ValueError),
            default_value=None,
        )

        if self._function_call_result_transformer is not None:
            return self._function_call_result_transformer(result)
        else:
            return result

    def _bytes_to_string(self, b, ignore_errors=True):
        if b is None:
            return b

        try:
            b = b.decode("utf-8")
        except UnicodeDecodeError as e:
            if ignore_errors:
                logger.debug(
                    "A UnicodeDecodeError exception occurred while trying to decode bytes to string", exc_info=True
                )
                b = None
            else:
                raise e

        if self._function_call_result_transformer is not None:
            b = self._function_call_result_transformer(b)
        return b


async def call_contract_function(func, ignore_errors, default_value=None):
    try:
        result = await func.call()
        return result
    except Exception as ex:
        if type(ex) in ignore_errors:
            logger.debug(
                "An exception occurred in function {} of contract {}. ".format(func.fn_name, func.address)
                + "This exception can be safely ignored.",
                exc_info=True,
            )
            return default_value
        else:
            raise ex
