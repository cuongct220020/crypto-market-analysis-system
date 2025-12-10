# MIT License
#
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
# Change Description: Refactor to process EthTrace models and export EthContract models


from typing import Any, Iterable, List

from ingestion.blockchainetl.jobs.base_job import BaseJob
from ingestion.ethereumetl.mappers.contract_mapper import EthContractMapper
from ingestion.ethereumetl.models.contract import EthContract
from ingestion.ethereumetl.service.eth_contract_service import EthContractService
from utils.logger_utils import get_logger

logger = get_logger("Extract Contracts Job")


# Extract contracts
class ExtractContractsJob(BaseJob):
    def __init__(self, traces_iterable: Iterable[Any], item_exporter: Any):
        self.traces_iterable = traces_iterable
        self.item_exporter = item_exporter

        self.contract_service = EthContractService()
        self.contract_mapper = EthContractMapper()

    def _start(self) -> None:
        logger.info("Starting extraction of contracts from traces...")
        self.item_exporter.open()

    def _export(self) -> None:
        logger.info("Starting contract extraction process...")
        self._extract_contracts(self.traces_iterable)
        logger.info("Contract extraction completed")

    def _extract_contracts(self, traces: Iterable[Any]) -> None:
        # traces is a list of EthTrace models
        logger.debug("Starting to extract contracts from traces...")
        contract_creation_traces = []
        total_traces = 0
        valid_creation_traces = 0

        for trace in traces:
            total_traces += 1
            if (
                trace.trace_type == "create"
                and trace.to_address is not None
                and len(trace.to_address) > 0
                and trace.status == 1
            ):
                contract_creation_traces.append(trace)
                valid_creation_traces += 1
                logger.debug(f"Found valid contract creation trace for address: {trace.to_address}")

        logger.info(f"Processed {total_traces} total traces, found {valid_creation_traces} valid contract creation traces")

        contracts: List[EthContract] = []
        for i, trace in enumerate(contract_creation_traces):
            logger.debug(f"Processing contract creation {i+1}/{valid_creation_traces} for address: {trace.to_address}")
            contract = EthContract()
            contract.address = trace.to_address
            contract.bytecode = trace.output
            contract.block_number = trace.block_number

            logger.debug(f"Analyzing bytecode for contract {trace.to_address}...")
            function_sighashes = self.contract_service.get_function_sighashes(contract.bytecode)

            contract.function_sighashes = function_sighashes
            contract.is_erc20 = self.contract_service.is_erc20_contract(function_sighashes)
            contract.is_erc721 = self.contract_service.is_erc721_contract(function_sighashes)

            logger.debug(f"Contract {trace.to_address} - ERC20: {contract.is_erc20}, ERC721: {contract.is_erc721}")
            contracts.append(contract)

        logger.info(f"Exporting {len(contracts)} contracts...")
        for contract in contracts:
            self.item_exporter.export_item(contract)
        logger.info(f"Successfully exported {len(contracts)} contracts")

    def _end(self) -> None:
        logger.info("Shutting down ExtractContractsJob resources...")
        self.item_exporter.close()
        logger.info("ExtractContractsJob completed successfully")
