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
# Change Description: Refactor to Async Job


from typing import Any, List

from web3 import AsyncWeb3

from constants.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.async_batch_work_executor import AsyncBatchWorkExecutor
from ingestion.ethereumetl.mappers.trace_mapper import EthTraceMapper
from ingestion.ethereumetl.service.eth_special_trace_service import EthSpecialTraceService
from ingestion.ethereumetl.service.trace_id_service import TraceIdService
from ingestion.ethereumetl.service.trace_status_service import TraceStatusService
from utils.async_utils import gather_with_concurrency
from utils.logger_utils import get_logger
from utils.validation_utils import validate_block_range

logger = get_logger("Export Traces Job")


class ExportTracesJob(AsyncBaseJob):
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int,
        web3: AsyncWeb3,
        item_exporter: Any,
        max_workers: int,
        max_concurrent_requests: int = 5,
        include_genesis_traces: bool = False,
        include_daofork_traces: bool = False,
    ):
        validate_block_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3
        self.item_exporter = item_exporter
        self.max_concurrent_requests = max_concurrent_requests

        # Currently batch size is 1 for traces due to potential load/issues
        self.batch_work_executor = AsyncBatchWorkExecutor(1, max_workers)

        self.trace_mapper = EthTraceMapper()
        self.special_trace_service = EthSpecialTraceService()
        self.include_genesis_traces = include_genesis_traces
        self.include_daofork_traces = include_daofork_traces

    async def _start(self) -> None:
        logger.info(f"Starting export of traces from block {self.start_block} to {self.end_block}")
        if self.include_genesis_traces:
            logger.info("Genesis traces will be included")
        if self.include_daofork_traces:
            logger.info(f"DAOFORK traces will be included for block {DAOFORK_BLOCK_NUMBER}")
        self.item_exporter.open()

    async def _export(self) -> None:
        logger.info(f"Exporting traces for {self.end_block - self.start_block + 1} blocks from {self.start_block} to {self.end_block}")
        await self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
        )

    async def _export_batch(self, block_number_batch: List[int]) -> None:
        logger.debug(f"Processing batch of {len(block_number_batch)} blocks: {block_number_batch[0]} to {block_number_batch[-1]}")

        # Async fetch and export
        tasks = [self._fetch_and_export_traces(block_number) for block_number in block_number_batch]
        await gather_with_concurrency(self.max_concurrent_requests, *tasks)

        logger.debug(f"Completed processing batch of {len(block_number_batch)} blocks")

    async def _fetch_and_export_traces(self, block_number: int) -> None:
        logger.debug(f"Fetching and exporting traces for block #{block_number}")

        all_traces = []
        special_trace_count = 0

        if self.include_genesis_traces and block_number == 0:
            genesis_traces = self.special_trace_service.get_genesis_traces()
            all_traces.extend(genesis_traces)
            special_trace_count += len(genesis_traces)

            logger.debug(f"Added {len(genesis_traces)} genesis traces for block 0")

        if self.include_daofork_traces and block_number == DAOFORK_BLOCK_NUMBER:
            daofork_traces = self.special_trace_service.get_daofork_traces()
            all_traces.extend(daofork_traces)
            special_trace_count += len(daofork_traces)

            logger.debug(f"Added {len(daofork_traces)} DAOFORK traces for block {DAOFORK_BLOCK_NUMBER}")

        # Call trace_block
        try:
            logger.debug(f"Making trace_block request for block #{block_number}")
            response = await self.web3.provider.make_request("trace_block", [hex(block_number)])

            if "error" in response:
                raise ValueError(f"RPC Error in trace_block: {response['error']}")

            json_traces = response.get("result")

            if json_traces is None:
                raise ValueError(
                    "Response from the node is None. Is the node fully synced? "
                    "Is the node started with tracing enabled? Is trace_block API enabled?"
                )

            # Map traces (raw JSON from trace API is usually compatible with json_dict_to_trace)
            traces = [self.trace_mapper.json_dict_to_trace(json_trace) for json_trace in json_traces]
            all_traces.extend(traces)
            logger.debug(f"Fetched {len(traces)} traces from block #{block_number}")

        except Exception as e:
            logger.error(f"Error fetching traces for block #{block_number}: {str(e)}")
            raise

        TraceStatusService.calculate_trace_statuses(all_traces)
        TraceIdService.calculate_trace_ids(all_traces)
        self._calculate_trace_indexes(all_traces)

        logger.debug(f"Exporting {len(all_traces)} total traces for block #{block_number} ({len(traces)} from API, {special_trace_count} special)")
        for trace in all_traces:
            self.item_exporter.export_item(trace)
        logger.debug(f"Successfully exported {len(all_traces)} traces for block #{block_number}")

    def _calculate_trace_indexes(self, traces: List[Any]) -> None:
        for ind, trace in enumerate(traces):
            trace.trace_index = ind

    async def _end(self) -> None:
        logger.info("Shutting down ExportTracesJob resources...")
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
        logger.info("ExportTracesJob completed successfully")
