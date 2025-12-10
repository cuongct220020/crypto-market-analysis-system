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


from typing import Any, Dict, Iterable, List, Optional, Union

from web3 import AsyncWeb3

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.async_batch_work_executor import AsyncBatchWorkExecutor
from ingestion.ethereumetl.mappers.token_mapper import EthTokenMapper
from ingestion.ethereumetl.service.eth_token_metadata_service import EthTokenMetadataService
from utils.async_utils import gather_with_concurrency


class ExportTokensJob(AsyncBaseJob):
    def __init__(
        self,
        web3: AsyncWeb3,
        item_exporter: Any,
        token_addresses_iterable: Iterable[Union[str, Dict[str, Any], List[Any]]],
        max_workers: int,
    ):
        self.item_exporter = item_exporter
        self.token_addresses_iterable = token_addresses_iterable
        self.batch_work_executor = AsyncBatchWorkExecutor(1, max_workers)

        # Kafka and modern downstream systems typically handle raw bytes or require different sanitization.
        self.token_service = EthTokenMetadataService(web3, function_call_result_transformer=None)
        self.token_mapper = EthTokenMapper()

    async def _start(self) -> None:
        self.item_exporter.open()

    async def _export(self) -> None:
        await self.batch_work_executor.execute(self.token_addresses_iterable, self._export_tokens)

    async def _export_tokens(self, token_addresses: List[Union[str, Dict[str, Any], List[Any]]]) -> None:
        tasks = []
        for item in token_addresses:
            if isinstance(item, str):
                tasks.append(self._export_token(item))
            elif isinstance(item, dict):
                tasks.append(
                    self._export_token(token_address=item.get("address"), block_number=item.get("block_number"))
                )
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                tasks.append(self._export_token(token_address=item[0], block_number=item[1]))

        if tasks:
            await gather_with_concurrency(10, *tasks)

    async def _export_token(self, token_address: str, block_number: Optional[int] = None) -> None:
        token = await self.token_service.get_token(token_address)
        token.block_number = block_number
        token_dict = self.token_mapper.token_to_dict(token)
        self.item_exporter.export_item(token_dict)

    async def _end(self) -> None:
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
