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
# Change Description: Refactored for clean code, encapsulation, atomic file writes, and type hinting.


import asyncio
import os
from typing import Any, Optional

from config.settings import settings
from ingestion.blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from utils.file_utils import smart_open
from utils.logger_utils import get_logger

logger = get_logger("Streamer")


class Streamer:
    def __init__(
        self,
        blockchain_streamer_adapter: Any = StreamerAdapterStub(),
        last_synced_block_file: str = "last_synced_block.txt",
        lag: int = 0,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        period_seconds: int = settings.streamer.period_seconds,
        block_batch_size: int = settings.streamer.block_batch_size,
        retry_errors: bool = settings.streamer.retry_errors,
        pid_file: Optional[str] = None,
    ):
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file

        if self.start_block is not None or not os.path.isfile(self.last_synced_block_file):
            self._init_last_synced_block_file((self.start_block or 0) - 1)

        self.last_synced_block = self._read_last_synced_block()

    async def stream(self) -> None:
        try:
            if self.pid_file is not None:
                logger.info("Creating pid file {}".format(self.pid_file))
                self._write_to_file(self.pid_file, str(os.getpid()))

            # Open adapter (Async)
            await self.blockchain_streamer_adapter.open()

            await self._do_stream()
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logger.info("Deleting pid file {}".format(self.pid_file))
                self._delete_file(self.pid_file)

    async def _do_stream(self) -> None:
        while True and (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0

            try:
                synced_blocks = await self._sync_cycle()
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logger.exception("An exception occurred while syncing block data.")
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logger.info("Nothing to sync. Sleeping for {} seconds...".format(self.period_seconds))
                await asyncio.sleep(self.period_seconds)

    async def _sync_cycle(self) -> int:
        try:
            current_block = await self.blockchain_streamer_adapter.get_current_block_number()
        except Exception as e:
            logger.error(f"Failed to get current block number: {e}")
            raise e

        target_block = self._calculate_target_block(current_block, self.last_synced_block)
        blocks_to_sync = max(target_block - self.last_synced_block, 0)

        logger.info(
            "Current block {}, target block {}, last synced block {}, blocks to sync {}".format(
                current_block, target_block, self.last_synced_block, blocks_to_sync
            )
        )

        if blocks_to_sync != 0:
            logger.info(f"Syncing blocks {self.last_synced_block + 1} to {target_block}...")
            await self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            logger.info("Writing last synced block {}".format(target_block))
            self._write_last_synced_block(target_block)
            self.last_synced_block = target_block

        return blocks_to_sync

    def _calculate_target_block(self, current_block: int, last_synced_block: int) -> int:
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced_block + self.block_batch_size)
        target_block = min(target_block, self.end_block) if self.end_block is not None else target_block
        return target_block

    def _init_last_synced_block_file(self, start_block: int) -> None:
        if os.path.isfile(self.last_synced_block_file):
            raise ValueError(
                "{} should not exist if --start-block option is specified. "
                "Either remove the {} file or the --start-block option.".format(
                    self.last_synced_block_file, self.last_synced_block_file
                )
            )
        self._write_last_synced_block(start_block)

    def _read_last_synced_block(self) -> int:
        with smart_open(self.last_synced_block_file, "r") as last_synced_block_file:
            return int(last_synced_block_file.read())

    def _write_last_synced_block(self, last_synced_block: int) -> None:
        """
        Writes the last synced block number to a file atomically.
        This prevents file corruption if the process crashes during writing.
        """
        content = str(last_synced_block) + "\n"
        temp_file = self.last_synced_block_file + ".tmp"

        with smart_open(temp_file, "w") as file_handle:
            file_handle.write(content)
            file_handle.flush()
            # Ensure data is written to disk
            os.fsync(file_handle.fileno())

        # Atomic replacement of the old file with the new one
        os.replace(temp_file, self.last_synced_block_file)

    @staticmethod
    def _write_to_file(file_path: str, content: str) -> None:
        with smart_open(file_path, "w") as file_handle:
            file_handle.write(content)

    @staticmethod
    def _delete_file(file_path: str) -> None:
        try:
            os.remove(file_path)
        except OSError:
            pass
