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

from config.configs import configs
from ingestion.blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from utils.file_utils import smart_open
from utils.logger_utils import get_logger

logger = get_logger("Streamer")


class Streamer:
    def __init__(
        self,
        blockchain_streamer_adapter: Any = StreamerAdapterStub,
        last_synced_block_file: str = "last_synced_block.txt",
        lag: int = 0,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        period_seconds: int = configs.ethereum.streamer_period_seconds,
        block_batch_size: int = configs.ethereum.streamer_block_batch_size,
        retry_errors: bool = configs.ethereum.streamer_retry_errors,
        pid_file: Optional[str] = None,
    ):
        """
        Initializes the Streamer with configuration parameters.

        Args:
            blockchain_streamer_adapter (Any): An adapter that provides blockchain-specific logic (e.g., fetching blocks).
            last_synced_block_file (str): Path to the file storing the last successfully synced block number.
            lag (int): The number of blocks to lag behind the current network block number.
            start_block (Optional[int]): The block number to start syncing from initially.
                                        If None and last_synced_block_file exists, it resumes from there.
            end_block (Optional[int]): The block number to stop syncing at. If None, it streams indefinitely.
            period_seconds (int): How many seconds to sleep between sync cycles if there's nothing new to sync.
            block_batch_size (int): The number of blocks to process in a single synchronization round.
            retry_errors (bool): Whether to retry a sync cycle if an error occurs.
            pid_file (Optional[str]): Path to a PID file for process management.
        """
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file

        # Initialize last synced block: either from start_block or from the file
        if self.start_block is not None or not os.path.isfile(self.last_synced_block_file):
            self._init_last_synced_block_file((self.start_block or 0) - 1)

        # Read the last synced block number from the file for current session
        self.last_synced_block = self._read_last_synced_block()

    async def stream(self) -> None:
        """
        Starts the main streaming loop.
        Manages PID file creation/deletion and calls the blockchain adapter.
        """
        try:
            # Create PID file to signal that the process is running
            if self.pid_file is not None:
                logger.info(f"Creating pid file {self.pid_file}")
                self._write_to_file(self.pid_file, str(os.getpid()))

            # Open the streaming adapter (e.g., to initialize RPC connections, etc.)
            await self.blockchain_streamer_adapter.open()

            # Enter the core streaming logic
            await self._do_stream()
        except asyncio.CancelledError:
             logger.info("Streamer has been cancelled. Finalizing...")
             raise
        finally:
            # Ensure adapter resources are closed
            # Check if close is a coroutine function or a regular method
            if asyncio.iscoroutinefunction(self.blockchain_streamer_adapter.close):
                await self.blockchain_streamer_adapter.close()
            else:
                self.blockchain_streamer_adapter.close()
            
            # Clean up PID file on graceful shutdown
            if self.pid_file is not None:
                logger.info(f"Deleting pid file {self.pid_file}")
                self._delete_file(self.pid_file)

    async def _do_stream(self) -> None:
        """
        The main asynchronous loop for streaming blockchain data.
        It continuously syncs blocks until end_block is reached or indefinitely.
        """
        # Loop indefinitely if end_block is not set, or until last_synced_block reaches end_block
        while True and (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0

            try:
                # Perform one synchronization cycle
                synced_blocks = await self._sync_cycle()
            except Exception as e:
                # Log the exception and decide whether to retry or re-raise
                logger.exception("An exception occurred while syncing block data.")
                if not self.retry_errors:
                    raise e # Re-raise if retries are disabled

            # If no blocks were synced in this cycle, wait before trying again
            if synced_blocks <= 0:
                logger.info(f"Nothing to sync. Sleeping for {self.period_seconds} seconds...")
                await asyncio.sleep(self.period_seconds)

    async def _sync_cycle(self) -> int:
        """
        Executes a single synchronization round.
        Fetches current block number, calculates target, exports data, and updates synced block.
        Returns the number of blocks synced in this cycle.
        """
        try:
            # Get the current highest block number from the blockchain network
            current_block = await self.blockchain_streamer_adapter.get_current_block_number()
        except Exception as e:
            logger.error(f"Failed to get current block number: {e}")
            raise e # Critical error, re-raise to stop streaming

        # Determine the target block number to sync up to in this cycle
        target_block = self._calculate_target_block(current_block, self.last_synced_block)
        
        # Calculate how many blocks actually need to be synced
        blocks_to_sync = max(target_block - self.last_synced_block, 0)

        logger.info(
            f"Current network block: {current_block}, "
            f"Calculated target block: {target_block}, "
            f"Last successfully synced block: {self.last_synced_block}, "
            f"Blocks to process in this cycle: {blocks_to_sync}"
        )

        if blocks_to_sync != 0:
            # Log the range being synced
            logger.info(f"Syncing blocks {self.last_synced_block + 1} to {target_block}...")
            
            # Delegate the actual export process to the adapter
            # The adapter is responsible for parallelizing and exporting to Kafka
            await self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            
            # Update the last synced block number in the persistent file
            logger.info(f"Writing last synced block {target_block} to file.")
            self._write_last_synced_block(target_block)
            
            # Update the in-memory state
            self.last_synced_block = target_block

        return blocks_to_sync

    def _calculate_target_block(self, current_block: int, last_synced_block: int) -> int:
        """
        Calculates the maximum block number the streamer should sync up to in the current cycle.
        Considers network lag, configured block_batch_size, and explicit end_block.
        """
        # Apply lag: how many blocks to intentionally trail the network
        target_block = current_block - self.lag
        
        # Limit by block_batch_size: don't sync too many blocks at once
        target_block = min(target_block, last_synced_block + self.block_batch_size)
        
        # If an end_block is specified, don't go past it
        target_block = min(target_block, self.end_block) if self.end_block is not None else target_block
        
        return target_block

    def _init_last_synced_block_file(self, start_block: int) -> None:
        """
        Initializes or sets the starting point for syncing.
        If start_block is provided, it overrides the file's content (if file exists, it raises error).
        """
        if os.path.isfile(self.last_synced_block_file) and self.start_block is not None:
            # Prevent accidental overwrite if start_block is specified and file already exists
            raise ValueError(
                f"'{self.last_synced_block_file}' should not exist if --start-block option is specified. "
                "Either remove the file or the --start-block option to resume from the file."
            )
        # Write the initial start_block to the file (e.g., -1 so first sync starts at 0)
        self._write_last_synced_block(start_block)

    def _read_last_synced_block(self) -> int:
        """
        Reads the last synced block number from the persistent file.
        """
        with smart_open(self.last_synced_block_file, "r") as last_synced_block_file:
            return int(last_synced_block_file.read().strip()) # .strip() to remove potential newlines

    def _write_last_synced_block(self, last_synced_block: int) -> None:
        """
        Writes the last synced block number to a file atomically.
        This prevents file corruption if the process crashes during writing.
        """
        content = str(last_synced_block) + "\n"
        temp_file = self.last_synced_block_file + ".tmp"

        # Write to a temporary file first
        with smart_open(temp_file, "w") as file_handle:
            file_handle.write(content)
            file_handle.flush()
            # Ensure data is truly written to disk, not just buffer
            os.fsync(file_handle.fileno())

        # Atomically replace the old file with the new one
        # This ensures the file is always in a valid state
        os.replace(temp_file, self.last_synced_block_file)

    @staticmethod
    def _write_to_file(file_path: str, content: str) -> None:
        """Helper static method to write content to a specified file."""
        with smart_open(file_path, "w") as file_handle:
            file_handle.write(content)

    @staticmethod
    def _delete_file(file_path: str) -> None:
        """Helper static method to delete a file, ignoring if it doesn't exist."""
        try:
            os.remove(file_path)
        except OSError: # Catch OS-level errors like file not found
            pass
