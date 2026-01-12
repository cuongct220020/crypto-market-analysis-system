import multiprocessing
import time
import queue
from tqdm import tqdm

from typing import List
from config.configs import configs
from ingestion.blockchainetl.exporters.console_item_exporter import ConsoleItemExporter
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.rpc_client import RpcClient
from ingestion.ethereumetl.ingestion_worker import worker_entrypoint
from ingestion.ethereumetl.streaming.item_exporter_creator import create_topic_mapping
from utils.logger_utils import get_logger
from utils.validation_utils import validate_block_range

logger = get_logger("ETH Streamer Adapter")


class EthStreamerAdapter:
    def __init__(
        self,
        item_exporter: ConsoleItemExporter,
        entity_types_list: List[EntityType] = None,
        rpc_provider_uris: List[str] = None,
        rpc_batch_request_size: int = configs.ethereum.rpc_batch_request_size,
        num_worker_process: int = configs.ethereum.streamer_num_worker_process,
        rate_sleep: float = configs.ethereum.rpc_request_rate_sleep,
        chunk_size: int = configs.ethereum.streamer_chunk_size,
        queue_size: int = configs.ethereum.streamer_queue_size,
        topic_prefix: str = None,
        output: str = configs.kafka.output,
        rpc_min_interval: float = 0.15,
        enrich_contracts: bool = False,
    ):
        self._item_exporter = item_exporter
        self._entity_types = entity_types_list
        self._rpc_provider_uris = rpc_provider_uris
        self._rpc_batch_request_size = rpc_batch_request_size
        self._num_worker_process = num_worker_process
        self._rate_limit_sleep = rate_sleep
        self._chunk_size = chunk_size
        self._worker_internal_queue_size = queue_size
        self._topic_prefix = topic_prefix
        self._output = output
        self._rpc_min_interval = rpc_min_interval
        self._rpc_client_main = RpcClient(self._rpc_provider_uris, rpc_min_interval=self._rpc_min_interval)
        self._enrich_contracts = enrich_contracts

    async def open(self) -> None:
        self._item_exporter.open()

    async def close(self) -> None:
        await self._rpc_client_main.close()
        self._item_exporter.close()

    async def get_current_block_number(self) -> int:
        return await self._rpc_client_main.get_latest_block_number()

    async def export_all(self, start_block: int, end_block: int = None) -> None:
        """
        Orchestrates the parallel ingestion process using Work Stealing pattern.
        Strategy: 1 Worker Process per Unique RPC Provider
        """
        if end_block is None:
            end_block = start_block

        validate_block_range(start_block, end_block)

        # Auto-adjust workers to match available unique providers
        num_providers = len(self._rpc_provider_uris)
        if self._num_worker_process != num_providers:
            logger.warning(
                f"Adjusting worker count from {self._num_worker_process} to {num_providers} "
                f"to match available RPC Providers (1:1 mapping for max free-tier throughput)."
            )
            self._num_worker_process = num_providers

        logger.info(f"Starting Ingestion: {start_block}-{end_block}")
        logger.info(f"Workers: {self._num_worker_process} | Providers: {num_providers} |Rate Sleep: {self._rate_limit_sleep}s")

        manager = multiprocessing.Manager()
        job_queue = manager.Queue()
        progress_queue = manager.Queue()
        
        # Pre-calculate topic mapping to ensure consistency across workers
        item_type_to_topic_mapping = create_topic_mapping(self._topic_prefix)

        # 1. Populate Queue (Dynamic Work Stealing)
        total_chunks = 0
        for i in range(start_block, end_block + 1, self._chunk_size):
            chunk_end = min(i + self._chunk_size - 1, end_block)
            job_queue.put((i, chunk_end))
            total_chunks += 1

        logger.info(f"Exported {total_chunks} work chunks.")

        # Add Sentinels (Poison Pill) to signal workers to exit
        for _ in range(self._num_worker_process):
            job_queue.put(None)


        workers = []
        for i in range(self._num_worker_process):
            current_rpc = self._rpc_provider_uris[i]

            process = multiprocessing.Process(
                target=worker_entrypoint,
                args=(
                    i, current_rpc,
                    self._output,
                    job_queue,
                    self._rpc_batch_request_size,
                    self._worker_internal_queue_size,
                    self._rate_limit_sleep,
                    progress_queue,
                    item_type_to_topic_mapping,
                    self._rpc_min_interval,
                    self._enrich_contracts
                )
            )
            process.start()
            workers.append(process)
            logger.info(f"Worker {i} assigned RPC: {current_rpc[:20]}...")

        # 3. Wait for completion and Monitor Progress
        start_time = time.time()
        
        total_blocks = end_block - start_block + 1
        processed_count = 0
        
        with tqdm(total=total_blocks, unit="blk", desc="Progress", dynamic_ncols=True) as pbar:
            while any(p.is_alive() for p in workers) or not progress_queue.empty():
                while not progress_queue.empty():
                    try:
                        n = progress_queue.get_nowait()
                        pbar.update(n)
                        processed_count += n
                    except queue.Empty:
                        break
                
                # Exit loop if all work is done (workers might take a moment to die after finishing)
                if processed_count >= total_blocks:
                    break
                    
                time.sleep(0.1)

        # Ensure all workers are joined
        for p in workers:
            p.join()

        total_time = time.time() - start_time
        speed = total_blocks / total_time if total_time > 0 else 0

        logger.info(f"Ingestion Completed in {total_time:.2f}s.")
        logger.info(f"Average Speed: {speed:.2f} blocks/s")
