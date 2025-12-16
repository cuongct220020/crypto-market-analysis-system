import asyncio
import os
try:
    import uvloop
except ImportError:
    uvloop = None
from typing import List, Dict

from ingestion.blockchainetl.exporters.kafka_item_exporter import KafkaItemExporter
from ingestion.ethereumetl.rpc_client import RpcClient
from ingestion.ethereumetl.streaming.eth_data_enricher import EthDataEnricher
from ingestion.ethereumetl.service.eth_contract_service import EthContractService
from utils.logger_utils import get_logger
from functools import lru_cache

logger = get_logger("Ingestion Worker")


class IngestionWorker(object):
    """
    Autonomous Worker that fetches, processes, and publishes blockchain data.
    Designed for multiprocessing environment.
    """
    def __init__(self, worker_id: int,
         rpc_url: str,
         kafka_broker_url: str,
         rpc_batch_request_size: int,
         worker_internal_queue_size: int,
         progress_queue=None,
         rate_limit_sleep: float = 0.0,
         item_type_to_topic_mapping: Dict[str, str] = None
    ):
        self._worker_id = worker_id
        self._rpc_client = RpcClient(rpc_url)
        self._rate_limit_sleep = rate_limit_sleep
        self._rpc_batch_request_size = rpc_batch_request_size
        self._worker_internal_queue_size = worker_internal_queue_size
        self._progress_queue = progress_queue
        
        # Use KafkaItemExporter for data publishing
        # This encapsulates producer logic and ensures cleaner code
        self._exporter = KafkaItemExporter(
            kafka_broker_url=kafka_broker_url,
            item_type_to_topic_mapping=item_type_to_topic_mapping or {}
        )
        # Call open to initialize producer if needed (KafkaProducerWrapper initializes in init, but good practice)
        self._exporter.open()

        # Internal Async Queue for decoupling Network I/O from CPU Processing
        self._internal_queue = asyncio.Queue(maxsize=self._worker_internal_queue_size)
        
        # Enricher deals with data transformation
        self._enricher = EthDataEnricher()
        
        # Contract Service
        self._contract_service = EthContractService(self._rpc_client)
        # Simple set for deduplication within the worker lifetime (or use LRU logic if memory is concern)
        # In a streaming context, seeing the same contract twice is common (multiple txs to same contract),
        # but here we are only looking at *created* contracts from receipts, which are unique per chain history.
        # However, re-processing blocks might cause duplicates.
        self._seen_contracts = set() 

    async def run(self, job_queue):
        """
        Main Async Loop.
        job_queue: A multiprocessing.Queue containing (start, end) tuples.
        """
        logger.info(f"Worker {self._worker_id} started. PID: {os.getpid()}")
        
        # Start the Processor task (Consumer)
        processor_task = asyncio.create_task(self._processor_loop())
        
        # Run the Fetcher loop (Producer)
        try:
            while True:
                try:
                    # Use run_in_executor to make queue.get non-blocking for event loop
                    # This allows asyncio to cancel this task if needed
                    loop = asyncio.get_running_loop()
                    job = await loop.run_in_executor(None, lambda: job_queue.get())
                except (EOFError, BrokenPipeError) as err:
                    # If manager is shutdown, we might get EOFError or BrokenPipeError
                    logger.info(f"Worker {self._worker_id} queue closed or error: {err}")
                    break

                if job is None: # Sentinel
                    break
                
                start_block, end_block = job
                # Process the range in chunks of rpc batch request size
                for i in range(start_block, end_block + 1, self._rpc_batch_request_size):
                    batch_end = min(i + self._rpc_batch_request_size - 1, end_block)
                    
                    # Fetch Batch
                    data = await self._rpc_client.fetch_blocks_and_receipts(i, batch_end)
                    if data:
                        # Put to Internal Queue (Wait if full -> Backpressure)
                        await self._internal_queue.put((i, batch_end, data))
                        
                    # Rate Limit
                    if self._rate_limit_sleep > 0:
                        await asyncio.sleep(self._rate_limit_sleep)
            
        except asyncio.CancelledError:
             logger.info(f"Worker {self._worker_id} received cancellation signal.")
        except Exception as e:
            logger.error(f"Worker {self._worker_id} crashed: {e}")
        finally:
            logger.info(f"Worker {self._worker_id} stopping processor...")
            # Signal Processor to stop
            await self._internal_queue.put(None)
            
            # Wait for Processor to finish remaining items
            try:
                await processor_task
            except Exception as e:
                logger.error(f"Worker {self._worker_id} processor task error: {e}")

            logger.info(f"Worker {self._worker_id} shutting down rpc client...")
            await self._rpc_client.close()
            logger.info(f"Worker {self._worker_id} flushing Kafka...")
            self._exporter.close()

    async def _processor_loop(self):
        """
        CPU-Bound Processing Loop.
        Consumes raw RPC data -> Maps -> Enriches -> Publishes.
        """
        while True:
            item = await self._internal_queue.get()
            if item is None:
                self._internal_queue.task_done()
                break
            
            start, end, raw_data = item
            try:
                await self._process_batch(start, end, raw_data)
            except Exception as e:
                logger.error(f"Error processing batch {start}-{end}: {e}")
            finally:
                self._internal_queue.task_done()

    async def _process_batch(self, start: int, end: int, raw_data: List[Dict]):
        """
        Logic to transform Raw JSON to Domain Objects and Publish.
        """
        # Delegate complex logic to Enricher
        blocks, transactions, receipts, token_transfers, contract_addresses = self._enricher.enrich_batch(raw_data, start, end)

        # Batch Export using KafkaItemExporter
        # This handles topic selection based on item type and publishing
        self._exporter.export_items(blocks)
        self._exporter.export_items(transactions)
        self._exporter.export_items(receipts)
        self._exporter.export_items(token_transfers)
        
        # Process Contracts
        # 1. Deduplicate
        new_contracts = [addr for addr in contract_addresses if addr not in self._seen_contracts]
        
        if new_contracts:
            # Update seen set
            self._seen_contracts.update(new_contracts)
            
            # 2. Fetch Metadata
            # Use the end block number as reference for 'latest' state if needed, or just current state
            contracts = await self._contract_service.get_contracts(new_contracts, end)
            
            # 3. Export
            self._exporter.export_items(contracts)
            
        # Report Progress
        if self._progress_queue and blocks:
            try:
                self._progress_queue.put(len(blocks))
            except Exception as e:
                logger.error(f"Worker {self._worker_id} progress queue error: {e}")
                pass # Ignore queue errors (e.g., closed)

def worker_entrypoint(
    worker_id,
    rpc_url,
    kafka_url,
    job_queue,
    rpc_batch_request_size,
    worker_internal_queue_size,
    rate_limit_sleep,
    progress_queue=None,
    item_type_to_topic_mapping=None
):
    """
    Entrypoint needed for multiprocessing to bootstrap the class.
    """
    if uvloop:
        uvloop.install()

    worker = IngestionWorker(
        worker_id=worker_id,
        rpc_url=rpc_url,
        kafka_broker_url=kafka_url,
        rpc_batch_request_size=rpc_batch_request_size,
        worker_internal_queue_size=worker_internal_queue_size,
        rate_limit_sleep=rate_limit_sleep,
        progress_queue=progress_queue,
        item_type_to_topic_mapping=item_type_to_topic_mapping
    )
    try:
        asyncio.run(worker.run(job_queue))
    except KeyboardInterrupt:
        pass # Allow main process to handle cleanup, suppress traceback in workers
    except Exception as e:  # noqa
        # Last resort log
        logger.info(f"Worker {worker_id} unhandled exception: {e}")
