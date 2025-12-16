import asyncio
import os
import time
try:
    import uvloop
except ImportError:
    uvloop = None
from typing import List, Dict, Set

from ingestion.blockchainetl.exporters.kafka_item_exporter import KafkaItemExporter
from ingestion.ethereumetl.rpc_client import RpcClient
from ingestion.ethereumetl.streaming.eth_data_enricher import EthDataEnricher
from ingestion.ethereumetl.service.eth_contract_service import EthContractService
from utils.logger_utils import get_logger

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
        self._exporter = KafkaItemExporter(
            kafka_broker_url=kafka_broker_url,
            item_type_to_topic_mapping=item_type_to_topic_mapping or {}
        )
        # Call open to initialize producer
        self._exporter.open()

        # Queue 1: Raw Blocks & Receipts (Fetcher -> Processor)
        self._worker_internal_queue = asyncio.Queue(maxsize=self._worker_internal_queue_size)
        
        # Queue 2: New Contract Addresses (Processor -> Contract Worker)
        # Size limit helps provide backpressure if contract fetching is too slow compared to block processing
        self._worker_contract_queue = asyncio.Queue(maxsize=self._worker_internal_queue_size * 10)
        
        # Enricher deals with data transformation
        self._enricher = EthDataEnricher()
        
        # Contract Service
        self._contract_service = EthContractService(self._rpc_client)
        
        # Deduplication set for contracts within the worker lifetime
        self._seen_contracts: Set[str] = set() 

    async def run(self, job_queue):
        """
        Main Async Loop.
        job_queue: A multiprocessing.Queue containing (start, end) tuples.
        """
        logger.info(f"Worker {self._worker_id} started. PID: {os.getpid()}")
        
        # Start the Processor task (Consumer 1: Blocks/Logs)
        processor_task = asyncio.create_task(self._processor_loop())
        
        # Start the Contract Worker task (Consumer 2: Contracts)
        contract_task = asyncio.create_task(self._contract_loop())
        
        # --- STAGE 1: FETCH LOOP (Producer) ---
        try:
            while True:
                try:
                    # Use run_in_executor to make queue.get non-blocking for event loop
                    loop = asyncio.get_running_loop()
                    job = await loop.run_in_executor(None, lambda: job_queue.get())
                except (EOFError, BrokenPipeError) as err:
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
                        await self._worker_internal_queue.put((i, batch_end, data))
                        
                    # Rate Limit
                    if self._rate_limit_sleep > 0:
                        await asyncio.sleep(self._rate_limit_sleep)
            
        except asyncio.CancelledError:
             logger.info(f"Worker {self._worker_id} received cancellation signal.")
        except Exception as e:
            logger.error(f"Worker {self._worker_id} crashed: {e}")
        finally:
            logger.info(f"Worker {self._worker_id} stopping pipelines...")
            
            # 1. Stop Processor
            await self._worker_internal_queue.put(None)
            try:
                await processor_task
            except Exception as e:
                logger.error(f"Worker {self._worker_id} processor task error: {e}")

            # 2. Stop Contract Worker
            # Processor has finished, so no new contracts will be added.
            # We send None to signal contract worker to finish queue and exit.
            await self._worker_contract_queue.put(None)
            try:
                await contract_task
            except Exception as e:
                logger.error(f"Worker {self._worker_id} contract task error: {e}")

            logger.info(f"Worker {self._worker_id} shutting down resources...")
            await self._rpc_client.close()
            self._exporter.close()

    async def _processor_loop(self):
        """
        --- STAGE 2: PROCESS BLOCK/LOGS ---
        Consumes raw RPC data -> Maps -> Enriches -> Publishes Core Entities.
        Identifies new contracts and dispatches them to Stage 3.
        """
        while True:
            item = await self._worker_internal_queue.get()
            if item is None:
                self._worker_internal_queue.task_done()
                break
            
            start, end, raw_data = item
            try:
                await self._process_batch(start, end, raw_data)
            except Exception as e:
                logger.error(f"Error processing batch {start}-{end}: {e}")
            finally:
                self._worker_internal_queue.task_done()

    async def _process_batch(self, start: int, end: int, raw_data: List[Dict]):
        """
        Logic to transform Raw JSON to Domain Objects and Publish.
        """
        # Delegate complex logic to Enricher
        blocks, transactions, receipts, token_transfers, contract_addresses = self._enricher.enrich_batch(raw_data, start, end)

        # Batch Export Core Items
        self._exporter.export_items(blocks)
        self._exporter.export_items(transactions)
        self._exporter.export_items(receipts)
        self._exporter.export_items(token_transfers)
        
        # Process Contracts
        # Deduplicate against local cache
        new_contracts = [addr for addr in contract_addresses if addr not in self._seen_contracts]
        
        if new_contracts:
            self._seen_contracts.update(new_contracts)
            # Push to contract queue for parallel processing
            # We pass 'end' block to allow the service to query state at that point if needed
            await self._worker_contract_queue.put((new_contracts, end))
            
        # Report Progress
        if self._progress_queue and blocks:
            try:
                self._progress_queue.put(len(blocks))
            except Exception as e:
                pass 

    async def _contract_loop(self):
        """
        --- STAGE 3: PROCESS CONTRACTS ---
        Consumes contract addresses, fetches metadata (RPC), exports contracts.
        Running in parallel with Stage 1 & 2.
        """
        while True:
            item = await self._worker_contract_queue.get()
            if item is None:
                self._worker_contract_queue.task_done()
                break
            
            addresses, block_number = item
            try:
                # This RPC call is now decoupled from the block processing loop
                contracts = await self._contract_service.get_contracts(addresses, block_number)
                if contracts:
                    self._exporter.export_items(contracts)
            except Exception as e:
                logger.error(f"Error processing contracts for block {block_number}: {e}")
            finally:
                self._worker_contract_queue.task_done()

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
        pass # Allow main process to handle cleanup
    except Exception as e:  # noqa
        logger.info(f"Worker {worker_id} unhandled exception: {e}")